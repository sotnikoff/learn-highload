// HTTP-бэкенд ученика.
//
//	GET  /catalog/{id}            — синхронно: чтение товара из Postgres
//	GET  /catalog?category=N      — синхронно: список по категории (без индекса — медленно)
//	POST /orders                  — асинхронно: валидация → Kafka → 202 Accepted
//	GET  /healthz, /metrics
//
// Это стартовая точка. Узкие места оставлены специально: нет кэша, нет индекса,
// маленький пул соединений, синхронная запись в Kafka на каждый запрос.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"highload-lab/internal/metrics"
)

type Product struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	CategoryID int    `json:"category_id"`
	PriceCents int64  `json:"price_cents"`
	Stock      int    `json:"stock"`
}

type OrderRequest struct {
	UserID    int64 `json:"user_id"`
	ProductID int64 `json:"product_id"`
	Quantity  int   `json:"quantity"`
}

type OrderEvent struct {
	ID        string    `json:"id"`
	UserID    int64     `json:"user_id"`
	ProductID int64     `json:"product_id"`
	Quantity  int       `json:"quantity"`
	CreatedAt time.Time `json:"created_at"`
}

type app struct {
	db       *pgxpool.Pool
	producer *kafka.Writer
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := pgxpool.ParseConfig(env("DATABASE_URL", "postgres://lab:lab@localhost:5432/lab"))
	must(err)
	maxConns, _ := strconv.Atoi(env("DB_MAX_CONNS", "20"))
	cfg.MaxConns = int32(maxConns)
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	must(err)
	defer db.Close()

	producer := &kafka.Writer{
		Addr:                   kafka.TCP(env("KAFKA_BROKERS", "localhost:9092")),
		Topic:                  env("ORDERS_TOPIC", "orders"),
		Balancer:               &kafka.Hash{}, // ключ = user_id → заказы юзера в одной партиции, порядок сохраняется
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
		// Синхронная отправка с BatchTimeout 10ms: каждый запрос ждёт батч.
		// Попробуйте Async: true или меньший таймаут и сравните p99.
		BatchTimeout: 10 * time.Millisecond,
	}
	defer producer.Close()

	a := &app{db: db, producer: producer}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /catalog/{id}", metrics.Instrument("/catalog/{id}", a.getProduct))
	mux.HandleFunc("GET /catalog", metrics.Instrument("/catalog", a.listByCategory))
	mux.HandleFunc("POST /orders", metrics.Instrument("/orders", a.createOrder))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("GET /metrics", metrics.Handler())

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Println("backend listening on :8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func (a *app) getProduct(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	var p Product
	err = a.db.QueryRow(r.Context(),
		`SELECT id, name, category_id, price_cents, stock FROM products WHERE id = $1`, id).
		Scan(&p.ID, &p.Name, &p.CategoryID, &p.PriceCents, &p.Stock)
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (a *app) listByCategory(w http.ResponseWriter, r *http.Request) {
	cat, err := strconv.Atoi(r.URL.Query().Get("category"))
	if err != nil {
		http.Error(w, "bad category", http.StatusBadRequest)
		return
	}
	rows, err := a.db.Query(r.Context(),
		`SELECT id, name, category_id, price_cents, stock FROM products
		 WHERE category_id = $1 ORDER BY price_cents LIMIT 20`, cat)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	products, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Product, error) {
		var p Product
		err := row.Scan(&p.ID, &p.Name, &p.CategoryID, &p.PriceCents, &p.Stock)
		return p, err
	})
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, products)
}

func (a *app) createOrder(w http.ResponseWriter, r *http.Request) {
	var req OrderRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if req.UserID <= 0 || req.ProductID <= 0 || req.Quantity <= 0 {
		http.Error(w, "invalid order", http.StatusUnprocessableEntity)
		return
	}

	ev := OrderEvent{
		ID:        uuid.NewString(),
		UserID:    req.UserID,
		ProductID: req.ProductID,
		Quantity:  req.Quantity,
		CreatedAt: time.Now().UTC(),
	}
	payload, _ := json.Marshal(ev)

	err := a.producer.WriteMessages(r.Context(), kafka.Message{
		Key:   []byte(strconv.FormatInt(ev.UserID, 10)),
		Value: payload,
	})
	if err != nil {
		metrics.OrdersProduced.WithLabelValues("error").Inc()
		http.Error(w, "queue unavailable", http.StatusServiceUnavailable)
		return
	}
	metrics.OrdersProduced.WithLabelValues("ok").Inc()
	writeJSON(w, http.StatusAccepted, map[string]string{"order_id": ev.ID, "status": "accepted"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
