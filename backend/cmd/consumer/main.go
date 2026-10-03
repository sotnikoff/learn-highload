// Consumer: читает заказы из Kafka и пишет в Postgres.
//
// Пишет по одному INSERT на сообщение — это медленно на нагрузке, и consumer lag
// будет расти. Задание: батчевая вставка (COPY / multi-row INSERT), несколько
// воркеров, корректный коммит офсетов, DLQ для «битых» сообщений.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"highload-lab/internal/metrics"
)

type OrderEvent struct {
	ID        string    `json:"id"`
	UserID    int64     `json:"user_id"`
	ProductID int64     `json:"product_id"`
	Quantity  int       `json:"quantity"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, env("DATABASE_URL", "postgres://lab:lab@localhost:5432/lab"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{env("KAFKA_BROKERS", "localhost:9092")},
		Topic:    env("ORDERS_TOPIC", "orders"),
		GroupID:  env("CONSUMER_GROUP", "orders-writer"),
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,
	})
	defer reader.Close()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		log.Println("consumer metrics on :9100")
		_ = http.ListenAndServe(":9100", mux)
	}()

	log.Println("consumer started")
	for {
		// FetchMessage + явный CommitMessages: офсет коммитится только после записи в БД
		// (at-least-once). Дубли гасит PRIMARY KEY + ON CONFLICT DO NOTHING.
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("fetch: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var ev OrderEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			// «Ядовитое» сообщение: логируем и пропускаем. В идеале — в DLQ-топик.
			log.Printf("bad message at offset %d: %v", msg.Offset, err)
			metrics.OrdersConsumed.WithLabelValues("error").Inc()
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		tag, err := db.Exec(ctx,
			`INSERT INTO orders (id, user_id, product_id, quantity, created_at)
			 VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING`,
			ev.ID, ev.UserID, ev.ProductID, ev.Quantity, ev.CreatedAt)
		if err != nil {
			// Не коммитим офсет — после рестарта сообщение придёт снова.
			log.Printf("insert: %v", err)
			metrics.OrdersConsumed.WithLabelValues("error").Inc()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if tag.RowsAffected() == 0 {
			metrics.OrdersConsumed.WithLabelValues("duplicate").Inc()
		} else {
			metrics.OrdersConsumed.WithLabelValues("stored").Inc()
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit: %v", err)
		}
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
