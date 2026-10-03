// Package metrics — серверные метрики, которые видит Prometheus.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_server_request_duration_seconds",
		Help:    "Латентность обработки запроса на стороне сервера.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .2, .5, 1, 2.5, 5},
	}, []string{"route", "method", "status"})

	HTTPInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "http_server_requests_in_flight",
		Help: "Запросы, которые сейчас обрабатываются.",
	})

	OrdersProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orders_produced_total",
		Help: "Заказы, отправленные в Kafka (result=ok|error).",
	}, []string{"result"})

	OrdersConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "orders_consumed_total",
		Help: "Заказы, прочитанные consumer'ом (result=stored|duplicate|error).",
	}, []string{"result"})
)

// Handler отдаёт /metrics. Метрики Go-рантайма (горутины, GC, память) входят по умолчанию.
func Handler() http.Handler { return promhttp.Handler() }

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Instrument оборачивает хендлер замером латентности. route — шаблон пути,
// а не реальный URL, иначе /catalog/1 … /catalog/100000 дадут 100k серий.
func Instrument(route string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		HTTPInFlight.Inc()
		defer HTTPInFlight.Dec()
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next(rec, r)
		HTTPDuration.WithLabelValues(route, r.Method, strconv.Itoa(rec.status)).
			Observe(time.Since(start).Seconds())
	}
}
