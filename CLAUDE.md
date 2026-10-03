# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A high-load training lab ("Highload Lab"): a deliberately under-tuned Go backend plus Postgres, Kafka, a k6 load generator and a Prometheus/Grafana stack. The student's job is to find and fix the bottlenecks. **The bottlenecks listed in README.md ("Узкие места, оставленные специально") are intentional.** Don't "fix" them unless the user asks you to work on one of those tasks.

All code comments, README and commit-facing text are in Russian. Keep new comments in Russian to match.

## Commands

```bash
docker compose up -d --build                        # everything except k6
docker compose up -d --build backend consumer       # rebuild after Go changes
docker compose run --rm k6                          # load test: RATE=1000 rps, DURATION=5m by default
docker compose run --rm -e RATE=3000 -e DURATION=10m k6
docker compose down -v                              # stop and wipe volumes (needed to re-run postgres/init.sql)
```

Go module lives in `backend/` (module name `highload-lab`, Go 1.24):

```bash
cd backend && go mod tidy        # go.sum is not committed; the Dockerfile runs `go mod tidy` at build time
cd backend && go build ./... && go vet ./...
```

There are no tests and no linter config. The pass/fail check is the k6 thresholds in `k6/load.js`: k6 exits with code 0 when the level is passed.

Endpoints: backend on :8080 (`/catalog/{id}`, `/catalog?category=N`, `POST /orders`, `/healthz`, `/metrics`), consumer metrics on :9100 (container only), Grafana on :3000 (anonymous admin), Prometheus on :9090, k6 REST API on :6565 (pause/resume during a run, see README).

## Architecture

```
k6 ──HTTP──► backend (cmd/server) ──sync──► Postgres (products)
                     └──async──► Kafka topic "orders" ──► consumer (cmd/consumer) ──► Postgres (orders)
```

- One Docker image (`backend/Dockerfile`) builds both binaries, `/app/server` and `/app/consumer`. docker-compose picks one per service via `command`.
- **Resource limits are part of the exercise.** backend gets 1 CPU / 512 MB, consumer gets 0.5 CPU / 256 MB (set in `docker-compose.yml`). Postgres `max_connections=200`. Kafka is a single KRaft broker with 6 partitions by default and auto-created topics.
- **Server** (`cmd/server/main.go`): stdlib `http.ServeMux` with Go 1.22 method+pattern routes, `pgxpool` (size from `DB_MAX_CONNS`), and a `segmentio/kafka-go` `Writer` with a hash balancer keyed by `user_id`, so one user's orders keep their order within a partition. Writes are synchronous with `BatchTimeout: 10ms`. Config comes only from env vars, read through a local `env(key, default)` helper. The helper is duplicated in each `cmd`.
- **Consumer** (`cmd/consumer/main.go`): delivery is at-least-once. It does `FetchMessage` → INSERT → `CommitMessages`. Idempotency comes from the `orders.id` UUID primary key plus `ON CONFLICT DO NOTHING`. Malformed messages are logged and committed (skipped), and there is no DLQ. When an INSERT fails, the offset isn't committed.
- **Metrics** (`internal/metrics`): shared by both binaries. Wrap each new HTTP handler in `metrics.Instrument(routeTemplate, h)` and pass the route *template*, not the raw path, to keep label cardinality low. The k6 script does the same with `tags: { name: ... }`. The Grafana dashboard (`grafana/dashboards/highload-lab.json`) and the k6 thresholds use these names (`http_server_request_duration_seconds`, `orders_produced_total`, `orders_consumed_total`, the k6 tag `name`), so a rename has to be updated in all of them.
- **Schema/data**: `postgres/init.sql` creates `products` (100k rows, 50 categories, intentionally no index on `category_id`) and `orders`. It only runs on the first start of an empty volume, so schema changes need `docker compose down -v`.
- **Observability**: Prometheus scrapes backend, consumer, kafka-exporter, postgres-exporter and cAdvisor every 5s. k6 *pushes* its metrics via remote write (`--web.enable-remote-write-receiver`), so it doesn't appear in the scrape configs. Grafana datasource/dashboard are provisioned from `grafana/provisioning`.

## Caveats from README

- k6 and the backend share the host CPU, so numbers are skewed. For honest measurements, run k6 elsewhere with `TARGET=http://<ip>:8080`.
- cAdvisor often can't see containers on macOS/Docker Desktop, so the "container resources" panels may stay empty.
