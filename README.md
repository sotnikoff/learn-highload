# Highload Lab — тренажёр хай-лоада (вертикальный срез)

```
k6 ──HTTP──► backend (Go, 1 CPU / 512 MB) ──sync──► Postgres
                     │
                     └──async──► Kafka ──► consumer ──► Postgres

Prometheus ◄── backend, consumer, kafka-exporter, postgres-exporter, cAdvisor
           ◄── k6 (remote write)
Grafana    ──► дашборд «Highload Lab»
```

## Запуск

```bash
docker compose up -d --build      # всё, кроме генератора нагрузки
docker compose run --rm k6        # нагрузка: 1000 RPS, 5 минут
```

- Grafana: http://localhost:3000 — откроется дашборд «Highload Lab», логин не нужен
- Prometheus: http://localhost:9090
- Бэкенд: http://localhost:8080/catalog/42

Поменять нагрузку:

```bash
docker compose run --rm -e RATE=3000 -e DURATION=10m k6
```

Пауза или продолжение во время теста (в соседнем терминале, через REST API k6):

```bash
curl -X PATCH localhost:6565/v1/status -H 'Content-Type: application/json' \
  -d '{"data":{"type":"status","id":"default","attributes":{"paused":true}}}'
```

Остановить и всё стереть: `docker compose down -v`.

## Что в бэкенде

| Эндпоинт | Тип | Что делает |
|---|---|---|
| `GET /catalog/{id}` | sync | товар по PK из Postgres |
| `GET /catalog?category=N` | sync | топ-20 по категории (**индекса нет**) |
| `POST /orders` | async | валидация → Kafka (ключ = user_id) → `202 Accepted` |

Consumer читает топик `orders` и пишет в таблицу `orders`. Режим at-least-once: офсет коммитится после INSERT, дубли гасит `ON CONFLICT DO NOTHING`.

## Узкие места, оставленные специально (задания)

1. **Нет индекса** по `products.category_id`. Найти по p99 в разрезе эндпоинтов и `EXPLAIN ANALYZE`.
2. **Нет кэша** для `/catalog/{id}`. Добавить Redis или in-process LRU и сравнить нагрузку на Postgres.
3. **Пул БД = 20 соединений** (`DB_MAX_CONNS`). Найти точку, где запросы стоят в очереди за соединением.
4. **Синхронная запись в Kafka** с `BatchTimeout: 10ms`: каждый `POST /orders` ждёт батч. Попробовать async-продюсер и подумать, что теряется.
5. **Consumer по одному INSERT**: lag растёт уже на умеренной нагрузке. Батчи, COPY, несколько воркеров.
6. **«Ядовитые» сообщения** просто пропускаются. Сделать DLQ-топик.
7. **Лимиты ОС**: поднять RATE до 10 000 и посмотреть, что упадёт первым.

## Цели уровня (thresholds в `k6/load.js`)

- p99 `GET /catalog/{id}` < 100 мс
- p99 `POST /orders` < 200 мс
- ошибок < 0.1%
- dropped_iterations < 100

Если k6 завершился с кодом 0, уровень пройден. Следующий шаг — поднять `RATE`.

## Важно про цифры

k6 и бэкенд на одной машине конкурируют за CPU. Для честных замеров запускайте k6 на другой машине (`TARGET=http://<ip>:8080`) или хотя бы следите на дашборде за CPU контейнера k6.

cAdvisor на macOS / Docker Desktop может не видеть контейнеры. Тогда панели «Ресурсы контейнеров» останутся пустыми, остальное будет работать.
