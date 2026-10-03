-- Каталог для синхронных запросов: 100 000 товаров.
CREATE TABLE products (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT        NOT NULL,
    category_id INT         NOT NULL,
    price_cents BIGINT      NOT NULL,
    stock       INT         NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO products (name, category_id, price_cents, stock)
SELECT 'Product #' || g,
       (random() * 50)::int + 1,
       (random() * 100000)::bigint + 100,
       (random() * 1000)::int
FROM generate_series(1, 100000) AS g;

-- Индекса по category_id специально нет: найти и добавить его — одно из заданий.

-- Заказы пишет consumer из Kafka.
CREATE TABLE orders (
    id          UUID        PRIMARY KEY,          -- идемпотентность: повтор сообщения не создаст дубль
    user_id     BIGINT      NOT NULL,
    product_id  BIGINT      NOT NULL,
    quantity    INT         NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    stored_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
