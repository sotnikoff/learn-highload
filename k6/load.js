// Нагрузка на бэкенд ученика. Открытая модель: фиксированный поток запросов,
// не зависящий от того, успевает сервер или нет.
//
// Параметры через env:
//   RATE      — итераций (= запросов) в секунду, по умолчанию 1000
//   DURATION  — длительность, по умолчанию 5m
//   TARGET    — адрес бэкенда
//
// Микс: 80% чтение товара, 10% список по категории, 10% создание заказа (async).

import http from 'k6/http';
import { check } from 'k6';

const TARGET = __ENV.TARGET || 'http://localhost:8080';
const RATE = parseInt(__ENV.RATE || '1000', 10);
const DURATION = __ENV.DURATION || '5m';

export const options = {
  scenarios: {
    open_load: {
      executor: 'ramping-arrival-rate',
      startRate: Math.max(1, Math.floor(RATE / 10)),
      timeUnit: '1s',
      preAllocatedVUs: Math.min(RATE, 2000),
      maxVUs: 10000,
      stages: [
        { target: RATE, duration: '30s' }, // разгон
        { target: RATE, duration: DURATION }, // плато
      ],
    },
  },
  // SLO «уровня». Нарушение → k6 завершится с ненулевым кодом.
  thresholds: {
    'http_req_duration{name:GET /catalog/{id}}': ['p(99)<100'],
    'http_req_duration{name:POST /orders}': ['p(99)<200'],
    http_req_failed: ['rate<0.001'],
    dropped_iterations: ['count<100'], // сервер не успевал — k6 не смог отправить запрос вовремя
  },
  // Не держать ответы в памяти — на 10k VU это гигабайты.
  discardResponseBodies: true,
};

const JSON_HEADERS = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const roll = Math.random();

  if (roll < 0.8) {
    const id = 1 + Math.floor(Math.random() * 100000);
    // tag name — чтобы 100k разных URL не превратились в 100k серий метрик
    const res = http.get(`${TARGET}/catalog/${id}`, { tags: { name: 'GET /catalog/{id}' } });
    check(res, { 'catalog 200': (r) => r.status === 200 });
  } else if (roll < 0.9) {
    const cat = 1 + Math.floor(Math.random() * 50);
    const res = http.get(`${TARGET}/catalog?category=${cat}`, { tags: { name: 'GET /catalog?category' } });
    check(res, { 'category 200': (r) => r.status === 200 });
  } else {
    const body = JSON.stringify({
      user_id: 1 + Math.floor(Math.random() * 10000), // те самые 10 000 пользователей
      product_id: 1 + Math.floor(Math.random() * 100000),
      quantity: 1 + Math.floor(Math.random() * 3),
    });
    const res = http.post(`${TARGET}/orders`, body, { ...JSON_HEADERS, tags: { name: 'POST /orders' } });
    check(res, { 'order 202': (r) => r.status === 202 });
  }
}
