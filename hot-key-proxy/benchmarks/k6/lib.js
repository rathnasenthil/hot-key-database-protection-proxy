import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';

export const BASE = __ENV.BASE_URL || 'http://localhost:8080';
export const PRODUCTS = Number(__ENV.PRODUCTS || 100000);
export const measured = new Trend('measured_latency', true);

export function randomId() {
  return Math.floor(Math.random() * PRODUCTS) + 1;
}

export function getProduct(id) {
  const response = http.get(`${BASE}/api/products/${id}`, { tags: { name: 'get_product' } });
  check(response, { 'not 5xx': (r) => r.status < 500 });
  measured.add(response.timings.duration);
  return response;
}

export function getReviews(id) {
  const response = http.get(`${BASE}/api/products/${id}/reviews`, { tags: { name: 'get_reviews' } });
  check(response, { 'not 5xx': (r) => r.status < 500 });
  measured.add(response.timings.duration);
  return response;
}

export function writeReview(id) {
  const response = http.post(
    `${BASE}/api/products/${id}/reviews`,
    JSON.stringify({ userId: 1, rating: 5, comment: 'benchmark' }),
    { headers: { 'Content-Type': 'application/json' }, tags: { name: 'write_review' } }
  );
  check(response, { 'write accepted': (r) => r.status === 200 || r.status === 429 });
  measured.add(response.timings.duration);
  return response;
}

export const summary = {
  summaryTrendStats: ['avg', 'p(95)', 'p(99)', 'count'],
};
