import http from 'k6/http';
import { check } from 'k6';
import { BASE, measured, randomId, summary } from './lib.js';

export const options = {
  summaryTrendStats: summary.summaryTrendStats,
  scenarios: {
    overload: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 400),
      duration: __ENV.DURATION || '20s',
    },
  },
};

export default function () {
  const response = http.get(`${BASE}/api/products/${randomId()}`);
  check(response, { 'served or shed': (r) => r.status === 200 || r.status === 429 });
  measured.add(response.timings.duration);
}
