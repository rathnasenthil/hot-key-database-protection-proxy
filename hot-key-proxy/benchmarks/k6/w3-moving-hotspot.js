import { getProduct, randomId, summary } from './lib.js';

const started = Date.now();
const duration = __ENV.DURATION || '90s';

export const options = {
  summaryTrendStats: summary.summaryTrendStats,
  scenarios: {
    moving: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 200),
      duration,
    },
  },
};

function hotId() {
  const slot = Math.floor((Date.now() - started) / 30000);
  return (slot % 20) + 1;
}

export default function () {
  const id = Math.random() < 0.8 ? hotId() : randomId();
  getProduct(id);
}
