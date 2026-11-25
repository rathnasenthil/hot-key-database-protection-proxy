import { getProduct, randomId, summary } from './lib.js';

const duration = __ENV.DURATION || '45s';
const hot = Number(__ENV.HOT_PRODUCT || 1);

export const options = {
  summaryTrendStats: summary.summaryTrendStats,
  scenarios: {
    background: {
      executor: 'constant-vus',
      vus: Number(__ENV.BACKGROUND_VUS || 20),
      duration,
      exec: 'background',
    },
    spike: {
      executor: 'constant-vus',
      vus: Number(__ENV.SPIKE_VUS || 1000),
      duration,
      startTime: '5s',
      exec: 'spike',
    },
  },
};

export function spike() {
  getProduct(hot);
}

export function background() {
  getProduct(randomId());
}
