import { getProduct, summary } from './lib.js';

const hot = Number(__ENV.HOT_PRODUCT || 1);

export const options = {
  summaryTrendStats: summary.summaryTrendStats,
  scenarios: {
    traffic: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 100),
      duration: __ENV.DURATION || '70s',
    },
  },
};

export default function () {
  const response = getProduct(hot);
  if (response.status >= 500) {
    console.error(`origin error status=${response.status}`);
  }
}
