import { getProduct, getReviews, randomId, summary, writeReview } from './lib.js';

const duration = __ENV.DURATION || '30s';

export const options = {
  summaryTrendStats: summary.summaryTrendStats,
  scenarios: {
    uniform: {
      executor: 'constant-vus',
      vus: Number(__ENV.VUS || 50),
      duration,
    },
  },
};

export default function () {
  const id = randomId();
  const roll = Math.random();
  if (roll < 0.90) {
    getProduct(id);
  } else if (roll < 0.95) {
    getReviews(id);
  } else {
    writeReview(id);
  }
}
