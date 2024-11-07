import http from 'k6/http';
import { check } from 'k6';
const scenario = __ENV.SCENARIO || 'throughput';
const path = { throughput: '/api/users/fast', multiple: '/least/fast', rate: '/limited/fast', failure: '/api/users/flaky', timeout: '/api/users/slow' }[scenario];
if (!path) throw new Error('Unknown SCENARIO');
export const options = scenario === 'rate' ? {
  scenarios: { constant: { executor: 'constant-arrival-rate', rate: 500, timeUnit: '1s', duration: '30s', preAllocatedVUs: 100, maxVUs: 200 } },
} : {
  vus: Number(__ENV.VUS || 50), duration: __ENV.DURATION || '30s',
  thresholds: scenario === 'throughput' || scenario === 'multiple' ? { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<500'] } : {},
};
export default function () {
  const r = http.get((__ENV.BASE_URL || 'http://localhost:8080') + path);
  check(r, { 'expected response': (r) => scenario === 'rate' ? [200, 429].includes(r.status) : scenario === 'timeout' ? [503, 504].includes(r.status) : scenario === 'failure' ? [200, 503].includes(r.status) : r.status === 200 });
}
