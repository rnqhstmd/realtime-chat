// FR-3 / FR-4: 라이브 조회 시나리오.
//   대상: GET /sessions/{id}/messages?limit=50 (read model 직접 조회)
//   부하: VU 10, ramp-up 5초 → 30초 지속
//   append 시나리오와 독립 실행 가능(setup에서 자체 세션·데이터 준비).
//
// 참고: projection은 비동기(sync-enabled=false)라 GET /messages는 ProjectionWorker가
// 따라잡아야 데이터가 보인다. 빈 결과여도 read model 조회 경로의 부하/latency 측정은 유효하다.
//
// 실행:
//   k6 run query.js
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 \
//     -v "$(pwd)/load-test/k6:/scripts" grafana/k6 run /scripts/query.js
import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';
import {
  BASE_URL,
  COMMON_THRESHOLDS,
  createSession,
  joinSession,
  seedMessages,
} from './lib/common.js';

// 조회 대상 세션에 사전 적재할 메시지 수(read model이 채워지도록 의미있는 규모).
const QUERY_SEED_EVENTS = Number(__ENV.QUERY_SEED_EVENTS || 300);
const QUERY_LIMIT = Number(__ENV.QUERY_LIMIT || 50);

const queryLatency = new Trend('query_latency', true);

export const options = {
  // setupTimeout은 반드시 options 안에 두어야 k6가 인식한다(모듈 스코프 상수는 무효).
  setupTimeout: '300s',
  scenarios: {
    query: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '5s', target: 10 }, // ramp-up 5초 → VU 10
        { duration: '30s', target: 10 }, // 30초 지속
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: COMMON_THRESHOLDS,
};

// setup: 조회 대상 세션 1개 생성 + 메시지 일부 적재(read model 모집단 확보).
export function setup() {
  const sessionId = createSession();
  const actorId = joinSession(sessionId);
  seedMessages(sessionId, actorId, QUERY_SEED_EVENTS);
  return { sessionId };
}

export default function (data) {
  const res = http.get(
    `${BASE_URL}/sessions/${data.sessionId}/messages?limit=${QUERY_LIMIT}`
  );
  queryLatency.add(res.timings.duration);
  check(res, {
    'messages 200': (r) => r.status === 200,
    'messages is array': (r) => Array.isArray(r.json()),
  });
}
