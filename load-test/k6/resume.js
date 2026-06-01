// FR-8 (Should): 재연결 delta 시나리오.
//   대상: GET /sessions/{id}/events?afterSeq=0&limit=100 (event store raw delta 페이징)
//   부하: VU 10, ramp-up 5초 → 30초 지속
//   afterSeq는 필수 파라미터(누락 시 400). 전체 이벤트 페이징 응답 latency 측정.
//
// 실행:
//   k6 run resume.js
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 \
//     -v "$(pwd)/load-test/k6:/scripts" grafana/k6 run /scripts/resume.js
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

// delta 모집단 확보용 사전 적재 수(페이징 응답이 비지 않도록 limit 이상으로 적재).
const RESUME_SEED_EVENTS = Number(__ENV.RESUME_SEED_EVENTS || 300);
const RESUME_AFTER_SEQ = Number(__ENV.RESUME_AFTER_SEQ || 0);
const RESUME_LIMIT = Number(__ENV.RESUME_LIMIT || 100);

const resumeLatency = new Trend('resume_latency', true);

export const options = {
  // setupTimeout은 반드시 options 안에 두어야 k6가 인식한다(모듈 스코프 상수는 무효).
  setupTimeout: '300s',
  scenarios: {
    resume: {
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

// setup: delta 조회 대상 세션 1개 생성 + 이벤트 사전 적재(event store 모집단).
export function setup() {
  const sessionId = createSession();
  const actorId = joinSession(sessionId);
  seedMessages(sessionId, actorId, RESUME_SEED_EVENTS);
  return { sessionId };
}

export default function (data) {
  const res = http.get(
    `${BASE_URL}/sessions/${data.sessionId}/events?afterSeq=${RESUME_AFTER_SEQ}&limit=${RESUME_LIMIT}`
  );
  resumeLatency.add(res.timings.duration);
  check(res, {
    'events 200': (r) => r.status === 200,
    'events has list': (r) => Array.isArray(r.json('events')),
  });
}
