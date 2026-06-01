// FR-1 / FR-4: 이벤트 append 시나리오.
//   대상: POST /sessions/{id}/events (MESSAGE_SENT)
//   부하: VU 10, ramp-up 5초 → 30초 지속(plateau)
//   멱등키는 요청마다 고유 생성(BR-1/AC-8) → 캐시 히트로 인한 TPS 오염 방지.
//
// 실행:
//   k6 run append.js
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 \
//     -v "$(pwd)/load-test/k6:/scripts" grafana/k6 run /scripts/append.js
import { check } from 'k6';
import { Trend } from 'k6/metrics';
import {
  COMMON_THRESHOLDS,
  createSession,
  joinSession,
  appendMessage,
} from './lib/common.js';

// append latency 전용 커스텀 Trend(http_req_duration과 별개로 append만 분리 측정).
const appendLatency = new Trend('append_latency', true);

export const options = {
  scenarios: {
    append: {
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

// setup: 테스트 시작 전 세션 1개 생성 + 참여자 join(FR-1).
export function setup() {
  const sessionId = createSession();
  const actorId = joinSession(sessionId);
  return { sessionId, actorId };
}

export default function (data) {
  const res = appendMessage(data.sessionId, data.actorId);
  appendLatency.add(res.timings.duration);
  check(res, {
    'append 200': (r) => r.status === 200,
    'append has eventId': (r) => !!r.json('eventId'),
  });
}
