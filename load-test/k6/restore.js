// FR-2 / FR-4: 시점 복원(timeline) latency 시나리오.
//   대상: GET /sessions/{id}/timeline (at 미지정 = 현재 시점 = snapshot+delta replay)
//   부하: VU 5, ramp-up 5초 → 30초 지속
//   두 케이스를 그룹으로 구분 측정:
//     - 스냅샷 無: trigger-interval(200) 미만으로 소규모 적재 + 명시적 스냅샷 미호출 → 전체 replay 경로.
//     - 스냅샷 有: 1,000건 이상 적재(BR-2) 후 POST /snapshots로 스냅샷 보장 → snapshot+delta 경로.
//
// 스냅샷 동작(소스 확인): 자동 스냅샷은 비동기 ProjectionApplier(OutboxRelay→ProjectionWorker)
// 경로에서만 트리거된다. projection은 sync-enabled=false라 비동기다. "스냅샷 無" 케이스를
// 결정적으로 만들기 위해 trigger-interval 미만(소규모)으로 적재하고 명시적 스냅샷을 호출하지 않는다.
// GET /timeline은 event store에서 직접 replay하므로 projection 진행과 무관하게 즉시 데이터가 있다.
//
// 실행:
//   k6 run restore.js
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 \
//     -v "$(pwd)/load-test/k6:/scripts" grafana/k6 run /scripts/restore.js
import http from 'k6/http';
import { check, group } from 'k6';
import { Trend } from 'k6/metrics';
import {
  BASE_URL,
  COMMON_THRESHOLDS,
  createSession,
  joinSession,
  seedMessages,
} from './lib/common.js';

// 스냅샷 無 케이스 사전 적재량(trigger-interval=200 미만으로 자동 스냅샷 회피).
const NO_SNAPSHOT_EVENTS = Number(__ENV.NO_SNAPSHOT_EVENTS || 150);
// 스냅샷 有 케이스 사전 적재량(BR-2: 1,000건 이상).
const SNAPSHOT_EVENTS = Number(__ENV.SNAPSHOT_EVENTS || 1200);

// 케이스별 timeline latency 분리 측정.
const restoreNoSnapshot = new Trend('restore_no_snapshot_latency', true);
const restoreWithSnapshot = new Trend('restore_with_snapshot_latency', true);

export const options = {
  // setup에서 150+1,200건을 순차 적재하므로 기본 60초로는 부족 → 넉넉히 확장(BR-2).
  // setupTimeout은 반드시 options 안에 두어야 k6가 인식한다(모듈 스코프 상수는 무효).
  setupTimeout: '600s',
  scenarios: {
    restore: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '5s', target: 5 }, // ramp-up 5초 → VU 5
        { duration: '30s', target: 5 }, // 30초 지속
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: COMMON_THRESHOLDS,
};

// setup: 두 세션을 준비한다(스냅샷 無/有). setupTimeout은 위 options에 설정한다.
export function setup() {
  // 케이스 1: 스냅샷 無 — 소규모 적재, 명시적 스냅샷 미호출.
  const noSnapId = createSession();
  const noSnapActor = joinSession(noSnapId);
  seedMessages(noSnapId, noSnapActor, NO_SNAPSHOT_EVENTS);

  // 케이스 2: 스냅샷 有 — 1,000건 이상 적재 후 명시적 스냅샷 트리거.
  const snapId = createSession();
  const snapActor = joinSession(snapId);
  seedMessages(snapId, snapActor, SNAPSHOT_EVENTS);
  // 명시적 스냅샷 보장(POST /snapshots → 202). 자동 스냅샷(비동기)에 의존하지 않는다.
  const snapRes = http.post(`${BASE_URL}/sessions/${snapId}/snapshots`, null);
  check(snapRes, { 'snapshot 202': (r) => r.status === 202 });

  return { noSnapId, snapId };
}

export default function (data) {
  group('restore - no snapshot (full replay)', () => {
    const res = http.get(`${BASE_URL}/sessions/${data.noSnapId}/timeline`);
    restoreNoSnapshot.add(res.timings.duration);
    check(res, { 'timeline(no-snap) 200': (r) => r.status === 200 });
  });

  group('restore - with snapshot (snapshot+delta)', () => {
    const res = http.get(`${BASE_URL}/sessions/${data.snapId}/timeline`);
    restoreWithSnapshot.add(res.timings.duration);
    check(res, { 'timeline(snap) 200': (r) => r.status === 200 });
  });
}
