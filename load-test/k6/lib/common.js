// 공통 헬퍼/상수 — 모든 k6 시나리오가 import해서 재사용한다.
//
// 제약(오프라인 docker 실행):
//   - 외부 jslib(https://jslib.k6.io/...) import 금지. k6 내장 모듈만 사용.
//   - 멱등키는 UUID 라이브러리 없이 요청마다 고유한 문자열을 인라인 생성한다.
//     서버 검증은 "공백 아님 + 200자 이하"만 요구하므로(BR-1/AC-8) UUID일 필요는 없고
//     '요청마다 고유'하면 충분하다.
import http from 'k6/http';
import exec from 'k6/execution';
import { check, fail } from 'k6';

// BASE_URL은 이 1곳에서만 정의한다(BR-3). 앱은 8081 포트로 기동되며,
// docker 실행 시 -e BASE_URL=http://host.docker.internal:8081 로 주입한다.
export const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';

// 모든 시나리오가 공유하는 임계치(thresholds). 부하 도구로서 합/불 판정 기준을 통일한다.
export const COMMON_THRESHOLDS = {
  // 실패율 1% 미만(BR-4: 1% 초과 시 결과 문서에 원인 분석 필요).
  http_req_failed: ['rate<0.01'],
  // p95 응답시간 가시화(절대 SLA가 아닌 측정 기준치 — 측정 후 문서에 실측 기재).
  http_req_duration: ['p(95)<2000'],
};

const HEADER_IDEMPOTENCY = 'Idempotency-Key';

// 멱등키 인라인 생성용 카운터(모듈 스코프 — VU별 인스턴스가 분리되어 충돌 없음).
let idemCounter = 0;

/**
 * UUID v4 문자열을 생성한다(k6 내장 crypto 없이 Math.random 기반).
 * participantId/senderId/actorId는 서버에서 UUID로 역직렬화되므로 유효한 UUID여야 한다(non-UUID면 400).
 */
export function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/**
 * 요청마다 고유한 멱등키를 생성한다(BR-1/AC-8).
 * setup()은 VU 컨텍스트 밖이라 exec.vu/exec.scenario 접근 시 예외가 발생하므로 try/catch로 0 폴백한다.
 * 고유성은 모듈 스코프 카운터(n) + Date.now()로 보장된다(setup의 순차 적재 포함, 같은 ms도 n으로 분리).
 */
export function uniqueIdempotencyKey(prefix = 'idem') {
  let vu = 0;
  let iter = 0;
  try { vu = exec.vu.idInTest; } catch (e) { /* setup context: VU 정보 없음 */ }
  try { iter = exec.scenario.iterationInTest; } catch (e) { /* setup context */ }
  const n = idemCounter++;
  return `${prefix}-${vu}-${iter}-${n}-${Date.now()}`;
}

/** JSON 본문 + 멱등키 헤더를 포함한 표준 POST 헤더. */
export function jsonHeaders(idempotencyKey) {
  const headers = { 'Content-Type': 'application/json' };
  if (idempotencyKey) {
    headers[HEADER_IDEMPOTENCY] = idempotencyKey;
  }
  return headers;
}

/**
 * 세션을 1개 생성한다(POST /sessions). 201 + { sessionId } 응답.
 * setup() 단계에서만 호출하는 것을 전제로 하며, 실패 시 즉시 fail로 중단한다.
 */
export function createSession() {
  const res = http.post(`${BASE_URL}/sessions`, null);
  if (!check(res, { 'create session 201': (r) => r.status === 201 })) {
    fail(`createSession failed: status=${res.status} body=${res.body}`);
  }
  return res.json('sessionId');
}

/**
 * 참여자를 세션에 join한다(POST /sessions/{id}/join). Idempotency-Key 헤더 필수.
 * @returns join 이벤트에 사용한 participantId(이후 append의 senderId/actorId로 재사용 가능).
 */
export function joinSession(sessionId, participantId) {
  // participantId는 서버에서 UUID로 역직렬화되므로 유효한 UUID여야 한다(non-UUID면 400).
  const pid = participantId || uuidv4();
  const res = http.post(
    `${BASE_URL}/sessions/${sessionId}/join`,
    JSON.stringify({ participantId: pid }),
    { headers: jsonHeaders(uniqueIdempotencyKey('join')) }
  );
  if (!check(res, { 'join 2xx': (r) => r.status >= 200 && r.status < 300 })) {
    fail(`joinSession failed: status=${res.status} body=${res.body}`);
  }
  return pid;
}

/**
 * MESSAGE_SENT 이벤트 1건을 append한다(POST /sessions/{id}/events).
 * content 필수(공백 불가), senderId 없으면 actorId로 폴백되므로 둘 다 actorId로 통일한다.
 * @returns k6 http response(호출부에서 check/Trend 기록).
 */
export function appendMessage(sessionId, actorId, content) {
  const body = JSON.stringify({
    type: 'MESSAGE_SENT',
    payload: {
      content: content || `msg-${uniqueIdempotencyKey('c')}`,
      senderId: actorId,
    },
    actorId: actorId,
  });
  return http.post(`${BASE_URL}/sessions/${sessionId}/events`, body, {
    headers: jsonHeaders(uniqueIdempotencyKey('idem')),
  });
}

/**
 * 세션에 메시지를 N건 사전 적재한다(setup 단계 전용). 적재 진행 상황을 로그로 남긴다.
 * restore/query/resume 시나리오의 사전 데이터 구성에 사용한다.
 */
export function seedMessages(sessionId, actorId, count) {
  let ok = 0;
  for (let i = 0; i < count; i++) {
    const res = appendMessage(sessionId, actorId, `seed-${i}`);
    if (res.status === 200) {
      ok++;
    }
  }
  return ok;
}
