# PR Context — Phase 3

## 비즈니스 맥락

Phase 1·2로 이벤트 소싱 + CQRS 완전 비동기 파이프라인(수집→Outbox→Redis Stream→Projection Worker→read model→STOMP broadcast)이 **단일 인스턴스 기준** 완성되었다. Phase 3는 과제 평가 기준 미충족 항목(FR-12 재연결 정합성, FR-13 수평 확장, FR-14 관측성)을 완성한다.

**구현 범위 (4개 영역):**
1. **수평 확장 팬아웃 (FR-P3-1)** — Redis Pub/Sub 백플레인. afterCommit broadcast를 `chat.fanout.{sessionId}` publish/subscribe로 전환하여 다중 인스턴스의 어느 노드에 연결된 참여자에게도 이벤트가 전달된다. `chat.fanout.mode` 프로퍼티로 local(기본·단일 인스턴스)/redis 스위치. `SimpSessionBroadcaster`(로컬) 보존, `RedisPubSubSessionBroadcaster` 신규 — `@ConditionalOnProperty`로 한 시점에 한 구현체만 등록(이중 전달 금지 불변식).
2. **presence TTL 자동 만료 (FR-P3-2)** — heartbeat를 **경량 ping**(이벤트 아님)으로 받아 Redis `presence:{sid}:{pid}` 키 TTL을 갱신. 비정상 종료 시 키가 만료되면 `@Scheduled` sweep이 감지하여 OFFLINE 이벤트를 자동 수집. 중복 방지는 Redis tracked Set의 **SREM 원자 반환값**으로 정확히 1회 보장(read model lag 무관). PRESENCE_CHANGED는 상태 전이(ONLINE 진입/OFFLINE)에만 발행하여 이벤트 스토어 오염을 방지.
3. **재연결 delta 조회 (FR-P3-3)** — `GET /sessions/{id}/events?afterSeq={seq}&limit={n}`로 마지막 수신 seq 이후 raw 이벤트만 선택 수신. `hasMore` 페이징 신호 포함. timeline(fold된 상태)과 구별되는 raw 이벤트 delta.
4. **관측성 (FR-P3-4/5/6)** — Micrometer Gauge(projection lag)/Counter(DLQ·stream·outbox 처리량), Actuator `/health`·`/info`·`/prometheus`, MDC(sessionId/seq/eventType) 구조화 로그.

**검증:** 수용 기준 AC-1~15 전부 통합/단위 테스트로 커버. 전체 테스트 **77 tests / 0 failures** (기존 Phase 1·2 회귀 포함). product-owner 인수 검증 ACCEPT.

**설계 단계 승인된 범위 조정:**
- heartbeat = 경량 ping(이벤트 아님)으로 재정의 — 이벤트 스토어 오염·last_seq 폭증 방지(critic 지적).
- BR-3 OFFLINE 중복 방지 = read model 확인 대신 Redis SREM 원자 판정 — read model lag 권위 부적합 해소.
- STOMP `SessionDisconnectEvent` 즉시 OFFLINE은 1차 제외(폴링 sweep로 충족) → Phase 4.

## Audit Summary
- 총 12건 (CRITICAL: 1, HIGH: 5, MEDIUM: 5, LOW: 2) — 상세는 Trust Ledger 참조.
- ✅ **조치 완료(리뷰 하드닝 4건)**: resume `afterSeq<0`→400, STOMP heartbeat 세션 검증·null 가드, SessionFanoutListener 채널 sessionId UUID 검증.
- 📋 **범위 외 기록(인증 미구현 — 과제 범위)**: Actuator 무인증 노출 + `show-details: always`(운영 전 management port 분리/Security 필요), OFFLINE actorId=클라이언트 participantId.
- 📋 **후속 기록**: presence:tracked 전역 Set 규모, consumer 이름 유일성, OFFLINE 멱등키 재시작 중복, fanout 역직렬화 실패 메트릭.
