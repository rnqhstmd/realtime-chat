# 설계서: Phase 2 — 비동기 파이프라인 (Outbox → Redis Stream → Projection Worker)

> 확정일: 2026-05-29 · 브랜치: feat/realtime-chat-phase2 · 설계 규모: 대형
> 확정 결정(사용자): Q1 수동 XREADGROUP 단일 루프 / Q2 min-idle 근사 백오프 / Q3 스냅샷 afterCommit 별도 TX / Q4 RedisContainer+@ServiceConnection / Q5 POC 단일 인스턴스 + consumer 호스트명 유일
> design-critic 정정 5건 전부 반영

## 배경 및 목적
- 현재: `CommandHandler.handle()`이 한 TX에서 `append → projectionUpdater.apply()(동기) → afterCommit broadcast`. `outbox` 테이블 미사용. Redis 미연결.
- 목적: ① Transactional Outbox 원자성, ② 동기 projection 제거 → Redis Stream 비동기 projection이 read model 갱신 유일 경로, ③ 재시도/DLQ 격리, ④ Snapshot 자동화로 복원 비용 상한, ⑤ 기존 39 통합테스트 Awaitility로 그린 유지.
- 핵심 불변식: event store가 진실의 원천. 스트림은 "알림", 순서·완전성 권위는 event store(FR-P2-7). 복원(timeline)은 lag 무관 정확(BR-6).
- 운영 전제(Q5): **POC 단일 인스턴스**. 다중 인스턴스 확장은 Phase 3+. consumer 이름은 호스트명 기반 유일 부여(재기동 시 pending 추적 일관).

## 변경 범위

### 신규 생성 파일 (13)
- `src/main/resources/db/migration/V2__async_pipeline.sql` (outbox 본문 컬럼 + projection_offset)
- `src/main/java/com/realtimechat/async/RedisConfig.java`
- `src/main/java/com/realtimechat/async/StreamConstants.java`
- `src/main/java/com/realtimechat/async/StreamProps.java` (@ConfigurationProperties `chat.redis`)
- `src/main/java/com/realtimechat/async/EventStreamCodec.java`
- `src/main/java/com/realtimechat/async/RedisStreamInitializer.java`
- `src/main/java/com/realtimechat/outbox/OutboxDao.java`
- `src/main/java/com/realtimechat/outbox/OutboxRecord.java`
- `src/main/java/com/realtimechat/outbox/OutboxRelay.java`
- `src/main/java/com/realtimechat/projection/async/ProjectionWorker.java` (단일 루프: XREADGROUP + XAUTOCLAIM + DLQ)
- `src/main/java/com/realtimechat/projection/async/ProjectionApplier.java` (자체 @Transactional: gap-fill + offset + seq-guard)
- `src/main/java/com/realtimechat/projection/async/ProjectionOffsetDao.java`
- `src/test/java/com/realtimechat/integration/AsyncPipelineIntegrationTest.java`
- `src/test/java/com/realtimechat/support/AwaitProjection.java`

### 수정 대상 파일 (9)
- `EventStore.java` (append에 outbox 본문 INSERT; OutboxDao 주입)
- `CommandHandler.java` (동기 projection 제거 + sync-enabled 분기 + @PostConstruct 경고)
- `ProjectionUpdater.java` (javadoc 갱신; apply 시그니처 유지)
- `SessionViewDao.java` (TOUCH_ACTIVITY_SQL 단조 가드)
- `ParticipantViewDao.java` (applyPresence 늦은 도착 처리 — javadoc만)
- `RealtimeChatApplication.java` (`@EnableScheduling`, `@ConfigurationPropertiesScan`)
- `build.gradle.kts`, `application.yml`, `application-test.yml`
- `AbstractIntegrationTest.java` (Redis RedisContainer + @ServiceConnection + flyway 테이블 검증 projection_offset)
- `EventSourcingIntegrationTest.java`, `RestApiIntegrationTest.java` (read-after-write → Awaitility). `WebSocketStompIntegrationTest.java`는 수정 불요.

## 적용 컨벤션
- 순수 Java(Lombok 미사용), 생성자 주입(final), record DTO.
- DAO: `@Repository` + `NamedParameterJdbcTemplate` + private static final SQL 상수 + `MapSqlParameterSource`. DAO에 @Transactional 없음(호출자 TX 가정).
- @Transactional은 서비스 경계에만. Phase 2에서 `ProjectionApplier.apply()`가 새 TX 경계.
- JSONB: `CAST(:col AS jsonb)`. 직렬화: `JsonUtil` 단일 진입점. 시각/식별자: `Instant`/`UUID`.
- seq-guard: DAO UPDATE `WHERE :seq > last_applied_seq`. INSERT `ON CONFLICT DO NOTHING`.
- afterCommit: `TransactionSynchronizationManager`(동기화 비활성 시 즉시 실행 폴백). 스냅샷 트리거도 동일.
- 테스트: 싱글톤 Testcontainer(static, stop 안 함). 세션 단위 데이터 격리.

## 상세 설계

### 1. EventStore.append — outbox 본문 INSERT (FR-P2-1, BR-1, critic ④)
- `(b)` 경로 event INSERT 성공(isNew=true) 직후 같은 TX에서 outbox INSERT 1건. **본문(event_type, payload, idempotency_key, actor_id, occurred_at) 전체 기록**.
- 이유(본문 self-contained): Relay가 event 재조회 없이 outbox만으로 XADD → 재조회 부하 + visibility race 제거. occurred_at은 event INSERT RETURNING 값 사용 → 본문 일관.
- 이유(isNew=true에만): "event 1건↔outbox 1건" 불변식, BR-1 원자성. `(a)/(c)` 멱등 재유입은 미기록.
- 시그니처: `OutboxDao.insert(StoredEvent e)`. EventStore 생성자에 OutboxDao 추가.

### 2. CommandHandler.handle — 동기 projection 제거 + sync-enabled 분기 (FR-P2-2, FR-P2-9)
```
if (result.isNew()) {
    if (syncProjectionEnabled) {   // 기본 false (테스트/디버깅 전용)
        projectionUpdater.apply(stored);
    }
    publishAfterCommit(sessionId, stored);
}
```
- `@Value("${chat.projection.sync-enabled:false}")`. ProjectionUpdater 의존 유지.
- @PostConstruct에서 sync=true이면 WARN 로그("운영 사용 금지, 이중 적용 위험"). broadcast는 afterCommit 유지(단일 발행 경로).

### 3. V2 마이그레이션 (outbox 본문 + projection_offset)
```sql
-- src/main/resources/db/migration/V2__async_pipeline.sql
ALTER TABLE outbox
    ADD COLUMN event_type      TEXT,
    ADD COLUMN payload         JSONB,
    ADD COLUMN idempotency_key TEXT,
    ADD COLUMN actor_id        UUID,
    ADD COLUMN occurred_at     TIMESTAMPTZ;
-- nullable 추가. Phase 2 이후 INSERT는 항상 본문 채움. POC 신규 전개라 backfill 불요.

CREATE TABLE projection_offset (
    session_id            UUID PRIMARY KEY,
    last_applied_seq      BIGINT NOT NULL DEFAULT 0,  -- gap 감지 기준(스트림 seq > last+1 → gap)
    events_since_snapshot BIGINT NOT NULL DEFAULT 0,  -- 마지막 스냅샷 이후 적용 건수(N건 트리거)
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
- projection_offset은 gap 감지 + 스냅샷 카운팅 **전용**(카운터 가드 아님 — critic ①). per-row guard로는 gap·카운팅 불가, PRESENCE_CHANGED는 행 미증가.
- touchActivity 단조 가드는 DDL이 아니라 SessionViewDao SQL 변경(§5).
- outbox attempt 컬럼 없음: 재시도는 Redis deliveryCount로 추적.

### 4. Redis 인프라 (build.gradle, application.yml, RedisConfig, StreamProps)
- build.gradle.kts: `spring-boot-starter-data-redis`(main), `org.testcontainers:redis`(test), `org.awaitility:awaitility`(test).
- application.yml:
```yaml
spring:
  data:
    redis: { host: localhost, port: 6379 }
chat:
  outbox:
    relay: { poll-interval-ms: 500, batch-size: 100 }
  projection: { sync-enabled: false }
  snapshot: { trigger-interval: 200 }
  redis:
    stream: events
    group: proj
    dlq-stream: events:dlq
    consumer: ${HOSTNAME:worker-1}   # Q5 호스트명 유일
    max-attempts: 3
    claim-min-idle-ms: 30000
    read-block-ms: 2000
    read-count: 50
    stream-maxlen: 100000            # critic ⑤ XADD MAXLEN trim
```
- StreamProps `@ConfigurationProperties("chat.redis")`. RedisConfig: `StringRedisTemplate`(필드맵 stream 연산). Lettuce 자동구성.

### 5. SessionViewDao.touchActivity — 단조 가드 (critic ②)
```sql
UPDATE session_view
   SET last_activity_at = :at
 WHERE session_id = :sid
   AND CAST(:at AS timestamptz) IS NOT NULL
   AND (last_activity_at IS NULL OR :at > last_activity_at)
```
- 이유: gap-fill/재청구로 과거 seq 재적용 시 활동시각이 과거로 회귀하지 않도록. 동기 순방향 경로 무영향.

### 6. EventStreamCodec + StreamConstants (FR-P2-4)
- 스트림 `events`, group `proj`, DLQ `events:dlq`.
- 엔트리 필드(outbox 본문과 1:1, self-contained): event_id, session_id, seq, type, payload(JsonUtil.toJson), idempotency_key, actor_id(null→빈문자열), occurred_at(epoch-milli).
- `toFields(StoredEvent)->Map<String,String>`, `toStoredEvent(Map)->StoredEvent`.
- 정상 경로(gap 없음)는 재조회 없이 복원 후 apply. gap만 findBySeqRange 보강.

### 7. ParticipantViewDao.applyPresence — 늦은 도착 처리 (critic ③)
- 확정안: **drop 허용 + 근거 문서화**(전용 컬럼 미채택).
- 근거: ① presence는 휘발성·최신 우선(설계서 §6/§0: Redis TTL 권위, 이벤트는 감사용). ② 정확한 과거 presence는 복원(replay) 경로가 Fold로 제공(BR-6). ③ 전용 seq 컬럼은 V2/DAO/복원 정합성 부담 → 과설계.
- 조치: applyPresence javadoc에 drop 근거 명시. 코드 변경 없음(last_applied_seq 가드 유지).

### 8. OutboxDao + OutboxRelay (FR-P2-3, BR-4, critic ④)
- OutboxDao:
  - `insert(StoredEvent e)`: 본문 포함 INSERT (`CAST(:payload AS jsonb)`).
  - `findUnpublished(int batch)`: `SELECT ... WHERE published=FALSE ORDER BY id ASC LIMIT :batch` (부분 인덱스 `ix_outbox_unpub`, id ASC=발행순서).
  - `markPublished(List<Long> ids)`: `UPDATE outbox SET published=TRUE WHERE id IN (:ids)`.
- OutboxRecord: record(id, eventId, sessionId, seq, eventType, payload(JsonNode), idempotencyKey, actorId, occurredAt).
- OutboxRelay `@Scheduled(fixedDelayString="${chat.outbox.relay.poll-interval-ms:500}")`:
  1. findUnpublished → 2. 각 레코드 EventStreamCodec.toFields → `XADD events MAXLEN ~ <maxlen> *` → 3. 성공 id markPublished.
- TX 경계: 배치 단일 TX로 안 감쌈. **XADD-then-markPublished**(유실<중복; mark 먼저면 XADD 실패 시 영구 유실). 중복은 seq-guard 흡수(at-least-once).
- BR-4: XADD 예외 시 markPublished 생략, WARN, 다음 폴링 재시도. append는 Redis 무관 정상. 복구 후 따라잡음(QE-2, AC-7).
- Q5: 다중 인스턴스 중복 XADD는 seq-guard 흡수 허용. SKIP LOCKED 미적용.

### 9. ProjectionWorker — 단일 소비 루프 (Q1, FR-P2-4~7, BR-2, BR-3)
- 메커니즘: **수동 XREADGROUP 단일 루프**(StreamMessageListenerContainer 미사용). 소비·재청구·DLQ 한 컴포넌트 순차 제어(2-track race 차단).
- 라이프사이클: `@Component implements SmartLifecycle`. start()에서 전용 데몬 스레드 1개, stop()에서 graceful shutdown(running 플래그 + join). @Scheduled 미사용.
- 루프 1회:
  1. 신규 소비: `XREADGROUP GROUP proj <consumer> COUNT <readCount> BLOCK <readBlockMs> STREAMS events >` → 각 엔트리 toStoredEvent → `ProjectionApplier.apply` → 성공 XACK / 실패 XACK 생략(pending).
  2. 재청구: `XAUTOCLAIM events proj <consumer> <claimMinIdleMs=30000> 0` → deliveryCount 확인:
     - `<=3`: apply 재처리 → 성공 XACK / 실패 pending(다음 사이클 deliveryCount++).
     - `>3`: `XADD events:dlq *` + `XACK`(본 스트림 제거) + ERROR 로그(상관키). DLQ 격리(BR-3, AC-5).
  3. 비면 BLOCK 타임아웃 자연 대기.
- XACK 타이밍: apply TX **커밋 성공 후** XACK(커밋 전 XACK 금지 — 유실). 재전달은 seq-guard 흡수(AC-6).
- 백오프(Q2): 메시지별 nextRetryAt 없음. min-idle(30s)을 재시도 간격으로, deliveryCount로 3회 판정. 트레이드오프: 1s/2s/4s 곡선 대신 ~30s 간격 근사(1회=30s,2회=60s,3회=90s→DLQ). 정체 방지 목적 충족.

### 10. ProjectionApplier — TX 경계: gap-fill + offset + 카운터 멱등 (critic ①, FR-P2-7, BR-2)
`@Transactional` 신규 경계:
1. `ProjectionOffsetDao.selectForUpdateOrInit(sessionId)` → L (행 없으면 INSERT 후 0). FOR UPDATE 세션 직렬화.
2. seq-guard 1차(BR-2): `incoming.seq <= L` → no-op, 정상 종료(호출자 XACK).
3. 적용 범위: 정상 `seq==L+1` → apply 1건. gap `seq>L+1` → `findBySeqRange(sid, L, incoming.seq)`로 `(L, incoming.seq]` 전체 seq순 apply.
4. 각 apply는 기존 `ProjectionUpdater.apply()` 경로(per-row seq-guard + 전이 boolean) 통과.
5. offset 갱신: `last_applied_seq=incoming.seq`, `events_since_snapshot += 적용건수`.
6. 스냅샷 트리거 → afterCommit 등록(§11).

**critic ① 카운터 멱등 권위 (명시):**
- 카운터 멱등 단일 권위 = **per-row 전이 boolean**(`applyJoined/applyLeft` FOR UPDATE 전이, `applySent` ON CONFLICT 반영행). `SessionViewDao.increment*`는 seq-guard 없고 전이 boolean이 true일 때만 호출되어 정확.
- `projection_offset.last_applied_seq`는 gap 감지 + 스냅샷 카운팅 **전용**, 카운터 가드 아님.
- 이중 평가 안전 이유: gap-fill로 과거 seq 재적용해도 per-row 가드가 흡수(applyJoined false→카운터 미증가, applySent ON CONFLICT→message_count 미증가). offset 범위판정과 per-row 전이판정이 독립 작동, 둘 다 통과해야 카운터 변동. offset이 넓은 범위를 줘도 per-row 가드가 최종 방어선 → 과증가 없음.
- 결론: offset="어디까지 봤는가"(진행·gap·snapshot), 전이 boolean="실제 상태 전이인가"(멱등 권위). 책임 분리.

시그니처: `ProjectionApplier.apply(StoredEvent)` @Transactional / `ProjectionOffsetDao.selectForUpdateOrInit(UUID)->long`, `advance(UUID,long,long)`, `resetSnapshotCounter(UUID)`.

CONSIDER: offset FOR UPDATE 세션 직렬화 → 단일 세션 burst 시 해당 세션 lag↑ 가능, 세션 간 병렬이라 전체 영향 작음(POC 수용).

### 11. Snapshot 자동화 — afterCommit 별도 TX (Q3, FR-P2-8, BR-5)
- events_since_snapshot 누적·리셋은 **apply TX 안**. `>= trigger-interval(200)`이면 리셋(=0)도 apply TX 안.
- createSnapshot 호출은 **afterCommit 별도 TX**(TransactionSynchronizationManager 패턴). createSnapshot 자체 @Transactional이라 별도 TX 실행.
- 실패 비중단(FR-P2-8): afterCommit 콜백 try-catch, 예외 시 WARN만.
- Q3 일관성: 리셋 후 스냅샷 실패 시 카운터=0인데 스냅샷 누락 → 다음 N건 재트리거(멱등 BR-5 무해). 스냅샷 일시 누락=복원 비용 상한 일시 위반일 뿐 정확성 무손상(BR-6).
- 이유: 리셋을 apply TX에 두면 스냅샷 실패가 projection 롤백 유발 안 함. "리셋 먼저, 스냅샷 best-effort"가 가장 단순·안전.

### 12. RealtimeChatApplication + 테스트 (AC-1~11)
- RealtimeChatApplication: `@EnableScheduling`(OutboxRelay), `@ConfigurationPropertiesScan`(StreamProps). ProjectionWorker는 SmartLifecycle.
- AbstractIntegrationTest: `RedisContainer`(`org.testcontainers:redis`) static 싱글톤 + `@ServiceConnection`(PostgreSQL 동일 패턴). flyway 테이블 검증에 `projection_offset` 추가.
- AwaitProjection: `await().atMost(5,SECONDS).untilAsserted(Runnable)`.
- 기존 39 테스트(AC-10): read model 단언만 Awaitility 래핑. `eventStore.*`/`restoreService.*`/`snapshotDao.*` 단언 수정 불요(BR-6).
  - Awaitility 필요: EventSourcing ac1/ac2/ac4/regression_participantCount*/regression_rejoin*/regression_duplicateKey*.
  - 수정 불요: ac3/ac5*/ac6/regression_snapshot*/regression_presence(예외)/regression_ended(maxSeq). RestApi timelineReturns200(복원). WebSocketStomp 전체(broadcast afterCommit, read model 미조회).
- 신규 AsyncPipelineIntegrationTest: AC-1(event/outbox+본문), AC-2(apply 스파이 미호출), AC-3(Relay), AC-4/6(중복→1회), AC-5(예외→pending→XAUTOCLAIM→DLQ), AC-7(Redis stop→append 정상→재개 따라잡음), AC-8(gap), AC-9(trigger=5 스냅샷), AC-11(sync-enabled=true 별도 컨텍스트).
- application-test.yml: redis @ServiceConnection 주입, poll-interval 짧게(100ms), retry/claim 테스트값.

## 구현 순서
1. [Must] 인프라 설정(build.gradle/application.yml) (의존: 없음)
2. [Must] V2__async_pipeline.sql (의존: 없음)
3. [Must] StreamConstants + StreamProps + EventStreamCodec (의존: 없음)
4. [Must] SessionViewDao.touchActivity 단조 가드 + ParticipantViewDao.applyPresence javadoc (의존: 없음)
5. [Must] OutboxRecord + OutboxDao (의존: 2,3)
6. [Must] ProjectionOffsetDao (의존: 2)
7. [Must] EventStore.append outbox 본문 INSERT (의존: 5)
8. [Must] CommandHandler 동기 projection 제거 + sync-enabled 분기 + 경고 (의존: 없음)
9. [Must] RedisConfig + RedisStreamInitializer (XGROUP CREATE MKSTREAM, BUSYGROUP 무시) (의존: 1,3)
10. [Must] OutboxRelay (@Scheduled XADD MAXLEN + markPublished) (의존: 5,7,9)
11. [Must] ProjectionApplier (TX: gap-fill+offset+카운터멱등+스냅샷 afterCommit) (의존: 6,3,4)
12. [Must] ProjectionWorker (SmartLifecycle 단일 루프) (의존: 9,11)
13. [Must] RealtimeChatApplication @EnableScheduling + @ConfigurationPropertiesScan (의존: 10,12)
14. [Must] AbstractIntegrationTest Redis + projection_offset 검증 + AwaitProjection (의존: 1,2)
15. [Must] 기존 39 테스트 Awaitility (EventSourcing/RestApi; WebSocketStomp 무수정) (AC-10) (의존: 8,12,14)
16. [Should] AsyncPipelineIntegrationTest AC-1~9 (의존: 10,12,14)
17. [Should] Snapshot 자동화 검증 AC-9 (의존: 11,16)
18. [Should] projection lag 메트릭(BR-7) occurred_at 기반 (의존: 12)
19. [Could] sync-enabled=true 테스트 컨텍스트 AC-11 (의존: 8,14)

병렬 그룹: {1,2,3,4,8} → {5,6,9} → {7,10,11} → {12} → {13,14,15,16}.

## 수용 기준 ↔ 설계 매핑
| AC | 설계 항목 |
|----|----------|
| AC-1 | §1 outbox 본문 INSERT(isNew=true) |
| AC-2 | §2 동기 projection 제거 |
| AC-3 | §8 OutboxRelay |
| AC-4 | §10 seq-guard + per-row 가드 |
| AC-5 | §9 재청구+DLQ |
| AC-6 | §9 XACK 타이밍 + seq-guard |
| AC-7 | §8 BR-4 |
| AC-8 | §10 gap-fill |
| AC-9 | §11 snapshot afterCommit |
| AC-10 | §12 Awaitility |
| AC-11 | §2 sync-enabled |

## 트레이드오프 요약
- outbox 본문 포함: 원자성 + Relay 재조회/race 제거. 멱등 재유입 미기록.
- XADD-then-markPublished: 유실<중복.
- 수동 XREADGROUP 단일 루프: race 차단·가시성. 스레드/종료 직접 관리.
- 백오프 min-idle 근사: 상태 최소화. 정확한 곡선 포기.
- projection_offset: gap·snapshot 전용. 카운터 권위는 per-row 전이 boolean.
- touchActivity 단조 가드: 과거 회귀 방지.
- presence drop 허용: 휘발성·최신 우선, 과거는 복원이 정확.
- gap 권위 = event store findBySeqRange.
- snapshot afterCommit + 리셋 apply TX: projection 비중단, 멱등 재트리거.
- events XADD MAXLEN: event store 권위라 적극 trim 안전.
- POC 단일 인스턴스 + consumer 호스트명 유일.
