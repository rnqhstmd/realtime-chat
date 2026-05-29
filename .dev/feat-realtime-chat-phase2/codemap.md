## 코드 맵: 실시간 대화 세션 — Phase 2 (비동기 파이프라인)

> Phase 2 범위(설계서 §5, §13): Outbox → Redis Stream → Projection Worker, 재시도/DLQ, snapshot 자동화.
> 현재 상태: 동기 projection(Phase 1). outbox 테이블은 존재하나 미사용. Redis 미연결.

### 핵심 파일 (Phase 2 변경 직접 대상)
- `command/CommandHandler.java:74` → 단일 수렴 진입점. 현재 한 TX에서 append→`projectionUpdater.apply()`(동기)→afterCommit broadcast. **Phase 2: outbox 기록 + 동기/비동기 전환 결정 지점**
- `event/EventStore.java:93` → `append()`가 `event` 테이블에만 INSERT. **outbox INSERT 미구현(같은 TX 추가 필요)**. seq 채번(UPDATE last_seq RETURNING), ON CONFLICT DO NOTHING, `findBySeqRange`(gap-fill/replay용 이미 존재), `maxSeq`.
- `projection/ProjectionUpdater.java:48` → 동기 projection 오케스트레이터. javadoc 명시: "gap-fill·스트림 재청구는 Phase 2 대상". **Phase 2: worker가 호출하는 비동기 경로로 전환 + seq-guard gap-fill**
- `src/main/resources/db/migration/V1__init_schema.sql` → outbox 테이블/부분인덱스(`ix_outbox_unpub WHERE published=FALSE`) 이미 정의. **Phase 2: 신규 마이그레이션 V2 (DLQ/attempt 컬럼 등 필요 시)**
- `build.gradle.kts` → **Redis 의존성 없음**. `spring-boot-starter-data-redis`(Lettuce) + testcontainers redis 추가 필요.
- `src/main/resources/application.yml` → **Redis 설정 없음**. spring.data.redis 추가 필요.

### 참조 파일
- `projection/{ParticipantViewDao,MessageViewDao,SessionViewDao}.java` → 뷰 DAO. seq-guard UPDATE, applyJoined/applySent 신규INSERT 여부 반환(카운터 중복증가 방지). last_applied_seq 가드.
- `restore/SnapshotService.java`, `restore/SnapshotDao.java` → 스냅샷 생성(현재 수동/REST 트리거, recent N=`chat.snapshot.recent-messages:200`), 멱등 save(PK session_id,up_to_seq). **Phase 2: N이벤트마다 자동 트리거**
- `restore/{Fold,SessionState,RestoreService,SnapshotState}.java` → 복원 로직(순수 fold + snapshot+replay).
- `event/{StoredEvent,AppendCommand,AppendResult,EventType}.java` + `event/payload/*.java` → 이벤트/payload DTO(record). 6종 타입.
- `realtime/{SessionBroadcaster,SimpSessionBroadcaster,EventBroadcast}.java` → 실시간 팬아웃(현재 로컬 STOMP만, Pub/Sub은 Phase 3).
- `common/json/JsonUtil.java` → 공유 ObjectMapper. 직렬화 단일 진입점.
- `docker-compose.yml:21` → redis:7 이미 프로비저닝(포트 6379, 무인증). 주석에 "Phase 2+에서 사용" 명시.
- `src/test/java/com/realtimechat/integration/*.java` → 통합테스트 3종(EventSourcing/RestApi/WebSocketStomp). **동기 projection 전제로 read-after-write 검증 → 비동기 전환 시 영향**. Testcontainers(현재 postgres만).
- `src/test/java/com/realtimechat/support/AbstractIntegrationTest.java` → 통합테스트 베이스(Testcontainers 컨테이너 정의).

### 설정
- `src/test/resources/application-test.yml` → 테스트 프로파일.
- `.claude/config.json` → java-spring 빌드 `./gradlew build`. 타임아웃 5분.

### Phase 2 구현 발견 사항 (B1)
- **StoredEvent 실제 시그니처**: `record StoredEvent(UUID eventId, UUID sessionId, long seq, EventType type, JsonNode payload, String idempotencyKey, UUID actorId, Instant occurredAt)`. 설계서의 `eventType`은 실제 `type`, payload는 `JsonNode`(String 아님). Codec/OutboxRecord/OutboxDao/EventStore는 이 시그니처 기준.
- **JsonUtil은 static util**(빈 아님): `JsonUtil.toJson(...)`, `JsonUtil.readTree(...)` 직접 호출. 생성자 주입 불요.
- **EventStreamCodec**: `@Component`, payload JsonNode↔JSON string, actorId null↔빈문자열, occurredAt Instant↔epoch-milli.
- **StreamProps**: `@ConfigurationProperties("chat.redis")` record + compact constructor 기본값. @ConfigurationPropertiesScan은 13단계에서 활성화.
- **[설계 deviation] Redis Testcontainer 의존성**: 설계서가 지정한 `org.testcontainers:redis`는 Spring Boot 3.3.5 testcontainers BOM에 미존재(버전 미관리, 해소 실패). → `com.redis:testcontainers-redis:2.2.2`로 교체. 14단계 AbstractIntegrationTest는 `com.redis.testcontainers.RedisContainer` 사용(GenericContainer 상속, image "redis" → Spring Boot `RedisContainerConnectionDetailsFactory`가 `@ServiceConnection` 자동 지원).

### Phase 2 버그 수정 (B6 테스트 검증)
- **RedisStreamInitializer BUSYGROUP 미처리 버그**: `e.getMessage().contains("BUSYGROUP")`가 최상위 RedisSystemException 메시지만 검사 → 실제 "BUSYGROUP"은 cause(Lettuce RedisBusyException)에 있어 매칭 실패 → 두 번째 컨텍스트부터 그룹 재생성 시 예외 재throw → ApplicationContext 로드 실패(34/54 테스트 실패). 증상: 첫 컨텍스트(AsyncPipelineIntegrationTest)는 그룹 신규 생성으로 통과, 이후 모든 distinct 컨텍스트(EventSourcing/RestApi/WebSocketStomp/SyncProjection)가 공유 Redis에서 BUSYGROUP. → `isBusyGroup(Throwable)` cause 체인 순회 판정으로 수정.

### 컨벤션
- 순수 Java(Lombok 미사용), 생성자 주입, record DTO, UUID/Instant, NamedParameterJdbcTemplate, JsonUtil 직렬화. 호출자 트랜잭션 가정(@Transactional은 CommandHandler 경계).

### Phase 2 비동기 전환 주의사항 (design-critic 발견 — 구현 시 필수 반영)
- **카운터 멱등 권위**: `SessionViewDao.incrementParticipants/incrementMessages`는 **seq-guard 없이 무조건 +1/-1**. 멱등성은 오직 `ParticipantViewDao.applyJoined`(FOR UPDATE 전이 boolean) / `MessageViewDao.applySent`(ON CONFLICT DO NOTHING 반영행) 판정에만 의존. → 비동기 재처리/gap-fill 시 카운터 권위를 per-row 전이 boolean으로 유지할지 projection_offset으로 옮길지 명확히 해야 함(이중 평가 금지).
- **touchActivity 회귀 위험**: `SessionViewDao.TOUCH_ACTIVITY_SQL`은 `last_activity_at = :at`을 **seq 비교 없이 덮어씀**. gap-fill/XAUTOCLAIM 재처리로 과거 seq 재적용 시 활동시각이 과거로 회귀 → seq/시각 단조 가드 추가 필요.
- **presence 가드 공유**: `applyPresence`는 `participant_view.last_applied_seq`를 JOINED/LEFT와 공유 → 늦게 도착한 과거 PRESENCE_CHANGED가 silently drop 가능.
- **outbox 본문 부재**: outbox에 payload/type 없음 → self-contained 엔트리 위해 Relay가 event 본문 추가 조회 또는 outbox 스키마에 본문 포함 결정 필요.
- **스트림 무한성장**: `events` 스트림 XTRIM/MAXLEN 정책 누락(event store가 권위라 적극 trim 안전).
- **consumer 이름 유일성**: 다중 인스턴스 시 같은 consumer 이름이면 PEL 공유로 XAUTOCLAIM/deliveryCount 깨짐 → 호스트명 기반 유일 부여 또는 단일 인스턴스 전제 명시.
