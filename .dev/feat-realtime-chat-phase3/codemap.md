## 코드 맵: 실시간 대화 세션 — Phase 3 (팬아웃·presence TTL·resume 고도화·관측성)

> Phase 3 후보 범위(Phase 2 PRD 제외범위 기준): **Redis Pub/Sub 팬아웃 · presence TTL 자동 갱신 · resume-by-seq 고도화 · 관측성 대시보드**.
> 정확한 범위는 phase-requirements(product-owner PRD)에서 확정한다. 아래는 setup 단계의 가벼운 초기 탐색 결과.
> 현재 상태: 팬아웃은 단일 인스턴스 로컬 STOMP만(SimpMessagingTemplate). presence는 이벤트 기반만(TTL 없음). resume는 timeline 조회 위주. 관측성은 Phase 2의 lag 메트릭만.

### 핵심 파일 (Phase 3 변경 후보 직접 대상)
- `realtime/SimpSessionBroadcaster.java` → `SessionBroadcaster` 구현체. 현재 SimpMessagingTemplate으로 **로컬 인스턴스 STOMP 구독자에게만** 팬아웃. **Phase 3: Redis Pub/Sub 구독→로컬 재발행으로 다중 인스턴스 팬아웃 확장 후보.**
- `realtime/SessionBroadcaster.java` → 팬아웃 추상화 인터페이스. **Phase 3: Pub/Sub 발행/구독 데코레이터 또는 신규 구현체 추가 지점.**
- `event/payload/PresenceChangedPayload.java` + `projection/ParticipantViewDao.java` → presence 상태 모델/적용. 현재 이벤트 기반 online/offline만, **TTL/heartbeat 없음**. **Phase 3: presence TTL 자동 갱신(Redis key TTL + 만료 시 offline 전이) 후보.**
- `projection/async/ProjectionLagMetrics.java` → Phase 2에서 추가된 projection lag 메트릭(BR-7 기반). **Phase 3: 관측성 고도화(Micrometer/Prometheus 노출, 처리량/지연/리플레이 비용 대시보드)의 출발점.**
- `command/CommandHandler.java:74` → 단일 수렴 진입점. afterCommit broadcast 호출. **Phase 3: 로컬 broadcast → Pub/Sub publish 전환/이중화 고려 지점.**

### 참조 파일
- `realtime/{EventBroadcast,StompEventController,StompError}.java` → STOMP 컨트롤러/브로드캐스트 DTO/에러 표준. 팬아웃 페이로드 형식 참조.
- `config/WebSocketConfig.java` → STOMP 엔드포인트/심플 브로커 설정. Pub/Sub 연동 또는 외부 브로커 릴레이 검토 지점.
- `query/QueryController.java` + `query/TimelineResponse.java` → 조회/타임라인 응답. **resume-by-seq API 고도화(마지막 seq 이후 이벤트 재생) 후보.**
- `restore/RestoreService.java` + `event/EventStore.java`(`findBySeqRange`) → seq 이후 replay 소스. resume 고도화 시 재사용.
- `projection/ProjectionUpdater.java` → projection 오케스트레이터. PRESENCE_CHANGED→`applyPresence` 위임. **[critic #1: BR-3 가드 판정 위치 검증 — presence가 last_applied_seq를 모든 이벤트와 공유하므로 read model은 lag 권위 부적합 → Redis 원자적 마커로 결정]**
- `async/RedisConfig.java` + `async/StreamProps.java` → Redis 연결/`chat.redis` 설정. **Pub/Sub 채널/presence TTL 프로퍼티 추가 지점.**
- `outbox/OutboxRelay.java` → Stream 발행 경로. 팬아웃(Pub/Sub)과 projection 전달(Stream)의 책임 분리 유지 참조. **[FR-P3-4: relay 처리량 카운터 주입 지점]**
- `projection/async/ProjectionWorker.java` → Redis Stream XREADGROUP 처리 루프 + DLQ 적재. **[FR-P3-4: stream 처리량/DLQ 카운터 주입 지점]**
- `src/test/java/com/realtimechat/support/AbstractIntegrationTest.java` → 통합테스트 베이스(postgres+redis Testcontainers). Phase 3 신규 통합테스트(팬아웃/presence TTL/resume) 확장 지점.

### 설정
- `src/main/resources/application.yml` → Redis/`chat.*` 프로퍼티. **Phase 3 신규(presence TTL, pub/sub channel, 관측성) 프로퍼티 추가.**
- `build.gradle.kts` → **관측성 의존성(micrometer-registry-prometheus, spring-boot-starter-actuator) 추가 후보.**
- `src/main/resources/db/migration/` → V1(init), V2(async pipeline). Phase 3에서 스키마 변경 필요 시 V3 추가.

### Phase 3 확정 설계 — 변경 대상 (design.md 기준)
**신규(10)**: `realtime/RedisPubSubSessionBroadcaster`, `realtime/SessionFanoutListener`, `realtime/FanoutProps`, `presence/PresenceProps`, `presence/PresenceTracker`(touch/isAlive/trackedMembers/claimExpired-SREM원자), `presence/PresenceSweeper`(@Scheduled 5s sweep+OFFLINE발행), `presence/PresenceController`(heartbeat ping REST+STOMP), `query/ResumeEventController`, `query/ResumeResponse`, `common/web/LimitSupport`(MAX_LIMIT=500 공용).
**수정(9)**: `realtime/SimpSessionBroadcaster`(@ConditionalOnProperty local matchIfMissing), `config/WebSocketConfig`(필요시), `async/RedisConfig`(redis 조건부 MessageListenerContainer), `projection/async/ProjectionLagMetrics`→ChatMetrics 확장(MeterRegistry+Counter), `projection/async/ProjectionWorker`(recordStreamProcessed), `outbox/OutboxRelay`(recordOutboxRelayed), `query/QueryController`(LimitSupport 위임), `common/web/ApiExceptionHandler`(MethodArgumentTypeMismatchException→400), `application.yml`/`application-test.yml`, `build.gradle.kts`(actuator+prometheus).
**후순위/범위외**: `presence/StompPresenceListener`(disconnect 즉시 OFFLINE, STOMP 헤더 규약 확정 후).
**DB**: V3 마이그레이션 불요(presence 권위=Redis 키, read model 가드 폐기).
**핵심 deviation**: heartbeat는 이벤트 아님(경량 ping→SETEX touch). PRESENCE_CHANGED는 상태전이(ONLINE 진입/OFFLINE)에만.

### 컨벤션 (Phase 1/2 계승)
- 순수 Java(Lombok 미사용), 생성자 주입, record DTO, UUID/Instant, NamedParameterJdbcTemplate, JsonUtil 단일 직렬화. `@Transactional`은 CommandHandler 경계. Redis 접근은 Lettuce(spring-data-redis).
