# 설계서: realtime-chat Phase 3 — 수평 확장 팬아웃 · presence TTL · resume-by-seq · 관측성

> 확정일: 2026-05-31 · 브랜치: feat/realtime-chat-phase3 · 모드: normal
> design-critic MUST-ADDRESS 3건 반영(2차). 사용자 확정 결정 A~G 반영.

## 1. 설계 규모

**대형** — 신규 파일 7~10개(presence 4분할 → 2개 통합), 수정 파일 9개, 4개 독립 영역(팬아웃 백플레인, presence liveness 추적, REST 조회 API, 관측성). 다중 인스턴스 동시성·스케줄러·외부 메트릭 인프라가 얽힘.

## 2. 아키텍처 개요

### (A) Redis Pub/Sub 팬아웃 백플레인 (FR-P3-1) — 기본 local, redis 명시 활성화
```
CommandHandler.afterCommit
  → SessionBroadcaster.broadcast(sessionId, StoredEvent)          [추상화 유지, 구현체 교체]

[chat.fanout.mode=local (기본, matchIfMissing)]
  → SimpSessionBroadcaster
    → messagingTemplate.convertAndSend("/topic/session.{id}", EventBroadcast)   [직접 로컬 전달]

[chat.fanout.mode=redis (다중 인스턴스 프로파일에서 명시)]
  → RedisPubSubSessionBroadcaster
    → publish 채널 "chat.fanout.{sessionId}", 값=EventBroadcast(JSON)  [BR-7: 실패 시 로그·무시]
  [모든 인스턴스 기동 시]
    RedisMessageListenerContainer.addMessageListener(SessionFanoutListener, PatternTopic("chat.fanout.*"))
      → onMessage: sid 파싱 + JSON→EventBroadcast
        → messagingTemplate.convertAndSend("/topic/session.{id}", EventBroadcast)  [BR-5 토픽 유지]
```
- **이중 전달 금지(불변식)**: local 모드는 직접 전달만, redis 모드는 publish만(자기 구독 echo가 로컬 전달을 대신함). 한 모드에서 두 경로를 동시에 수행하지 않는다.

### (B) presence liveness 추적 (FR-P3-2) — heartbeat=ping(이벤트 아님), Redis 키가 권위
```
[heartbeat = 경량 ping, 이벤트 아님]  STOMP SEND /app/heartbeat  또는  POST /sessions/{id}/heartbeat
  → PresenceController(또는 STOMP 핸들러)
    → PresenceTracker.touch(sid, pid)
        SETEX "presence:{sid}:{pid}" ttl=90s value=now
        SADD  "presence:tracked"  "{sid}:{pid}"            [sweep 대상 등록]

[최초 ONLINE 전이]  join 또는 첫 진입 시 PRESENCE_CHANGED(ONLINE) 1회 (상태 전이만 이벤트화)

[TTL 만료 감지]  @Scheduled(5s) PresenceSweeper.sweep
  for member in SMEMBERS "presence:tracked":
    if NOT EXISTS "presence:{member}":               // 키 만료(liveness 상실)
      if SREM "presence:tracked" member == 1:         // BR-3: 원자적 1회 보장
        CommandHandler.handle(sid, PRESENCE_CHANGED, {presence:OFFLINE}, idem, pid)  // 상태 전이 이벤트
```
- presence의 권위는 **Redis 키 존재 여부(liveness)**. read model `participant_view.presence`는 비동기 lag이 있어 "이미 OFFLINE인가" 판정 권위로 쓰지 않는다(critic #1 해소).
- OFFLINE 중복 방지는 **SREM 반환값(원자)**으로 보장 — read model 무관(critic #1, 결정 B).
- heartbeat는 SETEX touch만 하고 이벤트화하지 않아 event/outbox/stream/팬아웃·last_seq·resume delta 오염 없음(critic #2/#3 해소).

### (C) resume-by-seq (FR-P3-3) — raw 이벤트 delta
```
GET /sessions/{id}/events?afterSeq={seq}&limit={n}
  → ResumeEventController
    → 검증(공용 LimitSupport): afterSeq null→400, limit>500/<1→400, 세션 미존재→404
    → EventStore.findBySeqRange(id, afterSeq, afterSeq + limit + 1)   [limit+1로 hasMore]
    → ResumeResponse(events[seq,type,payload,occurredAt], hasMore)
```

### (D) 관측성 (FR-P3-4/5/6) — 메트릭 전용 컴포넌트 집약
```
ChatMetrics(=ProjectionLagMetrics 확장)  ──(MeterRegistry 주입)──>
  Gauge   chat.projection.lag.millis{state=last|max}
  Counter chat.projection.dlq.failures
  Counter chat.stream.processed       ← ProjectionWorker 호출
  Counter chat.outbox.relayed         ← OutboxRelay 호출
actuator: /actuator/prometheus, /health(Redis+DB 기본 indicator), /info
MDC: CommandHandler/ProjectionApplier/Broadcaster 경로 sessionId/seq/eventType
```

## 3. 변경 범위

### 신규 파일
| 파일 | 역할 | 주요 시그니처 |
|------|------|--------------|
| `realtime/RedisPubSubSessionBroadcaster.java` | SessionBroadcaster Redis 구현체(publish), `@ConditionalOnProperty(...havingValue="redis")` | `void broadcast(UUID, StoredEvent)` |
| `realtime/SessionFanoutListener.java` | Pub/Sub 구독 수신 → 로컬 STOMP 전달(redis 모드만 등록) | `void onMessage(Message, byte[])` |
| `realtime/FanoutProps.java` | `chat.fanout.*`(mode, channel-prefix) | record |
| `presence/PresenceProps.java` | `chat.presence.*`(ttl, poll, heartbeat 가이드) | record |
| `presence/PresenceTracker.java` | touch(SETEX+SADD), isAlive(EXISTS), trackedMembers(SMEMBERS), claimExpired(SREM 원자) | 아래 상세 |
| `presence/PresenceSweeper.java` | `@Scheduled(5s)` sweep + SREM 원자 판정 + OFFLINE 발행 | `void sweep()` |
| `presence/PresenceController.java` | heartbeat ping 수신(REST + STOMP) | `void heartbeat(...)` |
| `query/ResumeEventController.java` | resume-by-seq REST | `ResumeResponse events(UUID, Long, Integer)` |
| `query/ResumeResponse.java` | resume 응답 DTO(events + hasMore) | record |
| `common/web/LimitSupport.java` | 공용 limit 검증 상수/유틸 | `static int validate(int)`, `MAX_LIMIT` |

### 수정 파일
| 파일 | 변경 |
|------|------|
| `realtime/SimpSessionBroadcaster.java` | `@ConditionalOnProperty(name="chat.fanout.mode", havingValue="local", matchIfMissing=true)` 추가(기본 빈) |
| `config/RedisConfig.java`(또는 async/RedisConfig) | redis 모드 조건부 `RedisMessageListenerContainer` 빈 추가 |
| `projection/async/ProjectionLagMetrics.java` | `ChatMetrics`로 확장 — `MeterRegistry` 주입, Gauge/Counter 등록, stream/relay 카운터 메서드 추가 |
| `projection/async/ProjectionWorker.java` | XACK 성공 직후 `chatMetrics.recordStreamProcessed()` 호출 |
| `outbox/OutboxRelay.java` | markPublished 후 `chatMetrics.recordOutboxRelayed(n)` 호출 |
| `query/QueryController.java` | 내부 `MAX_LIMIT`/`validateLimit`를 `LimitSupport`로 위임(중복 제거) |
| `common/web/ApiExceptionHandler.java` | `MethodArgumentTypeMismatchException` → 400 핸들러 추가 |
| `application.yml` / `application-test.yml` | `chat.fanout.mode`, `chat.presence.*`, `management.*` 추가 |
| `build.gradle.kts` | actuator + micrometer-prometheus 의존성 |

**presence 컴포넌트 통합(critic SIMPLIFY, 결정 F)**: 1차 4분할(Tracker/ExpiryService/ExpiryScheduler/StompPresenceListener)을 **2개(PresenceTracker + PresenceSweeper) + heartbeat 수신 PresenceController**로 통합. ExpiryService의 OFFLINE 발행 로직을 Sweeper에 흡수. StompPresenceListener는 disconnect 보조경로(결정 D)라 1차 제외.

**CommandHandler 수정**: 없음. OFFLINE 발행은 기존 `handle` 호출, heartbeat는 이벤트가 아니므로 핸들러 무관. 쓰기 핫패스 무결합.

**DB**: V3 마이그레이션 **불요**(§6).

## 4. 적용 컨벤션
- **네이밍**: 클래스 PascalCase, DTO·프로퍼티는 record. 프로퍼티 클래스는 `*Props` record + compact constructor 기본값(`StreamProps` 패턴), `@ConfigurationProperties("chat.xxx")` + relaxed binding.
- **DI**: 생성자 주입 전용(Lombok 미사용). 스칼라 설정은 `@Value("${...:default}")`.
- **트랜잭션**: `@Transactional`은 CommandHandler/ProjectionApplier 경계에만. DAO·Tracker·Sweeper는 자체 어노테이션 없음(Sweeper의 OFFLINE 발행은 CommandHandler.handle 호출로 그 안의 TX 사용).
- **Redis**: `StringRedisTemplate` 위임(`RedisConfig`). 직렬화는 `JsonUtil` 정적 유틸 단일 진입점.
- **afterCommit 패턴**: `TransactionSynchronizationManager.isSynchronizationActive()` 분기 후 `registerSynchronization`/else 즉시 실행(CommandHandler.publishAfterCommit·ProjectionApplier.triggerSnapshotAfterCommit 동일).
- **예외 처리**: REST는 `InvalidEventException`(400)/`SessionNotFoundException`(404) → `ApiExceptionHandler`. 컨트롤러는 위임·검증만(`QueryController.validateLimit` → `LimitSupport`로 추출).
- **백그라운드 장애 격리**: relay/worker/sweeper는 예외를 WARN으로 흡수하고 루프·스케줄 지속.
- **STOMP 수집 패턴**: `@MessageMapping`(StompEventController) — heartbeat STOMP 핸들러 작성 시 참조.

## 5. 상세 설계

### 5-A. Redis Pub/Sub 팬아웃 (FR-P3-1, BR-1, BR-5, BR-7) — 기본 local
**`SessionBroadcaster` 추상화 유지**: 인터페이스(`broadcast(UUID, StoredEvent)`) 무변경. CommandHandler는 인터페이스만 의존하므로 구현체 교체로 모드 전환(개방-폐쇄).

**모드 스위치(결정 C)**:
- `SimpSessionBroadcaster`(기존, 수정): `@ConditionalOnProperty(name="chat.fanout.mode", havingValue="local", matchIfMissing=true)` → **기본 빈**.
- `RedisPubSubSessionBroadcaster`(신규): `@ConditionalOnProperty(name="chat.fanout.mode", havingValue="redis")` → 다중 인스턴스 프로파일에서만 등록. 한 시점에 한 구현체만 빈 등록 → 주입 모호성 없음.

**`RedisPubSubSessionBroadcaster.broadcast`**:
```
String channel = fanoutProps.channelPrefix() + sessionId;     // "chat.fanout." + sid
String json = JsonUtil.toJson(EventBroadcast.from(event));
try { stringRedisTemplate.convertAndSend(channel, json); }
catch (RuntimeException e) { log.warn("fanout publish 실패(무시): sid={}, seq={}", ...); }   // BR-7
```
- 직렬화: `EventBroadcast` + `JsonUtil.toJson`(Stream용 `EventStreamCodec` 미재사용). 구독측이 동일 `EventBroadcast`로 복원해 STOMP 전달 → Pub/Sub·STOMP 페이로드 형태 일치.

**`SessionFanoutListener`(MessageListener, redis 모드만)**: 채널명에서 sid 파싱, `JsonUtil.mapper().readValue(body, EventBroadcast.class)` 후 `messagingTemplate.convertAndSend("/topic/session." + sid, broadcast)`. 역직렬화/전달 예외는 try-catch WARN.

**`RedisConfig` 수정(redis 모드 조건부)**:
```
@Bean
@ConditionalOnProperty(name="chat.fanout.mode", havingValue="redis")
RedisMessageListenerContainer fanoutContainer(
    RedisConnectionFactory cf, SessionFanoutListener listener, FanoutProps props) {
  var c = new RedisMessageListenerContainer();
  c.setConnectionFactory(cf);
  c.addMessageListener(listener, new PatternTopic(props.channelPrefix() + "*"));
  return c;
}
```
- `PatternTopic("chat.fanout.*")` 단일 구독. BR-1 격리는 채널명 sid로 자연 보장. `StringRedisTemplate` 기존 공유.
- 자기 echo는 의도된 동작(publish 노드도 자기 구독 경로로만 로컬 전달 → 노드 대칭, 중복 없음).

### 5-B. presence liveness (FR-P3-2, BR-2, BR-3) — heartbeat=ping(이벤트 아님), Redis 키 권위
**PRD deviation 명시**: PRD FR-P3-2 "heartbeat = PRESENCE_CHANGED(ONLINE) 전송"을 **"heartbeat = 경량 ping(이벤트 아님)"으로 재정의**한다. 근거: heartbeat 이벤트화 시 30s마다 참여자 수만큼 event/outbox/stream/팬아웃이 돌고 last_seq 폭증·resume delta 오염(critic #2/#3). presence 본질은 liveness 추적이므로 heartbeat는 Redis TTL 갱신 수단(사용자 승인, 결정 A). `PRESENCE_CHANGED`는 **상태 전이 시에만** 발행: (i) 최초 ONLINE(join/첫 진입), (ii) 만료·disconnect 시 OFFLINE.

**키 스키마**:
- liveness 키: `presence:{sessionId}:{participantId}`, TTL = `chat.presence.ttl-seconds`(기본 90, 테스트 3), 값=epoch-milli(디버그용).
- sweep 대상 집합: Redis Set `presence:tracked`, 멤버 `{sessionId}:{participantId}`(TTL 없는 일반 Set, OFFLINE 처리 시 SREM 명시 제거).

**`PresenceTracker`(신규)**:
```
void touch(UUID sid, UUID pid)
   key = "presence:" + sid + ":" + pid
   redis.opsForValue().set(key, Long.toString(now), Duration.ofSeconds(ttl));   // SETEX
   redis.opsForSet().add(TRACKED_SET, member(sid,pid));                          // SADD (멱등)
boolean isAlive(UUID sid, UUID pid)            // EXISTS presence 키
Set<String> trackedMembers()                   // SMEMBERS presence:tracked
boolean claimExpired(String member)            // SREM presence:tracked member == 1 (원자)
```
- `claimExpired`가 BR-3 핵심: SREM 반환값 1이면 "이 호출이 멤버 제거에 성공"한 유일 주체 → 그 주체만 OFFLINE 발행. 동시 sweep/disconnect 경쟁도 SREM 원자성으로 1회 수렴(read model lag 무관, critic #1 해소).

**heartbeat 수신 — `PresenceController`(신규)**:
- REST: `POST /sessions/{id}/heartbeat`(body/param `participantId`) → `presenceTracker.touch(id, participantId)`, 204. 세션 존재 검증(`SessionDao.status` → 미존재 404, ENDED 무시/400).
- STOMP: `@MessageMapping("/heartbeat")` 또는 `/session/{id}/heartbeat`로 ping 수신 → 동일 touch. CommandHandler 미경유.
- heartbeat ping은 이벤트가 아니므로 멱등키·append·broadcast 없음.

**최초 ONLINE 전이**: join 시 기존 PARTICIPANT_JOINED 이벤트가 read model presence를 'ONLINE'으로 INSERT(`ParticipantViewDao.INSERT_JOINED_SQL`)하므로 join이 ONLINE 진입을 겸한다(상태 전이 최소화). 명시적 재-ONLINE(OFFLINE→ONLINE 복귀)은 클라이언트가 PRESENCE_CHANGED(ONLINE) 1회 전송(상태 전이라 이벤트화 정당) → 기존 CommandHandler `requirePresenceChanged` 처리.

**만료 감지 — `PresenceSweeper`(신규, 5s 폴링)**:
```
@Scheduled(fixedDelayString = "${chat.presence.expiry-poll-ms:5000}")
void sweep() {
  for (String member : presenceTracker.trackedMembers()) {
    var (sid, pid) = parse(member);
    if (!presenceTracker.isAlive(sid, pid)) {          // liveness 키 만료
      if (presenceTracker.claimExpired(member)) {      // SREM==1: 자기만 발행
        publishOffline(sid, pid);
      }
    }
  }
}
private void publishOffline(UUID sid, UUID pid) {
  try {
    ObjectNode payload = JsonUtil.mapper().createObjectNode()
        .put("participantId", pid.toString()).put("presence", "OFFLINE");
    String idem = "presence-offline-" + sid + "-" + pid + "-" + Instant.now().toEpochMilli();
    commandHandler.handle(sid, EventType.PRESENCE_CHANGED, payload, idem, pid);  // 정상 수집 경로
  } catch (InvalidEventException e) {                  // ENDED 세션 등
    log.warn("OFFLINE 발행 skip(세션 비활성): sid={}, pid={}", sid, pid, e);
  } catch (RuntimeException e) {                       // sweep 중단 방지
    log.warn("OFFLINE 발행 실패: sid={}, pid={}", sid, pid, e);
  }
}
```
- keyspace notification 미사용(PRD 5s 폴링 명시) — 단순·결정적, testcontainers 추가 설정 불요.
- 멱등키: SREM이 1회 보장하므로 단조요소(epoch-milli)로 충분. 재접속 후 재만료는 새 멤버 SADD → 새 SREM 사이클로 정상.
- OFFLINE 발행은 기존 `CommandHandler.handle` 호출 → projection·(local/redis)팬아웃 일반 이벤트와 동일 처리. CommandHandler 수정 0.

**disconnect 즉시 OFFLINE(결정 D)**: 1차 구현 제외. `SessionDisconnectEvent`는 STOMP 세션 ID만 제공, (chatSessionId, participantId) 매핑 규약 없음. AC-3/4/5는 5s 폴링 sweep만으로 충족. 보조 즉시 트리거는 STOMP connect 헤더 규약 확정 후 `StompPresenceListener`로 추가(후순위).

**BR-3 가드 권위 정정**: 1차의 `ParticipantViewDao.findPresence` 기반 가드 **폐기**(read model 비동기 lag, critic #1). 가드는 전적으로 SREM 원자 판정. → `findPresence` 신규 메서드 불요(ParticipantViewDao 수정 없음).

### 5-C. resume-by-seq (FR-P3-3, BR-4) — raw 이벤트 delta
**timeline과의 경계(결정 E)**: `GET /sessions/{id}/timeline`은 snapshot+replay로 **fold된 상태**를 반환. resume `GET /sessions/{id}/events?afterSeq=`는 **raw 이벤트 delta**(afterSeq 이후 실제 이벤트 시퀀스)를 반환. 클라이언트 규약: 재연결 시 마지막 수신 seq를 afterSeq로 보내 누락 구간만 delta 수신 후 STOMP 재구독. heartbeat 비이벤트화로 delta 오염 없음.

**`ResumeEventController`(신규, QueryController 패턴)**:
```
@GetMapping("/sessions/{id}/events")
public ResumeResponse events(
    @PathVariable UUID id,
    @RequestParam(name="afterSeq", required=false) Long afterSeq,
    @RequestParam(name="limit", required=false, defaultValue="100") int limit)
```
- 검증: ① afterSeq==null → `InvalidEventException` → 400(AC-9, required=false로 받아 직접 검증). ② `LimitSupport.validate(limit)` 1~500 초과/미달 → 400(AC-10). ③ `sessionDao.status(id).isEmpty()` → `SessionNotFoundException` → 404(AC-11).
- 조회/hasMore: `findBySeqRange(id, afterSeq, afterSeq+limit+1)`(반개구간 `(from, to]`, 최대 limit+1건). size==limit+1 → `hasMore=true`, 앞 limit개 반환. ≤limit → false(AC-6/7/8).
- afterSeq≥max → 빈 결과 → `events:[]`,`hasMore:false`(AC-7).
- limit 비정수 → `MethodArgumentTypeMismatchException` → ApiExceptionHandler 신규 핸들러 400(결정 F).

**`ResumeResponse`(신규 record)**:
```
record ResumeResponse(List<ResumeEvent> events, boolean hasMore) {}
record ResumeEvent(long seq, EventType type, JsonNode payload, Instant occurredAt) {
  static ResumeEvent from(StoredEvent e) { ... }   // payload는 JsonNode 그대로
}
```

**`common/web/LimitSupport`(신규)**:
```
public final class LimitSupport {
  public static final int MAX_LIMIT = 500;
  public static int validate(int limit) {           // QueryController.validateLimit 이관
    if (limit < 1 || limit > MAX_LIMIT) throw new InvalidEventException(...);
    return limit;
  }
}
```
- `QueryController`는 내부 `MAX_LIMIT`/`validateLimit`를 `LimitSupport`로 위임(중복 제거).

### 5-D. 관측성 (FR-P3-4/5/6, BR-6) — 메트릭 전용 컴포넌트 집약
**`ProjectionLagMetrics` → `ChatMetrics` 확장(결정 F)**: 메트릭을 한 컴포넌트에 집약. 기존 클래스명/패키지 유지하며 내부 확장(호출부 영향 최소) 또는 `ChatMetrics`로 개명.
- 생성자에 `MeterRegistry` 주입. 기존 `AtomicLong`(lastLag/maxLag/dlqFailures)은 Gauge 소스로 유지(기존 getter·호출부 무변경):
  ```
  Gauge.builder("chat.projection.lag.millis", lastLagMillis, AtomicLong::get).tag("state","last").register(reg);
  Gauge.builder("chat.projection.lag.millis", maxLagMillis,  AtomicLong::get).tag("state","max").register(reg);
  Counter dlqFailures      = Counter.builder("chat.projection.dlq.failures").register(reg);
  Counter streamProcessed  = Counter.builder("chat.stream.processed").register(reg);
  Counter outboxRelayed    = Counter.builder("chat.outbox.relayed").register(reg);
  ```
- 메서드: `record(occurredAt, appliedAt)`(기존), `recordDlqWriteFailure()`(기존 + `dlqFailures.increment()`), 신규 `recordStreamProcessed()`, `recordOutboxRelayed(long n)`.
- BR-6: 인메모리 기반이라 재시작 시 0 시작 자동 충족.

**카운터 호출 지점**: `ProjectionWorker.applyAndAck` XACK 성공 직후 `recordStreamProcessed()`. `OutboxRelay.relay` `markPublished` 후 `recordOutboxRelayed(successIds.size())`(OutboxRelay 생성자 1개 추가).

**Actuator(FR-P3-5)**: `management.endpoints.web.exposure.include: health,info,prometheus`, `management.endpoint.health.show-details: always`. Redis·DataSource health indicator는 starter-actuator + data-redis/jdbc 자동구성으로 기본 제공(AC-13). `/info`는 `management.info.*`.

**MDC(FR-P3-6, [Could])**: `CommandHandler.handle` 진입 시 `MDC.put("sessionId",...)`, `ProjectionApplier.apply`/Broadcaster에서 seq/eventType, finally `MDC.clear()`. logback 패턴 `%X{sessionId}` 추가. Must/Should 완료 후 최종.

## 6. DB 스키마 영향
**V3 마이그레이션 불요(근거 강화).** presence liveness 권위는 Redis 키. read model `participant_view.presence`(V1)는 OFFLINE 전이 이벤트가 정상 수집될 때 기존 `applyPresence`로만 갱신, BR-3 가드는 SREM 원자 판정이라 read model 조회조차 불요(1차 `findPresence` 폐기 → ParticipantViewDao 수정 없음). heartbeat는 이벤트·DB 미기록(Redis 키만). resume는 기존 `event` + `findBySeqRange`.

## 7. 설정 변경
**`application.yml` 추가**:
```yaml
chat:
  fanout:
    mode: local                 # 기본 local(단일 인스턴스). 다중 인스턴스 프로파일에서 redis 오버라이드.
    channel-prefix: "chat.fanout."
  presence:
    ttl-seconds: 90
    heartbeat-interval-seconds: 30   # 클라이언트 ping 주기 가이드(서버는 touch만)
    expiry-poll-ms: 5000
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
  endpoint:
    health:
      show-details: always
```
- 다중 인스턴스 운영 프로파일(예: `application-cluster.yml`)에서 `chat.fanout.mode: redis`만 오버라이드.

**`application-test.yml` 추가**: `chat.presence.ttl-seconds: 3`, `expiry-poll-ms: 500`(QE-2 단축). `chat.fanout.mode`는 기본 local 유지(기존 39개 테스트 회귀·지연 회피). 팬아웃 redis 검증은 전용 테스트에서 `@TestPropertySource(properties="chat.fanout.mode=redis")`.

**`build.gradle.kts` 추가**:
```kotlin
implementation("org.springframework.boot:spring-boot-starter-actuator")
implementation("io.micrometer:micrometer-registry-prometheus")
```

## 8. 테스트 전략
| AC | 테스트 | 방법 |
|----|--------|------|
| AC-1, QE-4 | 팬아웃(redis 모드) | `@TestPropertySource(chat.fanout.mode=redis)` 전용 클래스. publish→`RedisMessageListenerContainer`/`SessionFanoutListener` 수신 + 채널 격리(AC-2). **AC-1 한계(G)**: 단일 JVM이라 진짜 2-인스턴스는 불가 → 백플레인 동작 확인 + 수동 2-인스턴스 데모 문서화(README/설계서). |
| AC-2 | 채널 격리 | redis 모드에서 sidA 채널 publish가 sidB 구독자 미전달. |
| AC-3 | presence 키 TTL | test 프로파일 TTL=3s. heartbeat ping(touch) 후 `redis.getExpire(key)` 양수, `Awaitility.atMost(10s)` 키 소멸. |
| AC-4 | OFFLINE 자동 수집 | TTL 만료 후 Awaitility로 EventStore PRESENCE_CHANGED(OFFLINE) 1건 + read model OFFLINE(최대 10s). sweep 500ms. |
| AC-5 | 중복 방지(SREM 원자) | 만료 멤버 sweep 후 OFFLINE 정확히 1건(seq 1회 증가). 동시성: 같은 멤버 claimExpired 2회 호출 시 1회만 true(Tracker 단위 테스트). |
| AC-6~11 | resume API | MockMvc/TestRestTemplate 경계: 정상 오름차순, afterSeq=max→빈+hasMore false, hasMore true, limit 501→400, 비정수→400, afterSeq 누락→400, 없는 세션→404. |
| AC-12 | prometheus | `/actuator/prometheus` 본문에 `chat_projection_lag_millis`, `chat_projection_dlq_failures_total`, `chat_stream_processed_total`, `chat_outbox_relayed_total` 포함. |
| AC-13 | health | `/actuator/health` 200 + `components.redis`/`components.db` UP. |
| AC-14 | publish 실패 격리 | redis 모드 broadcaster publish 예외 유발해도 EventStore append 성공·예외 미전파. |
| AC-15, QE-1 | 회귀 | 기존 통합 테스트 전체. local 모드 기본 유지로 broadcast 무변경 → 회귀 최소. |

테스트 베이스: 기존 `AbstractIntegrationTest`(postgres+redis testcontainers).

## 9. 구현 순서
1. **[Must] 의존성·설정·공용 유틸 골격** — `build.gradle.kts`(actuator/prometheus), `application.yml`/`-test.yml`(`chat.fanout.mode=local`/`chat.presence.*`/`management.*`), `LimitSupport`, `ApiExceptionHandler`에 `MethodArgumentTypeMismatchException` 핸들러. (선행, 단독)
2. **[Must] 팬아웃 골격** — `FanoutProps`, `SimpSessionBroadcaster`에 `@ConditionalOnProperty(local, matchIfMissing=true)`, `RedisPubSubSessionBroadcaster`(redis), `SessionFanoutListener`, `RedisConfig` 조건부 컨테이너. (의존: 1)
3. **[Must] resume-by-seq** — `ResumeResponse`, `ResumeEventController`(LimitSupport), `QueryController`를 LimitSupport 위임. (의존: 1, **2와 병렬 가능**)
4. **[Must] presence 인프라** — `PresenceProps`, `PresenceTracker`(touch/isAlive/trackedMembers/claimExpired). (의존: 1, **2·3과 병렬 가능**)
5. **[Must] presence heartbeat 수신 + sweep** — `PresenceController`(REST+STOMP ping→touch), `PresenceSweeper`(@Scheduled 5s, SREM 원자, OFFLINE via CommandHandler.handle). (의존: 4)
6. **[Should] 관측성 메트릭** — `ProjectionLagMetrics`→`ChatMetrics`(MeterRegistry, Gauge/Counter). (의존: 1)
7. **[Should] 카운터 호출부** — `ProjectionWorker`(stream processed), `OutboxRelay`(relayed). (의존: 6)
8. **[Should] actuator 검증** — health/info/prometheus 노출 테스트. (의존: 1, 6, 7)
9. **[Could] MDC 구조화 로그** — CommandHandler/ProjectionApplier/Broadcaster MDC + logback. 최종. (의존: 2, 5)
10. **[Must] 통합 테스트** — AC별 작성, 회귀(AC-15). (각 영역 완료 후)
11. **[후순위/범위외] disconnect 즉시 OFFLINE** — `StompPresenceListener` + STOMP connect 헤더 규약. 결정 D로 1차 제외. (의존: 5)

병렬 그룹: {2}/{3}/{4→5}/{6→7} 영역 독립. 동일 파일 충돌 없음.

## 10. 리스크 / 트레이드오프
- **[해소] presence 이벤트 폭증**: heartbeat 비이벤트화(A)로 폭증·last_seq 폭증·resume delta 오염 제거(critic #2/#3).
- **[해소] BR-3 가드 권위**: read model lag 의존 폐기, SREM 원자로 OFFLINE 1회(critic #1).
- **폴링 비용**: 5s마다 SMEMBERS 전수 + 멤버별 EXISTS. 다수 시 왕복 증가. 완화: 배치 EXISTS(파이프라인)/세션별 Set 분할. POC 무시 가능.
- **tracked Set 좀비 멤버**: OFFLINE 처리 시 SREM 누락 시 잔존 → 다음 sweep에서 키 부재 시 재시도하나 claimExpired 1회 보장으로 실질 무해(잔여: 재접속 직전 1회 추가 OFFLINE, read model 무변경).
- **AC-1 검증 강도 한계(G)**: 단일 JVM으로 진짜 2-인스턴스 증명 불가 → 백플레인 동작 + 수동 데모. FR-13은 자동화 한계 + 수동 데모 조합.
- **PRD deviation(heartbeat 규약)**: FR-P3-2 문구와 구현 상이. 클라이언트 문서에 ping 엔드포인트(`/app/heartbeat` 또는 `POST /sessions/{id}/heartbeat`)·주기(30s) 명시 필요.
- **redis 모드 자기 echo 이중 전달**: broadcast가 publish+로컬 전달 동시 수행 시 중복. 불변식(§2-A·§5-A) 준수 — 코드 리뷰 체크포인트.
