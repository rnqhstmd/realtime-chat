# 장애 주입 테스트 설계서 (Fault Injection Tests)

> 작성일: 2026-06-01 · 브랜치: `test/fault-injection-tests` · 작성자: realtime-chat 팀
> 대상 요구사항: 과제 §4.4-(3) 장애 대응 시나리오(운영 관점), §5 가산점 BONUS-6(테스트 전략 고도화)
> 근거 설계: [통합 설계서 §9 장애 대응](./2026-05-28-realtime-chat-design.md) · §12 테스트 전략

---

## 0. 목적과 범위

### 0.1 목적

통합 설계서 §9에 **문서로만** 기술된 3종 장애 대응 시나리오(서버 다운 / DB 장애 / 데이터 정합성)를
**실제 장애를 주입하는 통합 테스트로 실증**한다. 이로써 다음을 달성한다.

1. **과제 §4.4-(3) 충족 보강**: "감지 → 완화 → 복구" 흐름이 말이 아니라 코드로 증명된다.
2. **가산점 BONUS-6 충족**: 장애 주입 테스트(fault injection) + 재현 가능한 시나리오 스크립트.
3. **회귀 안전망 확보**: 이미 구현된 복구 메커니즘(Outbox 재발행, XPENDING/XCLAIM 재청구,
   seq-guard 멱등)이 향후 변경에도 깨지지 않음을 CI에서 지속 검증한다.

### 0.2 핵심 통찰 — "복구 로직은 이미 있다. 테스트는 그것을 증명한다."

Phase 2~3에서 이벤트 소싱 + Transactional Outbox 패턴을 구현하며 **장애 복구 메커니즘은
이미 코드에 내장**되었다. 본 작업은 새 복구 로직을 만드는 것이 **아니라**, 그 메커니즘이
실제 인프라 단절 상황에서 의도대로 동작함을 검증하는 것이다. 따라서 산출물의 대부분은
**프로덕션 코드가 아닌 테스트 코드**이며, 프로덕션 변경은 테스트 의존성·타임아웃 설정에 그친다.

만약 테스트 작성 중 복구가 실패한다면(테스트 red), 그것은 본 작업이 발견한 **실제 결함**이며
별도로 수정한다. 이 경우 테스트의 가치가 가장 크게 입증된다.

### 0.3 비범위

- 부하 테스트·성능 측정(BONUS-3) — 별도 작업(k6/Gatling).
- 다중 JVM 실제 분산 환경에서의 인스턴스 페일오버(단일 JVM 내 컴포넌트 재기동으로 근사).
- Redis Stream → Kafka 전환, WebRTC 등.
- 락 경합(lock contention) 정밀 재현 — 설계서 §9.2 문서 분석으로 갈음(아래 §5.2 참조).

---

## 1. 요구사항 매핑

| 출처 | 요구 내용 | 본 설계 대응 | 테스트 |
|------|-----------|--------------|--------|
| §4.4-(3) ① | 서버 다운(인스턴스 장애): 감지→완화→복구 | Projection Worker 재기동 후 미ACK pending 재처리 | `WorkerOutageFaultTest` |
| §4.4-(3) ② | DB 장애/성능 저하(커넥션 고갈, 락 경합) | DB 연결 차단 시 append 실패·복구 후 정상화, 커넥션 고갈 타임아웃 | `DbOutageFaultTest` |
| §4.4-(3) ③ | 데이터 유실/정합성(중복 저장, 부분 실패) | Redis 차단 중 outbox 적체→복구 후 재발행, 유실 0 | `RedisOutageFaultTest` |
| §5 BONUS-6 | 테스트 전략 고도화(통합/장애주입/재현 스크립트) | Toxiproxy 기반 재현 가능한 장애 주입 하네스 | 상기 3종 + 공통 베이스 |
| 설계서 §9.1~9.3 | 장애 대응 문서 | 문서의 "복구" 항목을 테스트로 실증 | 상동 |
| 설계서 §12 | "장애 주입: 중복/순서/projection 재시작/Redis 끊김 후 outbox 재발행" | 본 설계가 §12의 장애 주입 행을 구현 | 상동 |

---

## 2. 현재 상태 분석 — 검증 대상 복구 메커니즘

테스트가 증명할 기존 구현을 명시한다(코드 위치 포함). **이미 존재하므로 새로 만들지 않는다.**

### 2.1 Outbox 재발행 (Redis 단절 복구) — `outbox/OutboxRelay.java`

- `@Scheduled(fixedDelay = chat.outbox.relay.poll-interval-ms, 기본 500ms)`로 미발행 레코드를 폴링.
- XADD 성공분만 `markPublished` 처리(XADD-then-mark, 유실 < 중복). **XADD 실패 시 WARN 로그 후
  다음 폴링에서 재시도**(BR-4). → Redis 복구 시 자동 따라잡기.
- `@Transactional` 없음: at-least-once 발행, 중복은 소비측 seq-guard가 흡수.

### 2.2 Worker 재기동 후 pending 재처리 — `projection/async/ProjectionWorker.java`

- `SmartLifecycle` 구현 → `stop()`/`start()`로 소비 루프 제어 가능(테스트에서 "인스턴스 다운" 근사).
- 단일 소비 스레드가 `consumeNew()`(XREADGROUP `>`) → `reclaimPending()`(XPENDING + XCLAIM) 순서로 수행.
- **XACK 타이밍**: `ProjectionApplier.apply()`의 `@Transactional` 커밋 성공 후에만 XACK.
  커밋 전 크래시 시 메시지는 pending 잔류 → 재기동 후 재청구로 복구(유실 0).
- `DataAccessException`(Redis 접근 예외 등) 발생 시 `ERROR_BACKOFF_MS`(1초) 대기 후 루프 지속(BR-4).

### 2.3 멱등 적용 (중복 저장 차단) — `projection/async/ProjectionApplier.java` (seq-guard)

- `last_applied_seq` 기준 `incoming_seq <= last_applied_seq`이면 적용 생략(BR-2).
- 동일 이벤트 2회 소비해도 read model은 1회 적용과 동일. (기존 `AsyncPipelineIntegrationTest` AC-4/6 커버)

### 2.4 append↔발행 분리 (부분 실패 격리) — `event/EventStore.java`, `command/CommandHandler.java`

- `event` INSERT와 `outbox` INSERT는 **같은 DB 트랜잭션**(원자성, BR-1).
- 발행(Redis)은 트랜잭션 밖 비동기 → Redis 장애가 append를 막지 않음.
- 진실의 원천은 event store. read model은 언제든 replay로 재구축 가능(설계서 §9.3).

> **기존 테스트가 이미 커버하는 것**: 중복 이벤트를 1회만 적용(seq-guard 멱등)하고 복원 결과가
> 일관됨 — `AsyncPipelineIntegrationTest`(AC-4/6), `RestApiIntegrationTest`(AC6) 등. 본 설계는
> 그 공백인 **인프라 단절 → 복구 사이클**만 채운다.

---

## 3. 설계 원칙

### 3.1 [P-1] 공유 싱글톤 컨테이너 절대 불가침

`support/AbstractIntegrationTest`는 PostgreSQL·Redis를 **static 싱글톤으로 1회 기동하고 JVM
종료까지 stop하지 않는다**(여러 통합 클래스가 한 컨텍스트/컨테이너를 공유, Ryuk가 정리). 장애 주입
테스트가 이 컨테이너를 멈추면 **같은 컨테이너를 공유하는 다른 모든 통합 테스트가 connection
refused로 깨진다.**

→ 장애 주입 테스트는 **자체 컨테이너 토폴로지를 직접 관리하는 별도 베이스**(`AbstractFaultInjectionTest`)를
사용한다. 기존 베이스를 상속하지 않는다.

### 3.2 [P-2] 네트워크 레벨 주입 (Toxiproxy)

컨테이너를 `stop()`하면 매핑 포트가 바뀌어 `@ServiceConnection` 재주입이 깨진다. 대신 **Toxiproxy
프록시를 DB/Redis 앞단에 두고 `setConnectionCut(true/false)`로 연결만 차단/복구**한다.

- 포트가 고정되어 Spring 데이터소스/Redis 설정이 안정적으로 유지된다.
- 연결 거부(connection refused)가 즉시 발생 → 타임아웃을 기다리지 않아 테스트가 빠르고 결정적.
- 실제 네트워크 단절(서버 살아있고 네트워크만 끊김)에 가장 근접한 충실도.

### 3.3 [P-3] 컨텍스트 격리

`@DynamicPropertySource`로 프록시 주소를 주입하므로 이 베이스는 기존 통합 테스트와 **다른 Spring
컨텍스트 캐시 키**를 갖는다(프로퍼티가 다름). worker `stop()` 등 컨텍스트 상태 변경이 다른 테스트로
누수되지 않는다. 안전을 위해 클래스 종료 시 컨테이너를 정리하고, 필요 시 `@DirtiesContext`를 둔다.

### 3.4 [P-4] 결정적 검증 (Awaitility)

비동기 복구는 시점이 비결정적이므로 `support/AwaitProjection`(최대 30초 폴링)으로 "복구 완료
상태"에 도달할 때까지 대기 후 단언한다. 고정 `sleep` 금지.

### 3.5 [P-5] event store = 정합성 기준선

모든 시나리오의 최종 단언은 "**read model이 event store와 일치**"다. event store는 장애와 무관하게
정확하므로(append는 동기·원자적), 복구 성공 = read model이 event store를 따라잡음으로 정의한다.

---

## 4. 테스트 인프라 아키텍처

### 4.1 컨테이너 토폴로지

```
                         ┌─────────────────────────────┐
   Spring App (test JVM) │   Docker Network (공유)      │
        │                │                             │
        │  jdbc://toxi:P1│  ┌──────────────┐           │
        ├───────────────▶│  │ Toxiproxy    │──proxy──▶ │  PostgreSQL(:5432)
        │                │  │  - pgProxy   │  alias    │   (networkAlias)
        │  redis://toxi:P2  │  - redisProxy│──proxy──▶ │  Redis(:6379)
        └───────────────▶│  └──────────────┘  alias    │   (networkAlias)
                         └─────────────────────────────┘
   setConnectionCut(true)  → proxy 경로 차단(앱↔DB/Redis 단절)
   setConnectionCut(false) → 복구
```

### 4.2 `AbstractFaultInjectionTest` (신규)

```java
@SpringBootTest(webEnvironment = RANDOM_PORT)
@ActiveProfiles({"test", "fault"})            // fault 프로파일로 타임아웃 단축
@AutoConfigureTestDatabase(replace = NONE)
public abstract class AbstractFaultInjectionTest {

    static final Network NETWORK = Network.newNetwork();

    static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:16").withNetwork(NETWORK);

    static final RedisContainer REDIS =
        new RedisContainer(DockerImageName.parse("redis:7")...).withNetwork(NETWORK);

    static final ToxiproxyContainer TOXIPROXY =
        new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0").withNetwork(NETWORK);

    static ToxiproxyContainer.ContainerProxy PG_PROXY;
    static ToxiproxyContainer.ContainerProxy REDIS_PROXY;

    static {
        POSTGRES.start(); REDIS.start(); TOXIPROXY.start();
        PG_PROXY    = TOXIPROXY.getProxy(POSTGRES, 5432);
        REDIS_PROXY = TOXIPROXY.getProxy(REDIS, 6379);
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", () ->
            "jdbc:postgresql://" + PG_PROXY.getHost() + ":" + PG_PROXY.getProxyPort()
                + "/" + POSTGRES.getDatabaseName());
        r.add("spring.datasource.username", POSTGRES::getUsername);
        r.add("spring.datasource.password", POSTGRES::getPassword);
        r.add("spring.data.redis.host", REDIS_PROXY::getHost);
        r.add("spring.data.redis.port", REDIS_PROXY::getProxyPort);
    }

    // 헬퍼: cutDb()/healDb()/cutRedis()/healRedis()
    protected void cutDb()   { PG_PROXY.setConnectionCut(true); }
    protected void healDb()  { PG_PROXY.setConnectionCut(false); }
    protected void cutRedis(){ REDIS_PROXY.setConnectionCut(true); }
    protected void healRedis(){ REDIS_PROXY.setConnectionCut(false); }
}
```

> Toxiproxy API는 **testcontainers 1.19.8**(spring-boot 3.3.5 BOM 관리, 확인 완료) 기준
> `getProxy(container, port)` + `ContainerProxy.setConnectionCut(boolean)` 정식 API를 사용한다
> (이 버전에서는 deprecated 아님). 이미지는 `ghcr.io/shopify/toxiproxy:2.5.0`.

### 4.3 fault 프로파일 (`application-fault.yml`)

차단 시 빠른 실패를 위해 타임아웃을 단축한다(테스트 한정, 운영 무영향).

```yaml
spring:
  datasource:
    hikari:
      connection-timeout: 2000      # DB 차단 시 2초 내 실패(기본 30초 → 테스트 지연 방지)
      validation-timeout: 1000
      maximum-pool-size: 10
  data:
    redis:
      timeout: 1000                 # Lettuce command timeout
      connect-timeout: 1000
```

---

## 5. 시나리오 상세

각 시나리오는 **감지 가설 → 장애 주입(When) → 복구(When) → 정합성 단언(Then)** 구조로 기술한다.
설계서 §9의 "감지/완화/복구" 항목과 1:1 대응한다.

### 5.1 FI-1 서버 다운 (인스턴스 장애) — `WorkerOutageFaultTest`

설계서 §9.1 "복구: 죽은 인스턴스의 stream pending은 XAUTOCLAIM으로 타 인스턴스가 이어받아 처리"를 실증.

| 단계 | 동작 |
|------|------|
| Given | 세션 생성·참여. Projection Worker 정상 가동. |
| When(장애) | `projectionWorker.stop()` — 소비 인스턴스 다운 시뮬레이션. |
| When(부하) | 그 사이 메시지 N건 append → event/outbox 기록·relay가 stream 적재(소비자 없음 → pending/미소비 누적). read model 미반영 확인(`message_count` 정체). |
| When(복구) | `projectionWorker.start()` — 인스턴스 재기동. |
| Then | `awaitProjection`: read model이 event store 기준(N건)으로 따라잡을 때까지 대기 후 일치 단언. 유실 0. |
| 보강 | apply 도중 강제 중단(pending 잔류) 후 재기동 시 **재청구로 복구**, 이미 적용된 seq는 재적용 안 됨(seq-guard) 단언. |

**핵심 단언**: 인스턴스가 죽은 동안 수집된 이벤트가 재기동 후 **누락 없이** read model에 반영된다.

### 5.2 FI-2 DB 장애 / 성능 저하 — `DbOutageFaultTest`

설계서 §9.2 대응.

| 단계 | 동작 |
|------|------|
| Given | 세션 생성·정상 append 1건 성공 확인. |
| When(장애) | `cutDb()` — DB 연결 차단. |
| Then(감지) | `POST /sessions/{id}/events` 호출 시 append 실패 → `DataAccessException` 전파 → HTTP 5xx. event/outbox에 **부분 기록 없음**(트랜잭션 원자성, BR-1). |
| When(복구) | `healDb()` — 연결 복구. |
| Then(복구) | 동일 세션에 정상 append 성공 → 이후 projection 정상 반영. seq 연속성 유지(채번 권위 보존). |
| 보강(선택) | **커넥션 고갈**: `maximum-pool-size=1`인 별도 컨텍스트에서 커넥션을 점유한 채 동시 요청 → `connection-timeout`(2초) 내 실패 단언. 구현 난도/불안정 시 §9.2 문서 분석으로 갈음(아래 주). |

> **락 경합·커넥션 고갈 주**: 락 경합 정밀 재현은 타이밍 의존적이라 flaky 위험이 높다. 설계서
> §9.2가 "seq 채번이 세션 단위 락이라 경합이 세션 국소로 한정"을 이미 분석했으므로, 커넥션
> 고갈은 가능하면 테스트로, 락 경합은 문서 분석으로 둔다(YAGNI·안정성 우선).

**핵심 단언**: DB 장애 중 부분 저장이 발생하지 않고(원자성), 복구 후 즉시 정상화된다.

### 5.3 FI-3 데이터 유실 / 부분 실패 — `RedisOutageFaultTest` (최고 임팩트)

설계서 §9.3 "Outbox로 이벤트 유실 방지(커밋=발행 보장)" + QE-2를 실증. Outbox 패턴의 핵심 가치.

| 단계 | 동작 |
|------|------|
| Given | 세션 생성. Redis·Worker 정상. |
| When(장애) | `cutRedis()` — Redis 연결 차단(발행·소비 경로 단절). |
| When(부하) | 메시지 N건 append. **append는 성공**(DB만 사용) → event N건 기록 + `outbox.published=false` N건 적체. relay XADD는 실패(WARN 로그)하나 데이터 유실 0. |
| Then(감지) | 미발행 outbox 수 == N, read model 미반영(lag) 확인. |
| When(복구) | `healRedis()` — Redis 복구. |
| Then(복구) | `awaitProjection`: relay가 미발행분 재발행 → worker 소비 → read model이 event store 기준 N건으로 **완전 일치**. 미발행 outbox 0. |

**핵심 단언**: 인프라 장애 중에도 이벤트 유실이 0이며(append=DB 커밋 보장), 복구 후 outbox relay가
자동으로 정합성을 회복한다.

---

## 6. 변경 사항 (최소 침습)

### 6.1 빌드 의존성 — `build.gradle.kts`

```kotlin
testImplementation("org.testcontainers:toxiproxy")   // 버전: spring-boot BOM 관리
```

추가 1줄. toxiproxy-java 클라이언트는 transitive로 포함된다.

### 6.2 테스트 설정 — `src/test/resources/application-fault.yml` (신규)

§4.3의 타임아웃 단축. `test` 프로파일과 함께 `fault` 프로파일로 활성화. **운영 설정 무변경.**

### 6.3 신규 테스트 파일

| 파일 | 역할 |
|------|------|
| `support/AbstractFaultInjectionTest.java` | Toxiproxy 토폴로지 + 차단/복구 헬퍼 |
| `integration/WorkerOutageFaultTest.java` | FI-1 서버 다운 |
| `integration/DbOutageFaultTest.java` | FI-2 DB 장애 |
| `integration/RedisOutageFaultTest.java` | FI-3 Redis 단절/부분 실패 |

### 6.4 프로덕션 코드 변경

**없음**(복구 로직은 기존 구현 재사용). 단, 테스트 중 복구 결함이 드러나면 별도 수정 항목으로 처리.

---

## 7. 검증 기준 (Definition of Done)

> **실행 결과(2026-06-01)**: `./gradlew test` → **80 tests / 0 failures / 0 errors** (3m 50s). 기존 77 + fault 3.

- [x] `./gradlew test` 전체 그린.
- [x] **기존 77개 테스트 회귀 0** (공유 컨테이너 불가침 원칙 P-1 검증 — 80 = 77 + 3).
- [x] FI-1: worker 다운 중 수집 이벤트가 재기동 후 누락 없이 반영(`WorkerOutageFaultTest`).
- [x] FI-2: DB 차단 시 append 예외(부분 저장 없음·seq 미소비), 복구 후 정상화(`DbOutageFaultTest`).
- [x] FI-3: Redis 차단 중 outbox 적체·유실 0, 복구 후 재발행으로 read model 완전 일치(`RedisOutageFaultTest`).
- [x] 각 테스트가 결정적(Awaitility, 고정 sleep 없음)으로 통과.
- [x] `context/realtime-chat/status.md`의 BONUS-6·FR-16·FR-22 → ✅ 갱신, 장애 시나리오 추적 반영.

---

## 8. 트레이드오프 & 리스크

| 항목 | 선택 | 포기/리스크 | 완화 |
|------|------|-------------|------|
| 주입 방식 | Toxiproxy 네트워크 프록시 | 컨테이너 3개 기동(무거움) | 별도 베이스로 한정, CI 1회 기동 |
| 분산 근사 | 단일 JVM worker stop/start | 실제 멀티 인스턴스 페일오버 아님 | XPENDING/XCLAIM 경로 자체는 동일하게 검증됨 |
| 락 경합 | 문서 분석(§9.2) | 테스트 미실증 | flaky 위험 회피, 커넥션 고갈은 테스트 시도 |
| 컨텍스트 | 전용 컨텍스트 캐시 | 테스트 부팅 비용 증가 | fault 테스트는 소수 클래스로 제한 |

### 8.1 알려진 리스크

- **Docker 환경 의존**: Toxiproxy 이미지 pull 필요(최초 1회). CI/로컬에 Docker 필수(기존 통합
  테스트와 동일 전제).
- **Toxiproxy API 버전차**: 확정 버전 testcontainers 1.19.8에서는 `getProxy()`/`setConnectionCut()`이
  정식 API다. 향후 1.20.x로 BOM이 올라가면 deprecated되므로, 그때는 `ToxiproxyClient` 직접 사용으로
  마이그레이션한다(현 시점 불필요).
- **타임아웃 상호작용**: Hikari/Lettuce 타임아웃이 너무 길면 차단 테스트가 지연 → fault 프로파일로 단축.
- **JDBC socketTimeout 필수(구현 중 발견)**: Hikari `connection-timeout`은 *풀에서 새 커넥션을 빌리는*
  대기만 제한한다. 차단 시점에 풀에 이미 있던 커넥션으로 쿼리하면 끊긴 소켓의 read가 PostgreSQL JDBC
  기본 `socketTimeout=0`(무한)으로 **영원히 블록**된다. JDBC URL에 `socketTimeout=2&connectTimeout=2`를
  넣어야 차단이 2초 내 예외로 전환된다(미설정 시 FI-2가 hang). 또한 트랜잭션 쿼리 실패 후 롤백마저
  실패하면 `DataAccessException`이 아닌 `TransactionException`(TransactionSystemException)이 나오므로
  단언은 두 계열을 모두 허용(`isInstanceOfAny`)한다.
- **로컬 Docker 리소스(운영 주의)**: 테스트를 강제 중단(JVM 비정상 종료)하면 Ryuk가 정리 타이밍을 놓쳐
  컨테이너가 좀비로 누적되고, 누적 시 신규 컨테이너 기동이 느려진다. 중단을 반복했다면
  `docker container prune -f`로 정리한다.

---

## 9. 향후 작업 (후속 Phase 후보)

- BONUS-3 부하 테스트(k6/Gatling): append TPS·restore latency 측정.
- 다중 인스턴스 실제 페일오버(docker-compose 2-노드 + LB) E2E.
- DLQ 자동 재처리 도구 + 재처리 시나리오 테스트.
- 정합성 배치 체크(스냅샷 ↔ 이벤트) 잡 + 그 테스트.

---

## 부록 A. 시나리오 ↔ 설계서 §9 매핑 요약

| 본 테스트 | 설계서 §9 항목 | 실증하는 "복구" |
|-----------|----------------|-----------------|
| WorkerOutageFaultTest | §9.1 서버 다운 | stateless 재기동 + pending 재청구(XPENDING/XCLAIM) |
| DbOutageFaultTest | §9.2 DB 장애 | 원자적 트랜잭션으로 부분 저장 차단, 복구 후 재개 |
| RedisOutageFaultTest | §9.3 데이터 정합성 | Outbox 커밋=발행 보장, replay 재구축, 유실 0 |
