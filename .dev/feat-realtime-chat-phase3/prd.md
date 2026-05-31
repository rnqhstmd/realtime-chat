# PRD: Phase 3 — 수평 확장 팬아웃 · presence TTL · resume-by-seq · 관측성

> 확정일: 2026-05-31 · 브랜치: feat/realtime-chat-phase3 · 모드: normal
> 확정 결정: (1) Phase 3 범위 4개 전부 포함(관측성은 [Should]) (2) presence = 클라이언트 heartbeat + Redis TTL(90s), SessionDisconnectEvent 즉시 트리거 보조 (3) resume 응답에 `hasMore` 필드 포함

## 배경

**현재 제품 상태 (Phase 1·2 완료, main 머지):**
- Phase 1 (PR #2): 이벤트 수집(append) → 동기 projection → afterCommit STOMP broadcast. REST API(세션 생성/참여/종료, 이벤트 수집, 타임라인 조회), 복원(snapshot+replay).
- Phase 2 (PR #3): 완전 비동기 전환. Transactional Outbox → Redis Stream(`events`) → Projection Worker(XREADGROUP/seq-guard/gap-fill) → read model. 재시도/DLQ(`events:dlq`), 지수 백오프, Snapshot 자동화, projection lag 메트릭(`ProjectionLagMetrics`). 동기 projection 제거(`chat.projection.sync-enabled=false` 디버그 플래그만 보존).

현재 파이프라인은 **단일 인스턴스 기준** 완성되어 있다. 다음 네 공백이 과제 평가 기준(FR-12 재연결 정합성, FR-13 수평 확장, FR-14 관측성) 충족에 직접 영향을 준다.

1. **팬아웃 단절**: `SimpMessagingTemplate`은 로컬 인메모리 STOMP 브로커에만 전달한다. 인스턴스를 2대 이상 띄우면 다른 노드에 WebSocket 연결한 참여자는 이벤트를 받지 못한다.
2. **presence 자동 만료 없음**: `PRESENCE_CHANGED` 이벤트를 명시적으로 보내야만 OFFLINE으로 전환된다. 클라이언트 비정상 종료(크래시, 네트워크 단절) 시 ONLINE 상태가 영구 잔존한다.
3. **재연결 정합성 미구현**: 재연결 시 클라이언트는 전체 timeline 재조회 외에 누락 이벤트만 선택적으로 수신하는 수단이 없다. seq 기반 delta replay API가 없다.
4. **관측성 부재**: `ProjectionLagMetrics`가 `AtomicLong`으로 lag만 인메모리 보관한다. 외부 메트릭 수집(Prometheus), 구조화 로그, Actuator 엔드포인트가 없어 운영 상태를 확인할 방법이 없다.

이 네 항목을 Phase 3에서 완성한다. 단, 본 프로젝트는 과제/POC·면접 평가용이므로 **평가 기준 충족 + 일관된 완성도**를 우선한다. 팬아웃·presence TTL·resume-by-seq는 [Must]로, 관측성은 [Should]로 두어 Must 완성 후 추가하는 방식으로 범위 위험을 줄인다.

## 목표

| 목표 | 성공 지표 |
|------|-----------|
| 수평 확장 시 모든 노드의 참여자에게 이벤트 전달 | 인스턴스 2대 기준, 어느 노드에 연결된 참여자도 이벤트 수신 |
| 비정상 종료 참여자 presence 자동 OFFLINE 전환 | heartbeat 미수신 후 TTL 내 OFFLINE 이벤트 자동 발행 |
| 재연결 후 누락 이벤트 선택적 수신 | `afterSeq` 파라미터로 delta 이벤트만 응답(+`hasMore` 페이징 신호) |
| 운영 상태 외부 노출 | `/actuator/prometheus`에서 projection lag·처리량·DLQ 카운터 스크레이핑 가능 |

## 요구사항

### 기능 요구사항

- **[Must] FR-P3-1 (Redis Pub/Sub 팬아웃 백플레인)**: `CommandHandler`의 afterCommit broadcast 시점에 `SimpMessagingTemplate` 직접 전달 대신, Redis Pub/Sub 채널 `chat.fanout.{sessionId}`로 이벤트를 publish한다. 각 인스턴스는 기동 시 해당 채널 패턴을 subscribe하여 수신 메시지를 로컬 `SimpMessagingTemplate`으로 전달한다. `SessionBroadcaster` 인터페이스의 신규 구현체(`RedisPubSubSessionBroadcaster`)로 교체하며, 기존 `SimpSessionBroadcaster`는 단일 인스턴스 호환용으로 보존한다.
- **[Must] FR-P3-2 (presence TTL + heartbeat 자동 OFFLINE 전환)**: 클라이언트는 STOMP 연결 유지 중 30초마다 heartbeat 이벤트(`PRESENCE_CHANGED`, presence=ONLINE)를 전송한다. 서버는 Redis에 `presence:{sessionId}:{participantId}` 키를 TTL 90초로 유지·갱신한다. TTL 만료를 감지하는 스케줄러(5초 주기 폴링)가 `PRESENCE_CHANGED`(presence=OFFLINE) 이벤트를 정상 수집 경로(CommandHandler)로 자동 발행하여 Projection·팬아웃이 동일하게 처리한다. STOMP `SessionDisconnectEvent` 감지 시에도 즉시 OFFLINE 전환을 트리거한다(보조 경로).
- **[Must] FR-P3-3 (resume-by-seq delta 조회 API)**: `GET /sessions/{id}/events?afterSeq={seq}&limit={n}` 엔드포인트를 추가한다. `afterSeq` 이후(초과) seq 이벤트를 오름차순으로 최대 `limit`개 반환한다. `limit` 기본값 100, 최대값 500. 응답 본문은 `events`(각 `seq`, `type`, `payload`, `occurredAt`) + **`hasMore`**(반환 후 더 가져올 이벤트 존재 여부) 필드를 포함한다. 재연결 클라이언트는 이 API로 누락 구간만 수신하고 STOMP를 재구독한다.
- **[Should] FR-P3-4 (Micrometer + Prometheus 메트릭 노출)**: `ProjectionLagMetrics`를 Micrometer `Gauge`로 노출하여 `chat.projection.lag.millis`(last, max)를 등록한다. DLQ 적재 횟수(`chat.projection.dlq.failures`), Redis Stream 처리량(`chat.stream.processed`), Outbox relay 처리량(`chat.outbox.relayed`)을 카운터로 등록한다. `spring-boot-starter-actuator`와 `micrometer-registry-prometheus`를 의존성에 추가하고 `/actuator/prometheus`를 활성화한다.
- **[Should] FR-P3-5 (Actuator 헬스/인포 엔드포인트)**: `/actuator/health`에서 Redis·PostgreSQL 연결 상태를 확인할 수 있다. `/actuator/info`에서 애플리케이션 정보를 반환한다.
- **[Could] FR-P3-6 (MDC 기반 구조화 로그)**: 이벤트 수집·projection·broadcast 경로에서 `sessionId`, `seq`, `eventType`을 MDC에 주입하여 로그에 포함한다. JSON 로그 포맷(logstash-logback-encoder)은 이번 Phase 제외(Phase 4 후보).

### 비즈니스 규칙

- **[Must] BR-1 (팬아웃 채널 격리)**: Redis Pub/Sub 채널은 세션 단위로 격리한다(`chat.fanout.{sessionId}`). 세션 간 메시지 혼선이 발생하지 않는다.
- **[Must] BR-2 (presence TTL 수치)**: heartbeat 전송 주기 30초 / Redis presence 키 TTL 90초(heartbeat 3회 미수신 시 만료) / 만료 감지 스케줄러 폴링 주기 5초 / TTL 만료 후 OFFLINE 전환까지 최대 지연 95초. 수치는 `chat.redis.*` 또는 `chat.presence.*` 프로퍼티로 조정 가능하며, 테스트 프로파일에서 단축(예: TTL 3초)한다.
- **[Must] BR-3 (presence OFFLINE 전환 중복 방지)**: 동일 참여자가 이미 OFFLINE이면 OFFLINE 이벤트를 재수집하지 않는다. `participant_view.presence`를 확인하여 이미 OFFLINE이면 건너뛴다. STOMP disconnect와 TTL 만료가 동시 발생해도 먼저 처리된 경로만 OFFLINE 이벤트를 수집한다.
- **[Must] BR-4 (resume-by-seq 데이터 범위)**: `EventStore.findBySeqRange`(또는 동등 쿼리)를 통해 seq 범위를 조회한다. 존재하지 않는 sessionId면 404. afterSeq가 현재 최대 seq 이상이면 빈 배열(`events: []`, `hasMore: false`)을 반환한다. `hasMore`는 `afterSeq` 이후 남은 이벤트 수가 반환한 개수를 초과할 때 true.
- **[Must] BR-5 (기존 STOMP 구독 경로 유지)**: 클라이언트 구독 토픽 `/topic/session.{id}`(현 broadcast 목적지 규약)는 변경하지 않는다. Pub/Sub 교체는 서버 내부 전달 경로만 변경한다.
- **[Should] BR-6 (메트릭 초기화 정책)**: `chat.projection.lag.millis` max 및 누적 카운터는 애플리케이션 재시작 시 0에서 다시 시작한다.
- **[Must] BR-7 (팬아웃 장애 격리)**: Redis Pub/Sub publish 실패는 로그로 남기고 무시한다(이벤트 수집은 이미 EventStore에 커밋됨). 실시간 전달만 지연되며, 재연결 후 resume-by-seq로 복구 가능하다(이벤트 유실 없음).

### 품질 기대

- **[Should] QE-1**: 단일 인스턴스 환경에서 Phase 1·2 기존 통합 테스트가 모두 통과한다.
- **[Should] QE-2**: presence TTL 만료 → OFFLINE 자동 전환이 통합 테스트에서 Awaitility로 검증된다(테스트 TTL 단축, 최대 대기 10초).
- **[Should] QE-3**: resume-by-seq API가 정상 응답과 경계 조건(afterSeq=최대 → 빈 결과/`hasMore=false`, limit 초과 → 400, afterSeq 누락 → 400, 없는 세션 → 404)을 모두 검증한다.
- **[Should] QE-4**: Pub/Sub 팬아웃이 통합 테스트에서 검증된다(동일 세션 채널 발행 → 구독 수신, 세션 격리).

## 사용자 시나리오

**정상 1 — 다중 인스턴스 팬아웃**: 인스턴스 A에 참여자 X, B에 Y 연결. X가 MESSAGE_SENT 전송 → A의 CommandHandler가 append, afterCommit에서 `chat.fanout.{sid}` publish. A·B 모두 구독 중이므로 각자 로컬 STOMP로 전달 → Y 수신.

**정상 2 — presence TTL 자동 만료**: X 접속 시 `presence:{sid}:{pid}` TTL=90s. 30s마다 heartbeat로 갱신. 크래시로 heartbeat 중단 → 90s 후 키 만료. 스케줄러(5s)가 만료 감지 → `participant_view.presence`가 OFFLINE이 아니면 PRESENCE_CHANGED(OFFLINE) 자동 수집 → 팬아웃 전파.

**정상 3 — 재연결 delta 수신**: 클라이언트가 seq=42까지 수신 후 단절. 재연결 후 `GET /sessions/{id}/events?afterSeq=42&limit=100` 호출 → seq 43~N + `hasMore` 수신 → STOMP 재구독.

**예외 — Redis Pub/Sub 장애**: publish 실패 시 afterCommit broadcast가 예외를 로그로 남기고 무시. 이벤트는 EventStore에 수집된 상태이므로 재연결 후 resume-by-seq로 복구(유실 없음, 전달만 지연).

**엣지**: afterSeq=0 → seq 1부터(limit 내). 이벤트 없는 세션 → `events:[]`, `hasMore:false`. TTL 만료 시 이미 OFFLINE → 미수집(BR-3). disconnect와 TTL 동시 → 먼저 처리된 것만 수집.

## 영향 범위

**영향받는 기존 기능:**
- `command/CommandHandler` afterCommit broadcast — `SessionBroadcaster` 구현체 교체. 호출 인터페이스 동일.
- `realtime/SimpSessionBroadcaster` — 신규 `RedisPubSubSessionBroadcaster` 추가, `@Primary`/프로퍼티 스위치로 단일 인스턴스 호환 유지.
- `projection/async/ProjectionLagMetrics` — Micrometer 연동 시 등록부 추가(기존 `lastLagMillis()`/`maxLagMillis()` 호출부 유지 또는 위임).
- `application.yml` — `chat.redis.*`/`chat.presence.*`에 `presence-ttl-seconds`, `heartbeat-interval-seconds`, `expiry-poll-seconds`, `fanout-channel-prefix` 추가.

**기존 테스트 영향:**
- `AbstractIntegrationTest`는 Redis Testcontainer를 이미 사용 → Pub/Sub·presence·resume 테스트 추가 가능.
- presence TTL 테스트는 테스트 프로파일에서 TTL 단축.
- 기존 read-after-write 단언은 Phase 2에서 이미 Awaitility 적용됨 — 회귀 방지 확인.

**인프라 추가:**
- `build.gradle.kts`: `spring-boot-starter-actuator`, `micrometer-registry-prometheus`.
- 별도 Redis 인스턴스 불필요(기존 Redis에 Pub/Sub 채널·presence 키 추가 사용).

**하위 호환성:**
- 클라이언트 STOMP 구독 토픽 변경 없음. 기존 REST 엔드포인트 변경 없음(`GET /sessions/{id}/events`는 신규).
- DB 스키마 변경 필요 여부는 설계에서 확정(presence는 기존 `participant_view.presence` 재사용 가능 전망 → V3 불요 가능성 높음).

## 수용 기준

- **AC-1**: 인스턴스 A에서 수집된 이벤트가 인스턴스 B에 WebSocket 연결된 클라이언트에게 전달된다. → [FR-P3-1, BR-1]
- **AC-2**: Pub/Sub 채널명이 `chat.fanout.{sessionId}` 형식이며, 다른 세션 채널 메시지가 혼입되지 않는다. → [BR-1]
- **AC-3**: heartbeat 미전송 후 90초 이내 Redis presence 키가 만료된다(테스트는 단축 TTL). → [FR-P3-2, BR-2]
- **AC-4**: presence 키 만료 후 최대 95초 이내 PRESENCE_CHANGED(OFFLINE) 이벤트가 EventStore에 수집되고 read model이 갱신된다. → [FR-P3-2, BR-2]
- **AC-5**: 참여자가 이미 OFFLINE이면 TTL 만료 스케줄러가 OFFLINE 이벤트를 중복 수집하지 않는다. → [BR-3]
- **AC-6**: `GET /sessions/{id}/events?afterSeq=42`가 seq 43 이상 이벤트를 오름차순으로 반환한다. → [FR-P3-3, BR-4]
- **AC-7**: afterSeq가 현재 최대 seq 이상이면 `events: []`, `hasMore: false`를 반환한다. → [BR-4]
- **AC-8**: 반환 가능한 이벤트가 limit를 초과하면 `hasMore: true`, limit개만 반환한다. → [FR-P3-3, BR-4]
- **AC-9**: `afterSeq` 파라미터 없이 호출하면 400 오류를 반환한다. → [FR-P3-3]
- **AC-10**: `limit` 초과 값(501 이상)으로 호출하면 400 오류를 반환한다. → [FR-P3-3]
- **AC-11**: 존재하지 않는 sessionId로 호출하면 404 오류를 반환한다. → [BR-4]
- **AC-12**: `/actuator/prometheus`에서 `chat_projection_lag_millis`, `chat_projection_dlq_failures_total`(또는 등가 메트릭)이 스크레이핑된다. → [FR-P3-4]
- **AC-13**: `/actuator/health`가 Redis·PostgreSQL 연결 상태를 포함한 200 응답을 반환한다. → [FR-P3-5]
- **AC-14**: Redis Pub/Sub publish 실패 시 이벤트 수집은 성공하고 broadcast 예외는 무시된다(유실 없음). → [BR-7]
- **AC-15**: 기존 Phase 1·2 통합 테스트가 모두 통과한다. → [QE-1]

## 제외 범위

- **Phase 4**: 분산 추적 에이전트 연동(Zipkin/Jaeger), JSON 구조화 로그(logstash-logback-encoder), 부하 테스트(BONUS-3), 장애 주입 테스트.
- **이번 미포함**: Redis Stream → Kafka 전환, WebRTC 등 추가 통신 방식(BONUS-5), Prometheus+Grafana 대시보드 실제 구성·대시보드 JSON(BONUS-4 — 메트릭 노출까지만), 인증/인가.
