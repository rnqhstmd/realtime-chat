# 실시간 대화 세션 — 이벤트 소싱 기반 설계서

- 작성일: 2026-05-28
- 수정일: 2026-05-28
- 관련 레포: rnqhstmd/realtime-chat
- 도메인 컨텍스트: `context/realtime-chat/`
- 요구사항 출처: `requirements/과제요구사항.md`

> **이 문서의 원칙**: 모든 설계 결정에는 *이유(Why)* 를 함께 기술한다. 면접 과제 평가의 핵심은 "기능 구현 여부"가 아니라 "일관된 모델과 근거 있는 트레이드오프"이기 때문이다.

---

## 0. 설계 전제

| 항목 | 결정 | 이유 |
|------|------|------|
| 기술 스택 | Spring Boot + PostgreSQL + Redis | 요구사항의 JSONB 언급 → PostgreSQL. Redis는 단일 인프라로 3역할(스트림·Pub/Sub·presence) 수행 |
| 아키텍처 | 이벤트 소싱 + CQRS | 과제가 평가하는 4영역(실시간/중복·순서/복원/운영)이 하나의 이벤트 모델에서 파생됨 |
| 메시징 인프라 | Redis 단일 | 진실의 원천은 PostgreSQL이고 Redis는 전송·캐시. Redis 유실은 Outbox 재발행으로 복구 가능. Kafka는 POC 규모에 운영 부담만 큼 |
| 구현 범위 | 최대 구현 + 가산점 | 단, 핵심을 end-to-end로 먼저 완성 후 가산점 적층(과대 착수로 인한 미완성 방지) |
| 규모 전제 | POC 수준 | 동시 세션 수백~수천, 세션당 메시지 수천~수만 건 기준으로 핫패스·인덱스 설계 |

---

## 1. 요구사항 → 설계 매핑

| 요구사항 | 설계 대응 | 구현/문서 |
|----------|-----------|-----------|
| 4.1 실시간 송수신 | WebSocket/STOMP + Redis Pub/Sub 팬아웃 (§6) | 구현 |
| 4.1 join/leave | PARTICIPANT_JOINED/LEFT 이벤트 → participant_view (§3,§4.3) | 구현 |
| 4.1 presence | Redis TTL 키 + PRESENCE_CHANGED 이벤트 (§6) | 구현 |
| 4.1 이벤트 수집 API | `POST /sessions/{id}/events`, command handler 단일 수렴 (§11) | 구현 |
| 4.1 중복 방지 | 2계층 멱등성 (§4.1) | 구현 |
| 4.1 순서 뒤바뀜 | 서버 채번 seq 권위 + projection seq-guard (§4.2) | 구현 |
| 4.1 시점 복원 | snapshot + replay (§4.3) | 구현 |
| 4.2 ERD/DDL/인덱스 | §3 | 구현(실 DDL) |
| 4.2 REST API 스펙 | §11 (OpenAPI) | 구현 |
| 4.2 재연결 정합성 | resume-by-seq (§4.2, §6) | 구현 |
| 4.2 수평 확장 | stateless + Redis backplane (§7) | 문서+구현 |
| 4.2 관측성 | §8 | 문서+구현 |
| 4.2 비동기 처리 | Outbox→Stream→projection, 재시도/DLQ/멱등 (§5) | 구현 |
| 4.2 장애 대응 | §9 | 문서 |
| 4.3 projection/replay 전략 | §4.3 | 구현 |
| 4.3 복원의 중복/순서 처리 | §4.3 (정제는 append에서 완료) | 구현 |
| 4.3 복원 비용/성능 | §4.3 비용 분석 | 문서+구현 |
| 4.4 쿼리/인덱스 최적화 | §10 핫패스 3종 | 문서 |
| 4.4 비동기 설계 | §5 | 문서 |
| 4.4 장애 시나리오 | §9 | 문서 |
| 5. 가산점 | snapshot 자동화(§5), projection 비동기(§5), resume(§6), 부하테스트(§12) | 구현 |

---

## 2. 아키텍처 개요

### 2.1 컴포넌트

```
            (1) STOMP/WebSocket                          (2) REST API
                    │                                          │
                    ▼                                          ▼
        ┌───────────────────────┐              ┌────────────────────────┐
        │  Realtime Gateway      │              │  Command API           │
        │  - 연결/세션 바인딩     │              │  - 세션 생성/종료       │
        │  - presence (Redis TTL)│              │  - 이벤트/메시지 수집    │
        └───────────┬───────────┘              └───────────┬────────────┘
                    └──────────────┬───────────────────────┘
                                   ▼
                       ┌─────────────────────────┐      ┌──────────────────────────┐
                       │  Command Handler         │      │  Event Store (PostgreSQL) │
                       │  - 멱등성 체크            │─────►│  append-only, 불변         │
                       │  - 세션별 seq 채번        │  TX  │  + outbox (동일 트랜잭션)  │
                       │  - [TX: event + outbox]  │      └──────────────────────────┘
                       └───────────┬─────────────┘
                                   │ commit 후 relay
                                   ▼
                       ┌─────────────────────────┐      ┌──────────────────────────┐
                       │  Outbox Relay            │─────►│  Redis Stream (events)    │
                       └─────────────────────────┘      └───────────┬──────────────┘
                                                                     │ consumer group
                    ┌────────────────────────────┬───────────────────┴───────────┐
                    ▼                             ▼                                ▼
        ┌────────────────────┐      ┌────────────────────┐         ┌────────────────────┐
        │ Projection Worker  │      │ Snapshot Service   │         │ Fan-out Publisher  │
        │ → read model 갱신  │      │ → 주기적 스냅샷     │         │ → Redis Pub/Sub     │
        │ (멱등, 재시도/DLQ) │      │ (N이벤트/시간)      │         │ → 타 인스턴스 전달  │
        └─────────┬──────────┘      └─────────┬──────────┘         └────────────────────┘
                  ▼                            ▼
        ┌──────────────────────────────────────────────────────┐
        │ Read Models: participant_view, message_view,           │
        │ session_view  +  snapshot                              │
        └──────────────────────────────────────────────────────┘
                  ▲
                  │ (A) 라이브 조회: read model 직접
                  │ (B) 시점 복원: snapshot(≤at) + event replay(snapshot.seq, at]
            ┌─────┴──────┐
            │ Query API  │
            └────────────┘
```

### 2.2 핵심 결정과 이유

| 결정 | 이유 |
|------|------|
| Event Store append-only/불변 | UPDATE/DELETE 없이 append만 → 락 경합·정합성 위험 최소화, 임의 시점 복원·감사 가능. 메시지 "수정/삭제"도 상태변경이 아닌 새 이벤트로 표현 |
| 세션별 단조증가 seq를 서버가 채번 | 순서의 단일 기준점. 클라 타임스탬프는 시계오차·조작 위험. 세션 단위 seq면 세션 내 전역순서 보장 + 복원 정렬 키로 직결 |
| Transactional Outbox | 이벤트 DB 저장과 브로커 발행의 이중쓰기 문제 회피. 같은 TX에 event+outbox → 커밋 후 relay → 유실 없는 at-least-once |
| Projection 비동기(Redis Stream) | 쓰기 경로를 읽기모델 갱신과 분리 → 쓰기 지연 최소화. consumer group의 ack/pending으로 재시도·DLQ 자연 구현 |
| Read Model 분리(CQRS) | 라이브 조회는 projection 직접 읽어 빠르게. 복원 같은 무거운 작업만 replay. 조회 패턴별 인덱스 최적화 자유도 |
| Redis Pub/Sub 팬아웃 | 다중 인스턴스에서 세션 참여자가 어느 노드든 메시지 수신. 수평 확장 backplane |
| 복원 = snapshot + 부분 replay | 전체 리플레이는 O(n)으로 비용 폭증. 최근접 스냅샷 이후만 replay → 비용 상한이 스냅샷 주기로 고정 |

---

## 3. 데이터 모델 (DDL · 인덱스 근거)

### 3.1 쓰기 측

```sql
CREATE TABLE session (
    id          UUID PRIMARY KEY,
    status      TEXT NOT NULL DEFAULT 'ACTIVE',   -- ACTIVE / ENDED
    last_seq    BIGINT NOT NULL DEFAULT 0,         -- 세션별 seq 채번 카운터
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at    TIMESTAMPTZ
);

CREATE TABLE event (
    event_id        UUID PRIMARY KEY,
    session_id      UUID NOT NULL REFERENCES session(id),
    seq             BIGINT NOT NULL,
    event_type      TEXT NOT NULL,   -- MESSAGE_SENT/EDITED/DELETED, PARTICIPANT_JOINED/LEFT, PRESENCE_CHANGED
    payload         JSONB NOT NULL,
    idempotency_key TEXT NOT NULL,
    actor_id        UUID,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_event_seq  UNIQUE (session_id, seq),
    CONSTRAINT uq_event_idem UNIQUE (session_id, idempotency_key)
);
CREATE INDEX ix_event_time ON event (session_id, occurred_at);  -- at(timestamp) → seq 매핑

CREATE TABLE outbox (
    id         BIGSERIAL PRIMARY KEY,
    event_id   UUID NOT NULL,
    session_id UUID NOT NULL,
    seq        BIGINT NOT NULL,
    published  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_outbox_unpub ON outbox (id) WHERE published = FALSE;  -- 미발행분만 부분 인덱스
```

| 결정 | 이유 |
|------|------|
| payload JSONB | 이벤트 타입이 이질적. 타입별 컬럼/테이블 분리는 스키마 폭증 + append 균일성 훼손. payload 검증은 append 전 커맨드 경계에서 완료, 핫패스는 payload 내부를 필터링하지 않음 → JSONB 약점이 핫패스에 없음 |
| `UNIQUE(session_id, seq)` | 세션별 순서 무결성 강제 + replay 범위 스캔 인덱스로 동시 활용 |
| `UNIQUE(session_id, idempotency_key)` | 중복 유입을 DB 제약으로 강제(애플리케이션 분기 불필요) |
| outbox 부분 인덱스 `WHERE published=FALSE` | relay가 미발행분만 스캔. 발행 완료분은 인덱스에서 빠져 스캔 비용 일정 유지 |

### 3.2 읽기 측 (Projections)

```sql
CREATE TABLE participant_view (
    session_id       UUID NOT NULL,
    participant_id   UUID NOT NULL,
    status           TEXT NOT NULL,   -- JOINED / LEFT
    presence         TEXT,            -- ONLINE / OFFLINE
    joined_seq       BIGINT,
    left_seq         BIGINT,
    last_applied_seq BIGINT NOT NULL, -- projection 멱등 가드
    PRIMARY KEY (session_id, participant_id)
);
CREATE INDEX ix_part_active ON participant_view (session_id, status);

CREATE TABLE message_view (
    session_id       UUID NOT NULL,
    message_id       UUID NOT NULL,
    seq              BIGINT NOT NULL, -- 생성 seq (정렬 키)
    sender_id        UUID NOT NULL,
    content          TEXT,
    state            TEXT NOT NULL,   -- SENT / EDITED / DELETED
    created_at       TIMESTAMPTZ,
    last_updated_seq BIGINT NOT NULL,
    PRIMARY KEY (session_id, message_id)
);
CREATE INDEX ix_msg_recent ON message_view (session_id, seq DESC);  -- 최근 N개 조회

CREATE TABLE session_view (
    session_id        UUID PRIMARY KEY,
    status            TEXT NOT NULL,
    participant_count INT NOT NULL DEFAULT 0,
    message_count     BIGINT NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL,
    last_activity_at  TIMESTAMPTZ,
    ended_at          TIMESTAMPTZ
);
CREATE INDEX ix_session_list ON session_view (status, created_at DESC);  -- 목록/필터
```

### 3.3 스냅샷

```sql
CREATE TABLE snapshot (
    session_id UUID NOT NULL,
    up_to_seq  BIGINT NOT NULL,
    state      JSONB NOT NULL,  -- { participants[], recent_messages[N], counts }
    taken_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, up_to_seq)
);
```

| 결정 | 이유 |
|------|------|
| 스냅샷에 "최근 N개" 메시지만 | 요구사항이 "메시지 목록 또는 최근 N개" 허용. 전체 스냅샷은 크기 폭증 → 크기 상한을 N으로 고정. 더 과거가 필요하면 event 추가 replay |
| 스냅샷 PK `(session_id, up_to_seq)` | 멱등 생성(같은 seq 재생성 무해) + 최근접 스냅샷 조회 `up_to_seq <= atSeq ORDER BY DESC LIMIT 1` 커버 |

---

## 4. 핵심 메커니즘

### 4.1 중복 방지 — 2계층 멱등성

| 계층 | 위치 | 메커니즘 | 막는 것 |
|------|------|----------|---------|
| 1 수집 | append | `INSERT ... ON CONFLICT (session_id, idempotency_key) DO NOTHING` → 충돌 시 기존 이벤트 반환 | 중복 *유입* (클라이언트 재시도) |
| 2 적용 | projection | `last_applied_seq` 가드: `event.seq <= last_applied_seq`면 skip | 중복 *적용* (스트림 at-least-once 재전달) |

**이유**: 두 계층은 서로 다른 실패 지점을 막는다. 수집 멱등성만으로는 스트림 재전달을 못 막고, 적용 멱등성만으로는 중복 seq 채번을 못 막는다.

### 4.2 순서 뒤바뀜

- **순서의 권위 = 서버 append 시점의 seq.** 클라이언트가 어떤 순서로 보냈든, 서버가 받은 순서로 seq를 채번하고 그것을 진실로 정의한다.
  - *이유*: 분산 환경에서 클라이언트 의도 순서는 신뢰 불가(시계 오차/조작). "서버 수신 순서 = 진실"은 채팅의 표준 일관성 기준이며 명시적으로 문서화할 규칙이다.
- **Projection seq-guard 재조정**: worker는 세션별 `last_applied_seq`를 들고
  - `event.seq == last+1` → 적용
  - `event.seq > last+1` (gap) → event store에서 누락분 fetch 후 순서대로 적용 (스트림은 알림, **권위는 store**)
  - `event.seq <= last` → skip (중복)
- **재연결 정합성(resume)**: 클라이언트가 마지막 수신 `seq`를 들고 재접속 → 서버는 `event WHERE seq > lastSeq`를 전달. 끊긴 구간을 정확히 따라잡음.

### 4.3 시점 복원 (Point-in-time Restore)

```
입력: session_id, at (ISO-8601 timestamp 또는 seq)
1. at(ts)면 → atSeq = max(seq) where occurred_at <= at      [ix_event_time]
2. base = 최근접 snapshot where up_to_seq <= atSeq DESC LIMIT 1 (없으면 빈 상태, seq=0)
3. replay = event where seq in (base.up_to_seq, atSeq] ORDER BY seq
4. state = fold(base.state, replay)
5. 반환: 참여자 목록 + 메시지 목록(또는 최근 N) + 각 메시지 상태
```

fold 규칙 (이벤트 타입별):

| 이벤트 | 상태 전이 |
|--------|-----------|
| PARTICIPANT_JOINED | 참여자 집합에 추가 |
| PARTICIPANT_LEFT | 참여자 status=LEFT |
| MESSAGE_SENT | 메시지 추가 (state=SENT) |
| MESSAGE_EDITED | 존재 & 미삭제면 content 갱신 (seq 기준 last-edit-wins) |
| MESSAGE_DELETED | state=DELETED |
| PRESENCE_CHANGED | presence 갱신 |

**핵심 통찰 (4.3 "복원의 중복/순서 처리" 답)**: 복원 로직은 이미 *정제된* 로그를 fold만 한다. 중복 제거는 append의 멱등 제약에서, 정렬은 seq 권위에서 이미 끝났다. 따라서 복원 단계에는 중복·순서를 위한 별도 처리가 없다 — **정제는 쓰기 시점에 1회 완료**되는 것이 이 모델의 핵심 우아함이다.

**비용 분석**: replay 행수 = `atSeq - base.up_to_seq` ≤ 스냅샷 주기 N. 즉 복원 비용 상한이 N으로 고정. N을 키우면 스냅샷 저장 비용↓·복원 비용↑, 줄이면 반대. POC 기본 N=200~500, replay 행수를 메트릭으로 관측해 조정.

---

## 5. 비동기 파이프라인 (Outbox → Stream → Projection)

```
[TX] event INSERT + outbox INSERT  ──commit──►  Outbox Relay  ──XADD──►  Redis Stream "events"
                                                                              │ consumer group "proj"
                                                                              ▼
                                                          Projection Worker (XREADGROUP)
                                                          - seq-guard 멱등 적용
                                                          - 성공 → XACK
                                                          - 실패 → 미ACK (pending 잔류)
                                                                              │
                                          XAUTOCLAIM(idle>timeout) ◄──────────┘
                                          - attempt++ 지수 백오프 재시도
                                          - attempt > MAX → XADD "events:dlq" + 알람
```

| 요소 | 결정 | 이유 |
|------|------|------|
| Outbox Relay | 폴링(부분 인덱스) 또는 LISTEN/NOTIFY | 커밋된 이벤트는 반드시 발행 → 유실 0 |
| 재시도 | 미ACK 잔류 + XAUTOCLAIM + 지수 백오프 | 일시 장애 자동 회복, 영구 장애와 구분 |
| DLQ | `events:dlq` 스트림 + 알람 | 독성 메시지 격리, 본 파이프라인 정체 방지. event store가 원천이라 언제든 재처리 가능 |
| 멱등 | seq-guard (§4.1 계층2) | at-least-once 재전달 안전 |
| Snapshot 자동화 | N이벤트마다/주기적 트리거, PK 멱등 | 복원 비용 상한 유지 (가산점) |

---

## 6. 실시간 전송 & presence

- **전송**: STOMP over WebSocket. 구독 `/topic/session.{id}`, inbound SEND는 REST `/events`와 **동일 command handler로 수렴**(단일 진입 로직 → 일관성).
  - *이유*: 실시간은 양방향 → WebSocket. STOMP는 sub/pub 의미와 Spring 통합이 좋아 보일러플레이트 절감.
- **presence**: 연결 시 Redis 키 `presence:{session}:{participant}` SET + TTL, 하트비트로 갱신. 끊기면 TTL 만료로 자동 OFFLINE. 변동은 PRESENCE_CHANGED 이벤트로도 기록.
  - *이유*: presence는 휘발성·고빈도 → Redis TTL이 적합. 영속 감사용으로만 이벤트화.
- **resume**: 재접속 시 `lastSeq` 기반 누락 이벤트 재생(§4.2).

---

## 7. 수평 확장 & 세션 분산

- 앱은 **stateless**(로컬 상태는 WebSocket 연결뿐). 모든 REST는 아무 인스턴스나 처리.
- 이벤트 append → Redis Pub/Sub `session:{id}` 발행 → 모든 인스턴스가 자신에게 붙은 참여자에게 전달.
- **확장 경로**: 트래픽 증가 시 Redis Stream → **Kafka로 전환(파티션 키=session_id = 세션 샤딩)**, projection worker를 파티션별 스케일아웃. 선택적으로 WebSocket sticky 라우팅.
  - *이유*: 세션 단위 파티셔닝이면 세션 내 순서가 파티션 내 순서로 보존되어 projection 단순화.

---

## 8. 관측성

| 종류 | 내용 |
|------|------|
| 로그 | 구조화(JSON), 상관키: session_id, seq, event_id, idempotency_key, trace_id |
| 메트릭 | append rate/latency, **projection lag**(now − last applied event time), stream pending count, **DLQ size**, snapshot age, restore latency & replay 행수 |
| 추적 | OpenTelemetry: append → relay → projection 스팬 연결 |
| 핵심 SLI | projection lag, DLQ size — 파이프라인 건강 신호 |

*이유*: projection lag과 DLQ size가 임계 초과면 읽기 모델이 뒤처지거나 독성 메시지가 쌓이는 것 → 가장 먼저 알아야 할 신호.

---

## 9. 장애 대응 시나리오 (감지 → 완화 → 복구)

### 9.1 서버 다운 (인스턴스 장애)
- **감지**: health check/heartbeat 누락, 인스턴스 메트릭 소실
- **완화**: LB가 트래픽 제거, WebSocket 클라이언트 자동 재연결(다른 인스턴스), presence TTL 만료로 자동 정리
- **복구**: stateless라 재기동 즉시 합류. 죽은 인스턴스의 stream pending은 XAUTOCLAIM으로 타 인스턴스가 이어받아 처리

### 9.2 DB 장애 / 성능 저하 (커넥션 고갈, 락 경합)
- **감지**: 커넥션풀 포화율, 쿼리 p99 지연, 락 대기 메트릭
- **완화**: append 우선순위 보장, 읽기는 스냅샷/캐시로 degrade, 커넥션풀 상한+statement timeout, seq 채번이 세션 단위 락이라 경합이 세션 국소로 한정
- **복구**: read replica로 조회 분산, 백프레셔로 유입 조절, 우선순위 큐

### 9.3 데이터 유실 / 정합성 (중복 저장, 부분 실패)
- **감지**: projection lag 급증, DLQ 증가, 스냅샷↔이벤트 정합성 배치 체크
- **완화**: Outbox로 이벤트 유실 방지(커밋=발행 보장), 멱등 키로 중복 저장 원천 차단
- **복구**: **event store가 진실의 원천이므로 read model을 언제든 replay로 재구축** — 이벤트 소싱의 최대 강점. projection 테이블 drop 후 재구축 가능

---

## 10. 쿼리 최적화 (핫패스 2~3개)

### Q1. 이벤트 append + 중복 체크 (최고빈도 쓰기)
```sql
UPDATE session SET last_seq = last_seq + 1 WHERE id = :sid RETURNING last_seq;
INSERT INTO event (event_id, session_id, seq, event_type, payload, idempotency_key, actor_id)
VALUES (:eid, :sid, :seq, :type, :payload, :idem, :actor)
ON CONFLICT (session_id, idempotency_key) DO NOTHING;
```
- 인덱스: `uq_event_idem`, `uq_event_seq`
- **예상 병목**: 동일 세션 고빈도 시 `session.last_seq` row 갱신 직렬화
- **개선 방향**: seq 채번을 Redis `INCR`로 옮기되 DB unique를 최종 권위로 유지(이중 안전). 세션 간은 본래 병렬이라 전체 처리량엔 영향 적음

### Q2. 라이브 메시지 최근 N개 (최고빈도 읽기)
```sql
SELECT * FROM message_view
WHERE session_id = :sid AND state <> 'DELETED'
ORDER BY seq DESC LIMIT :n;
```
- 인덱스: `ix_msg_recent (session_id, seq DESC)`
- **예상 병목**: `state <> 'DELETED'` 후필터로 삭제 다수 시 불필요 행 스캔
- **개선 방향**: partial index `WHERE state <> 'DELETED'` 또는 복합 `(session_id, state, seq DESC)`

### Q3. 시점 복원 (가장 무거운 읽기)
```sql
SELECT max(seq) FROM event WHERE session_id = :sid AND occurred_at <= :at;          -- atSeq
SELECT * FROM snapshot WHERE session_id = :sid AND up_to_seq <= :atSeq
  ORDER BY up_to_seq DESC LIMIT 1;                                                    -- base
SELECT * FROM event WHERE session_id = :sid AND seq > :baseSeq AND seq <= :atSeq
  ORDER BY seq;                                                                       -- replay
```
- 인덱스: `ix_event_time`, `snapshot` PK 역순, `uq_event_seq`
- **예상 병목**: 스냅샷 부재 구간이 길면 replay 행수 폭증
- **개선 방향**: 스냅샷 주기 보장 + replay 행수 메트릭 알람, N 동적 조정

---

## 11. API 스펙 (OpenAPI 요약)

| Method | Path | 설명 | 비고 |
|--------|------|------|------|
| POST | /sessions | 세션 생성 | |
| POST | /sessions/{id}/join | 참여 | PARTICIPANT_JOINED |
| POST | /sessions/{id}/events | 이벤트/메시지 수집 | `Idempotency-Key` 헤더 필수 |
| POST | /sessions/{id}/end | 종료 | |
| GET | /sessions | 목록(기간/상태/참여자 필터) | `ix_session_list` |
| GET | /sessions/{id}/timeline?at= | 시점 복원 | at = ISO-8601 ts 또는 seq |
| POST | /sessions/{id}/snapshots | (선택) 스냅샷 생성/갱신 | |
| GET | /sessions/{id}/events?from=&to= | (선택) 디버깅/검증 | |

WebSocket/STOMP: 구독 `/topic/session.{id}`, inbound SEND → REST와 동일 command handler.

| 결정 | 이유 |
|------|------|
| `Idempotency-Key` 헤더 | HTTP 표준 관행, REST/WS 공통 멱등 키 전달 |
| timeline at = ts 또는 seq 둘 다 | 운영(사람은 시각)·디버깅(개발자는 seq) 양쪽 편의 |
| REST/WS 단일 command handler 수렴 | 수집 경로가 둘이어도 멱등·순서·검증 로직은 1곳 → 일관성 |

---

## 12. 테스트 전략

| 레벨 | 내용 |
|------|------|
| 단위 | fold(이벤트→상태) 순수 함수, 멱등성, seq-guard 분기 |
| 통합 | append→projection→restore E2E (Testcontainers: PostgreSQL+Redis) |
| 장애 주입 | 중복 이벤트 주입 / 순서 뒤바뀜 주입 / projection 재시작 / Redis 끊김 후 outbox 재발행 |
| 재현 | 시나리오 시드 → 특정 시점 복원 결과 골든 검증 |
| 부하 | k6/Gatling — append 처리량 + restore latency 측정 (가산점) |

---

## 13. 구현 단계 (Phasing)

> "최대 구현"이되 핵심을 먼저 완성해 미완성 리스크를 차단한다.

1. **Phase 1 — 핵심 골격(필수)**: 도메인 모델, Event Store + seq 채번 + 멱등, REST 수집/세션 CRUD, **동기** projection, snapshot+replay 복원 API, WebSocket 기본 송수신. → 4.1 + 4.3 기본 동작.
2. **Phase 2 — 비동기 파이프라인(가산점)**: Outbox → Redis Stream → projection worker, 재시도/DLQ, snapshot 자동화.
3. **Phase 3 — 확장·운영(가산점)**: Redis Pub/Sub 팬아웃 다중 인스턴스, presence TTL, resume, 관측성(메트릭/추적).
4. **Phase 4 — 검증(가산점)**: 장애 주입 테스트, 부하 테스트, 문서(4.4) 마무리.

---

## 14. 트레이드오프 요약 & 미해결 항목

| 항목 | 선택 | 포기한 대안 | 재검토 조건 |
|------|------|-------------|-------------|
| 메시징 | Redis Stream | Kafka | 처리량 한계/멀티 파티션 필요 시 Kafka 전환 |
| 순서 권위 | 서버 수신 seq | 클라 타임스탬프/벡터클록 | 클라 의도 순서가 비즈니스상 중요해지면 재검토 |
| 스냅샷 메시지 | 최근 N개 | 전체 상태 | 임의 과거 전체 목록 요구 시 전체 스냅샷+압축 |
| projection | 비동기 | 동기 | 강한 read-after-write 일관성 요구 시 동기 옵션 |

**미해결(구현 시 결정)**: 메시지 content 영속 위치(event payload만 vs message_view 캐시), 스냅샷 주기 N 기본값 튜닝, presence 하트비트 간격, Idempotency-Key 보존 기간.
