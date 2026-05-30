# PRD: Phase 2 — 비동기 파이프라인 (Outbox → Redis Stream → Projection Worker)

> 확정일: 2026-05-29 · 브랜치: feat/realtime-chat-phase2 · 모드: normal
> 확정 결정: (1) 완전 비동기 전환 (2) Outbox Relay 폴링 방식 (3) Snapshot Worker 처리 건수 기준

## 배경

**현재 제품 상태:**
- Phase 1에서 이벤트 수집(append) → 동기 projection → afterCommit broadcast 경로가 완성되어 39개 통합 테스트가 통과 중.
- `outbox` 테이블은 스키마에 존재하나 단 한 번도 기록되지 않음. EventStore의 `append()`는 `event` 테이블에만 INSERT.
- Redis는 docker-compose에 프로비저닝(포트 6379, 무인증)되어 있으나 build.gradle.kts에 의존성 없음. application.yml에 Redis 설정 없음.
- CommandHandler가 한 트랜잭션 안에서 append + 동기 projection을 수행하여, 쓰기 응답 시간이 읽기 모델 갱신 시간과 직접 결합됨.
- SnapshotService는 수동 호출(createSnapshot)만 존재하며 자동 트리거 없음.

**왜 비동기로 전환하는가:**
1. **쓰기 지연 분리**: projection 갱신이 이벤트 수집 트랜잭션을 점유하지 않아 append 응답 시간이 단축됨.
2. **장애 격리**: projection worker 장애가 이벤트 수집 경로를 막지 않음. event store가 진실의 원천이므로 worker 재시작 시 언제든 replay 가능.
3. **재구축 가능성**: 이벤트 소싱의 핵심 가치 — read model을 언제든 drop 후 event store로부터 재구축 가능.
4. **과제 평가 기준 충족**: FR-15(비동기 처리 구조), FR-21(비동기 처리 설계), BONUS-1(Snapshot 자동화), BONUS-2(Projection 비동기 파이프라인)를 구현으로 충족.

**완전 비동기 전환의 트레이드오프 (면접 평가 근거):**

이번 Phase 2는 동기/비동기 병행이 아닌 **완전 비동기 전환**을 선택한다.

| 포기하는 것 | 얻는 것 |
|---|---|
| read-after-write 즉시 일관성 (append 직후 read model 갱신 보장) | 쓰기 경로에서 projection 처리 시간 완전 제거 |
| 통합 테스트의 동기적 단순성 | 이벤트 소싱 모델의 순수한 표현 (쓰기/읽기 경로 완전 분리) |

**감수하는 리스크:**
- 이벤트 수집 직후 read model 조회 시 projection이 아직 반영되지 않았을 수 있음 (projection lag). 라이브 조회 API가 일시적으로 오래된 데이터를 반환할 수 있음.
- 통합 테스트에서 read-after-write를 전제한 단언이 모두 Awaitility 비동기 대기 로직으로 교체되어야 함.
- 운영 환경에서 projection lag이 SLI가 되며, lag 모니터링 없이는 장애와 정상을 구분할 수 없음.

근거: **복원(timeline API)은 event store 직접 replay이므로 비동기 전환과 무관하게 항상 정확하다.** 라이브 조회의 일시적 lag은 수백 ms 수준으로 채팅 도메인에서 허용 가능하다. 반면 동기/비동기 병행은 "두 경로가 동시에 read model을 갱신하는 복잡성"과 "플래그 관리 부담"을 낳는다.

## 목표

- Transactional Outbox 패턴 완성: 이벤트 저장과 브로커 발행의 원자성 보장
- CommandHandler에서 동기 projection을 제거하고 Redis Stream 기반 비동기 projection 파이프라인을 유일한 read model 갱신 경로로 확립
- 재시도/DLQ로 독성 메시지를 격리하고 파이프라인 정체 방지
- Snapshot 자동화로 복원 비용 상한(replay 범위 ≤ N) 유지
- 기존 39개 통합 테스트를 비동기 대기 로직 적용 후 그린 유지

**성공 지표:**
- 이벤트 수집 후 projection이 비동기로 반영되어도 복원(timeline API) 결과가 event store 기준과 항상 일치
- 재시도 한도 초과 이벤트가 DLQ에 격리되어 파이프라인이 정체되지 않음
- N건 이벤트마다 스냅샷이 자동 생성되어 복원 시 replay 범위가 N 이하로 유지됨

## 요구사항

### 기능 요구사항

- **[Must] FR-P2-1**: EventStore.append() 실행 시, 동일 트랜잭션 안에서 `outbox` 테이블에 `(event_id, session_id, seq, published=false)`를 INSERT한다. event INSERT와 outbox INSERT는 원자적으로 성공하거나 실패해야 한다.
- **[Must] FR-P2-2**: CommandHandler에서 동기 projection 호출(projectionUpdater.apply)을 제거한다. CommandHandler의 책임은 append + outbox 기록 + afterCommit broadcast로 한정된다. read model 갱신은 오직 Projection Worker(비동기) 경로로만 일어난다.
- **[Must] FR-P2-3**: Outbox Relay가 미발행(published=false) outbox 레코드를 500ms 주기로 폴링하여, 각 레코드를 Redis Stream `events`에 XADD 발행하고 `published=true`로 업데이트한다. 부분 인덱스 `ix_outbox_unpub (id) WHERE published=FALSE`를 활용.
- **[Must] FR-P2-4**: Redis Stream `events`에 consumer group `proj`를 생성하고, Projection Worker가 XREADGROUP으로 이벤트를 소비하여 read model에 비동기 반영한다. seq-guard(last_applied_seq)로 중복 적용을 방지한다. 적용 성공 시 XACK.
- **[Must] FR-P2-5**: Projection Worker가 이벤트 적용에 실패하면 XACK 없이 pending 상태로 잔류시킨다. XAUTOCLAIM으로 idle 시간 30초 초과 pending 메시지를 재청구하며, 재시도 횟수(attempt)를 추적한다. attempt는 지수 백오프(초기 1초, 배수 2, 상한 32초)로 대기 후 재처리한다.
- **[Must] FR-P2-6**: attempt가 최대 3회를 초과한 메시지를 DLQ 스트림(`events:dlq`)에 XADD하고 XACK로 본 스트림에서 제거한다. DLQ 적재 시 로그 레벨 ERROR로 알람성 로그를 남긴다.
- **[Must] FR-P2-7**: Projection Worker는 gap(스트림에서 수신한 seq > last_applied_seq + 1)을 감지하면, event store에서 누락 seq 범위를 직접 조회(findBySeqRange)하여 순서대로 적용한 뒤 원래 이벤트를 처리한다. 스트림은 알림 역할이며, 순서와 완전성의 권위는 event store.
- **[Should] FR-P2-8**: Projection Worker가 세션당 이벤트를 N건(기본 200, `chat.snapshot.trigger-interval`로 조정 가능) 처리할 때마다 SnapshotService.createSnapshot()을 트리거한다. 스냅샷 생성 실패는 projection 처리를 중단시키지 않는다(로그만 남김).
- **[Could] FR-P2-9**: 디버깅/테스트 편의를 위해 `chat.projection.sync-enabled` 플래그를 `false` 기본값으로 보존한다. `true`일 때 CommandHandler에서 동기 projection을 추가 실행한다. 운영에서는 사용하지 않으며 테스트/디버깅 전용임을 문서화한다. 기본값은 반드시 `false`이며 `true` 설정 시 read model이 동기+비동기 중복 적용을 받는 위험을 문서화한다.

### 비즈니스 규칙

- **[Must] BR-1**: event INSERT와 outbox INSERT는 같은 DB 트랜잭션에서 커밋된다. 둘 중 하나라도 실패하면 전체 롤백.
- **[Must] BR-2**: Projection Worker의 seq-guard — `incoming_seq <= last_applied_seq`이면 XACK만 하고 read model을 수정하지 않는다.
- **[Must] BR-3**: DLQ에 적재된 이벤트는 본 파이프라인을 더 이상 점유하지 않는다. event store가 원천이므로 DLQ 이벤트는 수동/별도 도구로 재처리 가능해야 한다.
- **[Must] BR-4**: Redis 연결이 불가한 상황에서도 이벤트 수집(append + outbox 기록)은 정상 동작한다. outbox 레코드는 Redis 복구 후 Relay가 재발행한다.
- **[Must] BR-5**: 스냅샷 자동 트리거는 `(session_id, up_to_seq)` PK 멱등 저장을 이용한다.
- **[Must] BR-6**: 비동기 전환 후에도 복원(timeline API)은 event store를 직접 replay하므로 projection lag과 무관하게 항상 정확한 결과를 반환한다.
- **[Should] BR-7**: Projection lag(이벤트 occurred_at과 read model 반영 시각의 차이)을 메트릭으로 기록한다. Phase 3 관측성 고도화의 기반.

### 품질 기대

- **[Should] QE-1**: 정상 운영 시 projection lag이 수백 ms 이내로 유지되어 채팅 사용자가 인지하지 못하는 수준이어야 한다.
- **[Should] QE-2**: Redis 일시 장애(수십 초) 후 복구 시, outbox에 적재된 미발행 이벤트가 자동으로 재발행되어 projection 정합성이 회복된다.

## 영향 범위

**영향받는 기존 기능:**
- **EventStore.append()**: outbox INSERT 추가. 기존 event INSERT 동작 보존.
- **CommandHandler.handle()**: 동기 projection 호출 제거. append + outbox + afterCommit broadcast만 담당.
- **SnapshotService**: 기존 수동 createSnapshot() API 유지, Worker 경유 자동 트리거 경로 추가.
- **ProjectionUpdater**: CommandHandler에서 직접 호출되지 않음. Projection Worker에서만 호출됨.

**기존 통합 테스트 영향:**
- 39개 테스트 전부가 read-after-write 구조. 동기 projection 제거 시 단언 실패.
- 수정 방향: AbstractIntegrationTest에 Redis Testcontainer 추가, read model 단언 앞에 Awaitility 대기 로직 삽입(`await().atMost(5, SECONDS).until(...)`).
- 복원(restoreTo/restoreAt) 단언, 스냅샷 생성 단언은 event store/직접 호출이므로 수정 불필요.

**인프라 추가:**
- build.gradle.kts: `spring-boot-starter-data-redis` + `testcontainers:redis` + Awaitility.
- application.yml: `spring.data.redis` 설정(host, port).
- AbstractIntegrationTest: Redis Testcontainer ServiceConnection.

**하위 호환성:**
- outbox 스키마는 V1에 이미 존재, Flyway 추가 마이그레이션 불필요(필요 시 attempt 추적용 별도 검토).
- REST API 스펙/WebSocket broadcast 동작 변화 없음.

## 수용 기준

- **AC-1**: POST /sessions/{id}/events 호출 시, event/outbox 테이블에 각 1건씩 동시 기록. event만 있고 outbox 없는 상태는 없음. → [FR-P2-1, BR-1]
- **AC-2**: CommandHandler.handle() 후 ProjectionUpdater.apply()가 호출되지 않는다. read model 갱신은 Projection Worker만 수행. → [FR-P2-2]
- **AC-3**: Outbox Relay 기동 중 published=false 레코드가 Redis Stream `events`에 발행되고 published=true로 갱신, 미발행 수 0. → [FR-P2-3]
- **AC-4**: Projection Worker가 스트림 소비로 read model 반영. 동일 이벤트 2회 소비해도 1회 적용과 동일(seq-guard 멱등). → [FR-P2-4, BR-2]
- **AC-5**: Worker 처리 중 강제 예외 시 pending 잔류, 30초 후 XAUTOCLAIM 재처리. 3회 초과 시 `events:dlq`에서 확인, 본 파이프라인은 계속 처리. → [FR-P2-5, FR-P2-6, BR-3]
- **AC-6**: Worker 재시작 시 미ACK pending 재처리로 반영 완료. 이미 적용된 seq는 재적용 안 됨. → [FR-P2-4, BR-2]
- **AC-7**: Redis 중지 상태 수집 시 append 성공 + outbox.published=false 잔류. 재기동 시 Relay 발행, projection 따라잡음. → [BR-4, FR-P2-3]
- **AC-8**: gap(seq=N 수신, last_applied_seq=N-2) 시 event store 조회로 누락 seq 순서 적용 후 최종 seq XACK. → [FR-P2-7]
- **AC-9**: `chat.snapshot.trigger-interval=5` 설정 시 5/10/15건 수집마다 snapshot 생성, up_to_seq 일치. → [FR-P2-8, BR-5]
- **AC-10**: 기존 39개 통합 테스트가 Awaitility(최대 5초) 적용 후 모두 통과. 복원/스냅샷 단언은 수정 없이 그린. → [FR-P2-2, BR-6]
- **AC-11**: `chat.projection.sync-enabled=true` 시 CommandHandler 동기 projection 추가 실행, Awaitility 없이 read-after-write 통과. 기본값 false. → [FR-P2-9]

## 제외 범위

- **Phase 3**: Redis Pub/Sub 팬아웃, presence TTL 자동 갱신, resume-by-seq 고도화, 관측성 대시보드
- **Phase 4**: 부하 테스트, 장애 주입 테스트, k6/Gatling
- **이번 미포함**: Outbox Relay LISTEN/NOTIFY 방식, DLQ 자동 재처리 도구, Redis → Kafka 전환
