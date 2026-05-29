# PRD — 실시간 대화 세션 (Phase 1: 핵심 골격)

- 출처 요구사항: `requirements/과제요구사항.md`
- 설계 근거: `docs/design/2026-05-28-realtime-chat-design.md` (이벤트 소싱 + CQRS)
- 범위: 설계서 §13의 **Phase 1 — 핵심 골격(필수)**. Phase 2~4(비동기 파이프라인·확장/운영·검증 가산점)는 후속.

## 배경
면접 과제. 평가 핵심은 "기능 구현 여부"가 아니라 "일관된 모델 + 근거 있는 트레이드오프"다. Phase 1은 핵심을 end-to-end로 먼저 완성하여 미완성 리스크를 차단한다. 진실의 원천은 append-only Event Store(PostgreSQL), 읽기 모델은 동기 projection으로 파생, 시점 복원은 snapshot+부분 replay.

## 요구사항 (Phase 1 구현 대상)

### R1. 세션 라이프사이클 (REST)
- `POST /sessions` 세션 생성
- `POST /sessions/{id}/join` 참여 (PARTICIPANT_JOINED 이벤트)
- `POST /sessions/{id}/end` 종료
- `GET /sessions` 목록 (상태/기간 필터)

### R2. 이벤트/메시지 수집 API
- `POST /sessions/{id}/events` — 메시지 송신/수정/삭제, join/leave, presence 변경을 단일 수렴 command handler로 수집
- `Idempotency-Key` 헤더 필수
- REST와 WebSocket inbound가 동일 command handler로 수렴

### R3. 중복 방지 (2계층 멱등성)
- 계층1(수집): `UNIQUE(session_id, idempotency_key)` — 중복 유입 차단, 충돌 시 기존 이벤트 반환
- 계층2(적용): projection의 `last_applied_seq` seq-guard — 중복 적용 차단 (Phase 1은 동기 적용)

### R4. 순서 처리
- 서버 append 시점에 세션별 단조증가 `seq` 채번 (순서의 단일 권위)
- `UPDATE session SET last_seq = last_seq + 1 ... RETURNING`을 append와 동일 트랜잭션에서 수행
- `UNIQUE(session_id, seq)` 무결성 강제

### R5. 동기 Projection (읽기 모델)
- `participant_view`, `message_view`, `session_view`를 이벤트 append 직후 **동기**로 갱신
- seq-guard 멱등 적용

### R6. 시점 복원 API
- `GET /sessions/{id}/timeline?at=...` (at = ISO-8601 timestamp 또는 seq)
- snapshot(≤atSeq) 로드 + `(snapshot.seq, atSeq]` 구간 event replay + fold
- 반환: 참여자 목록 + 메시지 목록(또는 최근 N) + 각 메시지 상태(SENT/EDITED/DELETED)
- 스냅샷 생성 기능 포함 (`POST /sessions/{id}/snapshots` 또는 동등 경로)

### R7. WebSocket 기본 송수신
- STOMP over WebSocket, 구독 `/topic/session.{id}`
- inbound SEND → REST와 동일 command handler 수렴
- 이벤트 append 후 해당 토픽으로 브로드캐스트 (Phase 1은 단일 인스턴스 인메모리 브로커로 충분, 다중 인스턴스 fan-out은 Phase 3)

### R8. 데이터 모델 (실 DDL)
- 설계서 §3의 DDL을 마이그레이션으로 구현: `session`, `event`(+제약/인덱스), `outbox`(테이블만 생성, relay는 Phase 2), `participant_view`, `message_view`, `session_view`, `snapshot`

## 수용 기준 (Acceptance Criteria)
1. 세션 생성→참여→메시지 송신→조회→종료가 REST로 end-to-end 동작한다.
2. 동일 `Idempotency-Key`로 같은 이벤트를 2회 전송해도 이벤트가 1건만 저장되고 동일 결과를 반환한다 (계층1 검증).
3. 이벤트는 서버 채번 seq로 정렬되며, `(session_id, seq)`/`(session_id, idempotency_key)` 유니크 제약이 동작한다.
4. 메시지 송신/수정/삭제 후 `message_view`가 동기 갱신되고, 최근 N개 조회가 seq 역순으로 반환된다.
5. `GET /sessions/{id}/timeline?at=T`가 시점 T의 참여자 목록·메시지 목록·각 메시지 상태를 정확히 복원한다 (snapshot+replay 경로).
6. 중복/순서 뒤바뀜 시나리오에서 복원 결과가 일관된다 (fold가 정제된 로그를 접는 것으로 충분; 중복은 append 제약, 순서는 seq 권위로 이미 정제).
7. WebSocket 구독 클라이언트가 같은 세션의 새 메시지를 실시간 수신한다.
8. `./gradlew build`가 성공하고, 핵심 단위 테스트(fold 순수 함수, seq-guard 분기, 멱등성)와 최소 통합 테스트(append→projection→restore, Testcontainers PostgreSQL)가 통과한다.

## 비범위 (Phase 1 제외 — 후속 Phase)
- Outbox relay / Redis Stream / 비동기 projection worker / 재시도 / DLQ (Phase 2)
- Redis Pub/Sub 다중 인스턴스 fan-out, presence TTL 고도화, resume 고도화, 관측성 메트릭/추적 (Phase 3)
- 장애 주입 테스트, 부하 테스트 (Phase 4)
- 단, **presence 기본 처리(online/offline 상태)**와 **DDL의 outbox 테이블 생성**은 Phase 1에 포함(후속 연결 용이성).
