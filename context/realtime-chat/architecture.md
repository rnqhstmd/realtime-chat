# realtime-chat 아키텍처

> 전체 구조 요약과 주제별 상세 문서 링크를 관리합니다.
> 전제 스택: **Spring Boot + PostgreSQL**, 실시간 전송 **WebSocket/STOMP**, 비동기 파이프라인은 메시지 브로커(Kafka 또는 Redis Stream) 가정. 규모는 과제/POC 수준.

## 시스템 구조

(확정 시 상세화)

핵심 흐름은 **이벤트 수집 → 저장(이벤트 소싱) → 프로젝션/스냅샷 → 시점 복원** 이다.

```
[Client] --WebSocket/STOMP--> [Realtime Gateway]
   |                                |
   | REST(이벤트 수집/조회/복원)     | 이벤트 발행
   v                                v
[REST API (Spring Boot)] ----> [Event Store (PostgreSQL)]  <-- append-only events
                                     |
                                     | (비동기) 브로커
                                     v
                              [Projection Worker] --> [Read Model / Snapshot 테이블]
```

- **Realtime Gateway**: WebSocket 연결, presence, 송수신 fan-out. 수평 확장 시 세션-인스턴스 분산 필요.
- **REST API**: 세션 생성/참여/종료, 이벤트 수집, 목록·타임라인 조회.
- **Event Store**: append-only 이벤트 저장. seq + idempotency key로 중복·순서 관리.
- **Projection Worker**: 이벤트를 비동기로 읽어 읽기 모델/스냅샷 갱신. 재시도·DLQ·Idempotency 적용.

## 핵심 설계 결정 (요구사항 4.2/4.3/4.4 대응)

| 주제 | 설계 포인트 |
|------|-------------|
| 중복 이벤트 방지 | Idempotency Key + (session_id, seq) 유니크 제약 |
| 순서 뒤바뀜 | 서버 채번 seq 기준 정렬 / 복원 시 seq 순 적용 |
| 시점 복원 | 스냅샷 + 이후 이벤트 리플레이 (전체 리플레이 대비 비용 절감) |
| 재연결 정합성 | 마지막 수신 seq 이후 이벤트 resume/replay |
| 수평 확장 | 세션 단위 sticky 라우팅 또는 pub/sub fan-out |
| 관측성 | 구조화 로그 + 메트릭(처리량/지연/리플레이 비용) + 분산 추적 |
| 비동기 처리 | Projection/Snapshot 분리, 지수 백오프 재시도, DLQ |
| 장애 대응 | 서버 다운 / DB 장애 / 데이터 정합성 — 감지→완화→복구 |

## 주제 문서

전체 설계와 모든 결정의 이유는 통합 설계서에 정리되어 있다:

- **[이벤트 소싱 기반 설계서](../../docs/design/2026-05-28-realtime-chat-design.md)** — ERD/DDL·인덱스 근거, REST/WebSocket API, 이벤트 복원(snapshot+replay) 전략, 멱등성·순서 처리, 비동기 파이프라인, 수평확장, 관측성, 장애 대응 3종, 쿼리 최적화 핫패스, 테스트 전략, 구현 단계까지 결정별 이유 포함

| 주제 | 위치 |
|------|------|
| ERD / DDL / 인덱스 근거 | 설계서 §3, §10 |
| REST / WebSocket API 스펙 | 설계서 §11 |
| 이벤트 복원 전략 | 설계서 §4.3 |
| 중복·순서 처리 | 설계서 §4.1, §4.2 |
| 비동기 파이프라인 | 설계서 §5 |
| 장애 대응 시나리오 | 설계서 §9 |
| 쿼리 최적화 | 설계서 §10 |
