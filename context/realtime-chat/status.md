# realtime-chat 구현 추적

> 과제 요구사항(`requirements/과제요구사항.md`)별 구현/문서 상태를 추적합니다.
> **설계 완료** (이벤트 소싱 + CQRS): [설계서](../../docs/design/2026-05-28-realtime-chat-design.md). 구현은 설계서 §13 Phase 1~4 순서로 진행. 아래 항목은 전부 구현 미착수(⬜).

## 범례

- ✅ 반영됨 — 코드/문서에 완료
- ⬜ 미반영 — 정책/설계만 확정 또는 미착수

## 4.1 필수 구현

| ID | 요구사항 | 상태 | PR/커밋 |
|----|----------|------|---------|
| FR-1 | 실시간 기반 메시지 송수신 | ⬜ | |
| FR-2 | 사용자 join / leave 처리 | ⬜ | |
| FR-3 | 기본 presence 처리(online/offline 또는 세션 참여 상태) | ⬜ | |
| FR-4 | 이벤트 또는 메시지 수집 API | ⬜ | |
| FR-5 | 중복 이벤트 방지/최소화 전략 | ⬜ | |
| FR-6 | 순서 뒤바뀜 일관 처리 기준 정의·반영 | ⬜ | |
| FR-7 | 특정 시점 기준 세션 상태 복원 API/기능 1개 | ⬜ | |

## 4.2 필수 설계/문서

| ID | 요구사항 | 상태 | PR/커밋 |
|----|----------|------|---------|
| FR-8 | ERD 또는 테이블 관계 문서 | ⬜ | |
| FR-9 | 핵심 테이블 DDL 일부 | ⬜ | |
| FR-10 | 핫패스 중심 인덱스 설계 근거 | ⬜ | |
| FR-11 | REST API 스펙(OpenAPI 권장) | ⬜ | |
| FR-12 | 재연결 시 데이터 정합성 유지 방식 | ✅ | [#4](https://github.com/rnqhstmd/realtime-chat/pull/4) |
| FR-13 | 수평 확장 시 세션 분산·상태 저장 전략 | ✅ | [#4](https://github.com/rnqhstmd/realtime-chat/pull/4) |
| FR-14 | 관측 가능성 설계(로그/메트릭/추적) | ✅ | [#4](https://github.com/rnqhstmd/realtime-chat/pull/4) |
| FR-15 | 비동기 처리 구조(Projection/Snapshot/재시도/DLQ/Idempotency) | ✅ | [#3](https://github.com/rnqhstmd/realtime-chat/pull/3) |
| FR-16 | 장애 대응 시나리오(서버다운/DB장애/데이터유실) | ⬜ | |

## 4.3 이벤트 기반 상태 복원

| ID | 요구사항 | 상태 | PR/커밋 |
|----|----------|------|---------|
| FR-17 | 프로젝션/리플레이 전략(전체 리플레이 vs 스냅샷+리플레이) | ⬜ | |
| FR-18 | 복원 로직의 중복·순서 뒤바뀜 처리 | ⬜ | |
| FR-19 | 복원 비용·성능 설계(인덱스/스냅샷 주기/저장 포맷) | ⬜ | |

## 4.4 쿼리 최적화 및 트러블슈팅(문서)

| ID | 요구사항 | 상태 | PR/커밋 |
|----|----------|------|---------|
| FR-20 | 핫패스 주요 쿼리 2~3개(SQL/인덱스/병목 분석) | ⬜ | |
| FR-21 | 비동기 처리 설계(재시도/DLQ/Idempotency) | ✅ | [#3](https://github.com/rnqhstmd/realtime-chat/pull/3) |
| FR-22 | 장애 대응 시나리오(운영 관점, 감지→완화→복구) | ⬜ | |

## 5. 가산점 항목

| ID | 항목 | 상태 | PR/커밋 |
|----|------|------|---------|
| BONUS-1 | Snapshot 생성 자동화 | ✅ | [#3](https://github.com/rnqhstmd/realtime-chat/pull/3) |
| BONUS-2 | Projection 비동기 파이프라인 구성 | ✅ | [#3](https://github.com/rnqhstmd/realtime-chat/pull/3) |
| BONUS-3 | 부하 테스트 및 성능 측정 결과 | ⬜ | |
| BONUS-4 | 운영 대시보드 또는 메트릭 시각화 | ⬜ | |
| BONUS-5 | WebRTC 등 추가 통신 방식 비교/구현 | ⬜ | |
| BONUS-6 | 테스트 전략 고도화(통합/장애주입/재현 스크립트) | ⬜ | |
