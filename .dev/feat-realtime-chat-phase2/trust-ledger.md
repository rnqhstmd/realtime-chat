# Trust Ledger — Phase 2 비동기 파이프라인

> review 1회차 · 2026-05-29 · 전체 54 테스트 통과 기준

## 통합 감사 (security-auditor, review)

### CRITICAL
- [RISK/CRITICAL] `EventStreamCodec.toFields:42` — `occurredAt().toEpochMilli()` null 미방어.
  - 근거: OutboxDao.insert가 occurredAt null 허용(방어 코드 존재). 단, **V1 스키마 `event.occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()` 확인 → 정상 경로에서 null 불가**. 실질 위험은 외부 stream 임의 XADD/누락 필드 시나리오.
  - 권고: toFields/toStoredEvent에 null·누락 필드 방어 추가(강건성). → **수정 채택**.

### HIGH
- [RISK/HIGH] `ProjectionWorker.moveToDlq:261` — DLQ XADD 실패 시에도 XACK 미보장 → 영구 DLQ 실패 시 본 스트림에서 제거 안 되어 무한 재청구 가능.
  - 권고: DLQ XADD를 try-catch로 감싸고, 실패해도 ERROR 로그 후 XACK(event store 권위라 복원 가능)하여 파이프라인 정체 방지. → **수정 채택**.
- [POLICY/HIGH] `application.yml:8` — DB 비밀번호 평문(`password: realtimechat`). Phase 1부터 존재(주석에 로컬 전용 명시). 운영 오버라이드 누락 시 취약 기본값 사용.
  - 권고: `${DB_PASSWORD:...}` 환경변수 참조. → 사용자 확인(Phase 2 범위 외 pre-existing).
- [GAP/HIGH] AC-8 gap-fill 경로 직접 미검증(테스트가 정상 경로만). → 사용자 확인.
- [GAP/HIGH] AC-7(BR-4) Redis stop/start 통합 미검증(공유 컨테이너 제약, javadoc 명시). → POC 수용, 문서화됨.
- [불일치] FR-P2-5 설계는 XAUTOCLAIM, 구현은 XPENDING+XCLAIM(Spring Data Redis 3.3.x 미지원). AC-5 DLQ 경로는 통과. 등가성 단위검증 부재. → 문서화된 의도적 deviation, 수용.

### MEDIUM (POC 수용)
- [RISK/MEDIUM] triggerInterval=1 설정 시 매 이벤트 스냅샷(전체 replay) → 성능. 권고: 하한 가드/경고. (테스트 5라 무영향)
- [ASSUMPTION/MEDIUM] selectForUpdateOrInit + snapshotCounter 별도 SELECT(같은 잠긴 행, 안전하나 호출순서 의존). 권고: 단일 SELECT FOR UPDATE 통합.
- [ASSUMPTION/MEDIUM] consumer 이름 `${HOSTNAME:worker-1}` — HOSTNAME 미주입 시 동일 이름. 단일 인스턴스 POC 전제, 문서화 권고.
- [GAP/MEDIUM] ProjectionLagMetrics maxLag 리셋 없음(단조 증가). Phase 3 Micrometer 교체 시 해소. javadoc 명시 권고.
- [GAP/MEDIUM] OutboxRelay 다중 인스턴스 시 중복 XADD(SKIP LOCKED 미적용) — 설계 명시 의도, seq-guard 흡수. 운영 전환 시 SKIP LOCKED 필요.
- [ASSUMPTION/MEDIUM] Redis 무인증 — 외부 임의 XADD 가능. POC 전제, 운영 시 ACL/네트워크 격리 필수.

## QA 종합 리뷰 (qa-manager)
- [Warning] `ProjectionApplier` leftover: `snapshotEnabled=false`일 때 events_since_snapshot에 running 전체 누적 → 추후 true 전환 시 첫 스냅샷 트리거 지점 오류(latent 스펙위반). 권고: false면 0 저장. → **수정 채택**.
- [Info] `AbstractIntegrationTest` flyway 검증 에러 메시지 "V1" 고정(projection_offset은 V2). → **수정 채택**.
- [Info] ProjectionLagMetrics maxLag 단조 증가(상동).
- 자기점검 기보고 항목(reclaimPending 풀스캔, snapshotCounter 왕복, MAXLEN trim 분리)은 재확인됨 — 성능/스타일 여지, POC 기능 무해.

### 미답변/이월 QA QUESTION
- [Q1] ProjectionWorker graceful shutdown(BLOCK 중 interrupt, join 10s 내 미종료 가능). 데몬 스레드라 JVM 종료엔 무해. → 사용자 확인.
- [Q2] AC-8 gap-fill 직접 검증 테스트 추가 여부. → 사용자 확인.
- [Q3] occurredAt null NPE → **해소**(occurred_at NOT NULL 확인).
