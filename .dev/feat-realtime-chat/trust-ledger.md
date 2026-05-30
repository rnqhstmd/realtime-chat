# Trust Ledger — Phase 1 (실시간 대화 세션)

security-auditor 통합 감사 결과 + 조치. 감사 시점: 2026-05-29. **CRITICAL 0건.**

## 조치 완료 (코드 수정)
- [RISK/HIGH] limit 상한 미검증 → DoS. **조치**: timeline/messages `limit` 1~500 강제(초과 400). 테스트 추가.
- [GAP/HIGH] DataIntegrityViolation/IllegalState → 500. **조치**: ApiExceptionHandler에 409/500 명시 매핑(원문·스택 비노출).
- [GAP/MEDIUM] 없는 세션 snapshot → 오해성 202. **조치**: SnapshotService @Transactional + SessionDao 검증 → 404. 테스트 추가.
- [GAP/MEDIUM] SnapshotService 트랜잭션 미선언. **조치**: @Transactional 부여.
- [GAP/LOW] GET /sessions status 미검증. **조치**: ACTIVE/ENDED 외 400. 테스트 추가.
- [ASSUMPTION/MEDIUM] idempotency_key 길이 무제한. **조치**: 200자 제한(초과 400).
- [ASSUMPTION/MEDIUM] presence 자유문자열. **조치**: ONLINE/OFFLINE 검증(외 값 400), 대문자 정규화. 테스트 추가.

## 문서화로 해소 (Phase 1 비범위 — 평가자 인지용, README "Phase 1 보안 가정 및 비범위")
- [RISK/HIGH] actorId/senderId 클라이언트 자기 선언(인증 없음) → Phase 2+ Principal 대체. 코드 주석 없음→README 명시.
- [RISK/MEDIUM] WebSocket origin 와일드카드(`*`) + 구독 권한 미검증 → CSWSH. WebSocketConfig 주석 + README 명시. Phase 2+ 제한.
- [RISK/MEDIUM] 로컬 DB 크리덴셜 평문 → application.yml 주석(운영은 env/Vault) + README.
- [ASSUMPTION/LOW] at 미래 시각 → "현재 시점" 처리. README 명시(정상 동작).

## 교차 검증 (정합 확인)
- PRD R1~R8 / 설계 §3·§4·§6·§11 ↔ 구현 일치 확인.
- [Phase 2 의도] outbox 동일TX 미구현 — 테이블만 선행 생성(PRD 비범위 명시).
- [Phase 3 의도] §8 구조화 로그/메트릭/추적 미구현 — 관측성 후속(PRD 비범위 명시).

## 잔여 위험 (수용)
- 인증/인가 부재로 인한 신원 위조·구독 권한은 Phase 1 비범위로 수용. 운영 전 Phase 2 인증 도입 필수(README 명시).
