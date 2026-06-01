# 자기점검 결과 (phase-implement)

> qa-manager 자동 리뷰 (자기점검 1회 패스). 2026-05-31.
> 컴파일 green 상태에서 로직/스펙/불변식 검토.

## CERTAIN (Critical)
- **0건.** (qa-manager가 PresenceTracker UUID 파싱을 Critical 후보로 제기 → UUID에 `:`가 없어 `indexOf(':')` 분할이 정확함을 확인하고 자진 철회.)

## 검증 통과 불변식 (근거 확인됨)
1. 이중 전달 금지: RedisPubSub=publish만, Simp=직접만, `@ConditionalOnProperty`로 한 시점 한 빈만 등록. ✓
2. BR-3 SREM 원자성: claimExpired(SREM==1) 성공 시에만 OFFLINE 발행, read model 가드 없음. ✓
3. heartbeat 비이벤트화: PresenceController는 touch만, CommandHandler/append/broadcast 미경유. ✓
4. MDC 누수: CommandHandler/ProjectionApplier 모두 try-finally에서 put한 키만 remove. ✓
5. resume 검증/hasMore: afterSeq null→400, LimitSupport(1~500), 세션 404, findBySeqRange(afterSeq, afterSeq+limit+1) 반개구간으로 hasMore 판정, ORDER BY seq. ✓ (AC-6~11)
6. 회귀: QueryController→LimitSupport 위임 동작 동일, ProjectionLagMetrics 생성자 빈 주입 호환, ProjectionApplier seq-guard return이 try 내부라 finally 정상. ✓
7. 카운터: recordStreamProcessed=XACK 성공 1건당 1회, recordOutboxRelayed=발행 건수. ✓

## SELF_CHECK_FINDINGS (Warning/Info — phase-review 이월)
- [Warning] PresenceSweeper.java:56-57 - isAlive→claimExpired 사이 TOCTOU 간격. 다른 인스턴스가 먼저 SREM하면 claimExpired==0이라 중복 발행 없음(무해). 단 isAlive true 직후 만료 시 다음 폴링(최대 5s)까지 지연 — AC-4(95s) 허용 범위. 단일 인스턴스라면 isAlive 없이 claimExpired만으로도 동일 보장.
- [Warning] PresenceSweeper.java:77 - 멱등키 `presence-offline-{sid}-{pid}-{epochMilli}`. SREM 원자 선점으로 중복 발행이 이미 막히므로 epochMilli는 불필요한 복잡도일 수 있음(단, 재join 후 재만료를 새 이벤트로 허용하려면 단조요소 필요 — QUESTION 2 참조).
- [Info] ResumeEventController.java:64 - 세션 존재 확인이 afterSeq/limit 검증 이후. BR-4는 순서 미규정, 기능적 정확(AC 충족).
- [Info] ProjectionLagMetrics 카운터(dlq/stream/outbox)에 tag 없음. FR-P3-4는 카운터 존재만 요구 — 기능 충족. 대시보드 필터링용 태그는 선택.

## SELF_CHECK_QUESTIONS (확인 필요 — phase-review 이월)
1. PresenceTracker member 파싱: `{sid}:{pid}`를 첫 `:`로 분할. UUID에 `:` 없어 현재 정확. UUID 고정 전제가 의도된 계약인가(향후 ID 형식 변경 시 깨짐)?
2. PresenceSweeper 멱등키 epochMilli: 재join 후 재만료 시 다른 키 → 새 OFFLINE 정상 수집. 세션 생애 OFFLINE 1회만이어야 하는가, 아니면 사이클마다 허용인가(설계 의도=사이클마다 허용)?
3. ResumeEventController hasMore: seq gap 환경에서 limit=500 요청 시 실제 반환 <500이고 hasMore=false 가능. gap 시 적은 이벤트 수신 허용 설계인가?
