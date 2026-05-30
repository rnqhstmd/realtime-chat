# 자기점검 결과 (phase-implement) — Phase 1

- 일시: 2026-05-29
- 대상: 62파일/4123줄 (greenfield 전체 구현)
- 빌드: `./gradlew build` BUILD SUCCESSFUL, 32 tests 통과(0 fail) — 자기점검 수정 후.

## 발견 → 조치 (모두 해소됨)

### Critical (수정 완료)
- [Critical] `ProjectionUpdater`/`ParticipantViewDao.applyLeft` — seq-guard no-op이어도 `decrementParticipants` 무조건 호출 → participant_count 과차감 + 재가입 재증가 비대칭.
  - 조치: applyJoined/applyLeft를 `SELECT ... FOR UPDATE` 기반 **transition-aware**(boolean 반환)로 교체. 실제 전이(신규/LEFT→JOINED, JOINED→LEFT)일 때만 카운터 증감. xmax 트릭 제거. 회귀 테스트 `regression_participantCountTransitions` 추가.

### Warning (수정 완료)
- [Warning] `SnapshotService` — `maxSeqAtOrBefore(Instant.now())` 앱/DB 시계차로 최신 이벤트 누락 가능 → `EventStore.maxSeq(sessionId)`(시각 무관 MAX) 신설·사용. 회귀 `regression_snapshotCoversMaxSeqRegardlessOfClock`.
- [Warning] `RestoreService.restoreTo/restoreAt` — `@Transactional(readOnly=true)` 부재(phantom read) → 부여.
- [Warning] `SessionViewDao.touchActivity` — at=null 시 last_activity_at NULL 덮어쓰기 → SQL `CAST(:at AS timestamptz) IS NOT NULL` 가드.

### Info (정정 완료)
- [Info] `EventStore` 멱등 경로 주석 — "seq 소비 없음"이 (a)선조회 경로에만 해당. 동시 race(c)에서는 채번 seq가 소비되어 gap 가능 → 주석 정정.
- [Info] `Fold.applyPresenceChanged` 주석/구현 불일치 → 정정.

## QUESTION → 결정으로 해소
- Q1 seq gap 허용 여부 → **결정: 허용**. Phase 1은 seq를 "세션 내 단조증가 정렬 권위"로만 보장하며 연속성(gap 없음)은 보장하지 않음. 동기 projection은 append된 이벤트를 직접 적용하므로 gap이 문제되지 않음(gap-fill은 Phase 2 비동기 worker 관심사).
- Q2 재가입 시 `left_seq` 리셋 → **결정: 의도된 동작**. 뷰는 현재 상태만 반영, 이력은 event store가 보존. Fold의 재가입 처리와 일관(left_seq=NULL).

## AC 충족 (qa-manager 확인)
AC1~AC8 전부 충족. (E2E·멱등·seq/유니크·projection 전이·시점복원·복원 일관성·WebSocket 수신·build+테스트)

## 잔존 이슈
없음. Critical 0건 잔존. phase-review로 이월할 미해결 항목 없음.
