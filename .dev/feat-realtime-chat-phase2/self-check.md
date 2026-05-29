# 자기점검 결과 (phase-implement, qa-manager 1회 패스)

> 일시: 2026-05-29 · 전체 54개 테스트 통과 상태 · CERTAIN Critical 0건(자동 수정 불필요)

## SELF_CHECK_FINDINGS (Warning/Info — phase-review 이월)

- [Warning] `ProjectionWorker` reclaimPending: 매 루프 `pending(Range.unbounded(), readCount)`로 전체 pending 풀스캔 후 idle 미달 항목 skip → 불필요한 Redis 왕복. 단일 consumer 단일 루프(Q1) 전제라 기능 문제 없음. (성능 개선 여지)
- [Warning] `ProjectionApplier`: `selectForUpdateOrInit`(last_applied_seq) + `snapshotCounter`(events_since_snapshot)를 같은 잠긴 행에 대해 별도 SELECT 2회 → 왕복 1회 추가. `SELECT last_applied_seq, events_since_snapshot ... FOR UPDATE` 통합 + record 반환으로 개선 가능.
- [Warning] `OutboxRelay`: MAXLEN trim을 XADD와 분리(배치 후 `trim(stream, maxlen, true)` 1회). 설계 §8은 `XADD ~ MAXLEN`. 근사 trim이라 결과 동등하나 trim 실패 시 일시적으로 maxlen 초과 가능(다음 배치 보완).
- [Info] `AbstractIntegrationTest:85` flyway 검증 에러 메시지가 "V1"로 고정 — `projection_offset`은 V2 생성. "V1 or V2"로 수정 권장.
- [Info] `ProjectionApplier` leftover: `snapshotEnabled=false`(trigger-interval<=0)일 때 events_since_snapshot에 running 전체 누적 → 이후 interval>0 전환 시 즉시 트리거 가능. POC 무해.

## SELF_CHECK_QUESTIONS (phase-review 사용자 확인 이월)

- [Q1] `ProjectionWorker` graceful shutdown: XREADGROUP BLOCK 중 stop()→interrupt 시 Lettuce 인터럽트 복원 미보장, join(10s) 내 미종료 가능성. 데몬 스레드라 JVM 종료엔 무해하나 테스트 컨텍스트 재사용 시 우려. 현재 readBlockMs(테스트 200ms)로 종료 지연 최대 ~readBlockMs+1s. → POC 수용 여부 확인 필요.
- [Q2] `AsyncPipelineIntegrationTest` AC-8: 실제 stream gap(seq 2 누락)을 만들지 않고 정상 3건 append 후 결과만 검증 → gap-fill 경로(`incoming.seq > last+1` → findBySeqRange)를 직접 검증하지 않음. (단, full-suite 실행 시 worker 지연으로 gap-fill은 실제 실행됨 — AC-9 [13] 현상이 그 증거.) AC-8/FR-P2-7 결정적 검증을 위해 seq 2 누락 주입 테스트 추가 여부 확인 필요.
- [Q3] ~~EventStreamCodec occurredAt null NPE~~ → **해소**: V1 스키마 `occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()` 확인. non-null 보장이라 위험 없음.
