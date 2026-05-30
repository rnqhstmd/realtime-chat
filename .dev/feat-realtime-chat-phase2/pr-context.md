# PR 맥락 — Phase 2 비동기 파이프라인

## 비즈니스 맥락

**배경:** Phase 1에서 이벤트 수집(append) → 동기 projection → afterCommit broadcast 경로가 완성됐으나, `outbox` 테이블은 미사용이고 CommandHandler가 한 트랜잭션에서 append + 동기 projection을 수행해 쓰기 응답이 읽기 모델 갱신과 결합돼 있었다. Redis는 docker-compose에 프로비저닝됐으나 미연결.

**목표(완전 비동기 전환):** Transactional Outbox 패턴을 완성하고, CommandHandler에서 동기 projection을 제거하여 **Redis Stream 기반 비동기 projection 파이프라인을 read model 갱신의 유일 경로**로 확립한다. 재시도/DLQ로 독성 메시지를 격리하고, Snapshot을 자동화해 복원 비용 상한(replay ≤ N)을 유지한다. 기존 39개 통합 테스트는 Awaitility 비동기 대기로 전환해 그린 유지.

**핵심 불변식:** event store가 진실의 원천. Redis Stream은 "알림"이며 순서·완전성의 권위는 event store(gap-fill은 findBySeqRange로 보강). 복원(timeline API)은 projection lag과 무관하게 항상 정확(BR-6).

**요구사항 요약:** Must 9(FR-P2-1~7 + BR-1~6), Should 2(스냅샷 자동화/lag 메트릭), Could 1(sync-enabled 디버그 플래그). 수용 기준 AC-1~11.

**주요 설계 결정:**
- 소비 = 수동 XREADGROUP 단일 루프(SmartLifecycle, 2-track race 차단). Spring Data Redis 3.3.x에 XAUTOCLAIM 직접 지원이 없어 재청구는 XPENDING+XCLAIM으로 구현.
- 카운터 멱등 권위 = per-row 전이 boolean(projection_offset은 gap 감지·스냅샷 카운팅 전용).
- 스냅샷 up_to_seq = **Worker 적용 seq 기준**(createSnapshot(upToSeq) 오버로드), N의 배수 시점에 정확히 트리거.
- Outbox Relay = 폴링(XADD-then-markPublished, 유실<중복; seq-guard가 중복 흡수). events 스트림 MAXLEN trim.
- POC 단일 인스턴스 전제(consumer 호스트명 유일). Redis Testcontainer + @ServiceConnection.

## 구현 중 해결 이슈
- `org.testcontainers:redis`(미존재) → `com.redis:testcontainers-redis:2.2.2`로 교체.
- RedisStreamInitializer BUSYGROUP 미처리(cause 체인 미검사) → 다중 컨텍스트 로드 실패 → cause 순회 판정으로 수정.
- 스냅샷이 gap-fill 일괄 적용 시 N배수를 건너뛰던 문제 → 적용 범위 내 정확 트리거로 수정(AC-9 결정적 통과).

## Audit Summary
- 총 13건 (CRITICAL: 1, HIGH: 5, MEDIUM: 6) — review 1회차
- [CRITICAL] EventStreamCodec occurredAt NPE → occurred_at NOT NULL 확인 + toFields null 방어로 해소
- [HIGH] ProjectionWorker.moveToDlq: DLQ XADD 실패 시 XACK 미보장(무한 재청구) → finally XACK로 정체 방지(수정 완료)
- [HIGH/GAP] AC-8 gap-fill 직접 미검증 → GapFillIntegrationTest 신규 추가로 결정적 검증(해소)
- [HIGH/POLICY] application.yml DB 비밀번호 평문(Phase 1부터, 로컬 전용) → 현 유지(별도 보안 작업으로 분리, 사용자 결정)
- [HIGH/GAP] AC-7 Redis stop/start 통합 미검증(공유 컨테이너 제약) → BR-4 핵심 불변식만 검증, 문서화(POC 수용)
- 잠재버그: sync-enabled=false 시 스냅샷 카운터 누적 → 0 저장으로 수정(해소)
- MEDIUM(POC 수용): triggerInterval=1 성능, consumer 이름 유일성, Redis 무인증, lag 메트릭 리셋 부재 등 — Phase 3/운영 전환 시 처리

상세는 `.dev/feat-realtime-chat-phase2/trust-ledger.md` 참조.
