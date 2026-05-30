package com.realtimechat.projection.async;

import com.realtimechat.event.EventStore;
import com.realtimechat.event.StoredEvent;
import com.realtimechat.projection.ProjectionUpdater;
import com.realtimechat.restore.SnapshotService;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 비동기 파이프라인의 projection 적용 TX 경계(설계서 §10, §11; critic ①, FR-P2-7, BR-2, FR-P2-8, BR-5).
 *
 * <p>OutboxRelay가 이벤트를 소비할 때마다 {@link #apply(StoredEvent)}를 호출한다. 이 메서드가
 * Phase 2의 <b>새 트랜잭션 경계</b>다(Phase 1의 동기 projection은 운영 시 비활성). 한 번의 apply는
 * 한 트랜잭션에서:
 * <ol>
 *   <li>{@code projection_offset} 행을 FOR UPDATE로 잠가 세션을 직렬화하고(§10),</li>
 *   <li>이미 본 seq면 no-op 정상 종료(BR-2 seq-guard 1차),</li>
 *   <li>gap이 있으면 event store에서 누락 범위를 순서대로 재조회·적용하고(FR-P2-7),</li>
 *   <li>적용 범위 내에서 스냅샷 카운터가 주기 배수에 도달하는 각 시점의 정확한 seq를 수집하고,</li>
 *   <li>offset(last_applied_seq·잔여 카운터)을 절대값으로 갱신한 뒤 수집한 각 seq의 스냅샷 생성을
 *       afterCommit 별도 TX로 위임한다(FR-P2-8). gap-fill 다건 일괄 적용에서도 각 N배수 seq마다
 *       스냅샷이 정확히 떨어진다(AC-9).</li>
 * </ol>
 *
 * <p><b>멱등 권위는 ProjectionUpdater(critic ①)</b>: 카운터/상태 전이의 멱등성은 각 view DAO의
 * per-row seq-guard와 전이 boolean이 보장한다. ProjectionApplier는 카운터를 직접 만지지 않고
 * {@link ProjectionUpdater#apply}만 통과시킨다.
 */
@Service
public class ProjectionApplier {

    private static final Logger log = LoggerFactory.getLogger(ProjectionApplier.class);

    private final ProjectionOffsetDao offsetDao;
    private final ProjectionUpdater projectionUpdater;
    private final EventStore eventStore;
    private final SnapshotService snapshotService;
    private final ProjectionLagMetrics lagMetrics;

    /**
     * 스냅샷 생성 전용 단일 스레드 executor(SnapshotExecutorConfig). afterCommit 콜백에서 스냅샷
     * 생성을 이 executor로 위임하여 ProjectionWorker 소비 루프가 replay(restoreTo) 동안 블로킹되지
     * 않게 한다. 단일 스레드라 직렬 처리되며, 스냅샷 PK 멱등(BR-5)으로 중복도 무해하다.
     */
    private final ExecutorService snapshotExecutor;

    /** events_since_snapshot이 이 값에 도달하면 스냅샷을 트리거한다(§11, FR-P2-8, Q3). */
    private final long triggerInterval;

    public ProjectionApplier(ProjectionOffsetDao offsetDao,
                             ProjectionUpdater projectionUpdater,
                             EventStore eventStore,
                             SnapshotService snapshotService,
                             ProjectionLagMetrics lagMetrics,
                             ExecutorService snapshotExecutor,
                             @Value("${chat.snapshot.trigger-interval:200}") long triggerInterval) {
        this.offsetDao = offsetDao;
        this.projectionUpdater = projectionUpdater;
        this.eventStore = eventStore;
        this.snapshotService = snapshotService;
        this.lagMetrics = lagMetrics;
        this.snapshotExecutor = snapshotExecutor;
        this.triggerInterval = triggerInterval;
    }

    /**
     * 단일 incoming 이벤트를 비동기 projection에 적용한다(설계서 §10, §11).
     *
     * @param incoming OutboxRelay가 소비한 이벤트(seq는 적용 대상 상한)
     */
    @Transactional
    public void apply(StoredEvent incoming) {
        UUID sessionId = incoming.sessionId();

        // (1) 단일 upsert로 행 잠금 획득과 현재 오프셋 조회를 원자적으로 수행(락 공백·왕복 제거).
        ProjectionOffsetDao.Offset off = offsetDao.lockOrInit(sessionId);
        long lastApplied = off.lastAppliedSeq();

        // (2) seq-guard 1차(BR-2): 이미 본 seq면 read model 미수정 no-op 정상 종료(호출자가 XACK).
        //     lockOrInit이 행 잠금을 보유하므로 TX 종료 시 잠금이 해제된다(정상).
        if (incoming.seq() <= lastApplied) {
            return;
        }

        // (3) 적용 전 스냅샷 카운터(마지막 스냅샷 이후 잔여 건수). 항상 [0, triggerInterval) 범위.
        long since0 = off.eventsSinceSnapshot();

        // (4) 적용 범위 결정.
        List<StoredEvent> events;
        if (incoming.seq() == lastApplied + 1) {
            // 정상 진행: 재조회 없이 incoming 1건만 적용.
            events = List.of(incoming);
        } else {
            // gap(incoming.seq > lastApplied + 1): event store가 순서·완전성 권위(FR-P2-7).
            // findBySeqRange는 (fromExclusive, toInclusive] 반개구간 + seq 오름차순이므로
            // (lastApplied, incoming.seq] 범위 전체가 누락분 포함 순서대로 조회된다.
            events = eventStore.findBySeqRange(sessionId, lastApplied, incoming.seq());
        }

        // (5) triggerInterval 가드: 0/음수면 스냅샷 비활성(모듈로 회피).
        boolean snapshotEnabled = triggerInterval > 0;

        // (6) 이벤트를 순서대로 적용하며 running 카운터로 N배수 도달 시점의 정확한 seq를 수집한다.
        //     gap-fill로 다건을 일괄 적용해도 각 N배수 seq마다 스냅샷이 트리거되도록 한다(AC-9).
        long running = since0;
        List<Long> snapshotSeqs = new ArrayList<>();
        for (StoredEvent event : events) {
            projectionUpdater.apply(event);
            // lag은 live(incoming) 이벤트에 대해서만 기록한다. gap-fill 보강분은 과거 이벤트라
            // lag이 크게 잡혀 lastLag/maxLag을 왜곡하므로 제외한다(정상 경로는 events가 incoming
            // 1건이라 동일 동작, gap-fill은 마지막 incoming만 기록).
            if (event.seq() == incoming.seq()) {
                lagMetrics.record(event.occurredAt(), Instant.now());
            }
            running++;
            if (snapshotEnabled && running % triggerInterval == 0) {
                snapshotSeqs.add(event.seq());
            }
        }

        // (7) offset 갱신(절대값): last_applied_seq=incoming.seq, events_since_snapshot=마지막 스냅샷 이후 잔여.
        long leftover = snapshotEnabled ? (running % triggerInterval) : 0L;
        offsetDao.updateOffset(sessionId, incoming.seq(), leftover);

        // (8) 스냅샷 트리거(§11, FR-P2-8, Q3): 수집한 각 N배수 seq에 대해 afterCommit으로 createSnapshot 위임.
        //
        // Q3 일관성 근거: 카운터는 (7)에서 잔여값으로 절대 갱신되므로 스냅샷이 실패해도 다음 N건이 쌓이면
        // 재트리거되며, createSnapshot은 같은 up_to_seq 재생성이 무해하므로(멱등 BR-5) 중복도 안전하다.
        // 스냅샷 일시 누락은 복원 비용 상한의 일시 위반일 뿐 정확성에는 무손상(BR-6).
        for (long seq : snapshotSeqs) {
            final long upToSeq = seq;
            triggerSnapshotAfterCommit(sessionId, upToSeq);
        }
    }

    /**
     * 커밋 직후 스냅샷 생성을 등록한다(설계서 §11 afterCommit). 트랜잭션 동기화가 활성일 때만
     * afterCommit 콜백을 등록하고, 비활성(동기화 없는 호출 경로)이면 즉시 위임한다. 어느 경로든
     * 스냅샷 생성은 {@code snapshotExecutor}로 submit되어 worker 스레드를 즉시 반환시킨다(gemini ⓒ).
     * afterCommit 등록은 유지되므로 "apply TX 커밋 성공 후에만" 스냅샷이 시작되며 롤백 시에는
     * submit 자체가 일어나지 않는다.
     */
    private void triggerSnapshotAfterCommit(UUID sessionId, long upToSeq) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    submitSnapshot(sessionId, upToSeq);
                }
            });
        } else {
            submitSnapshot(sessionId, upToSeq);
        }
    }

    /**
     * 스냅샷 생성을 전용 executor에 submit한다(gemini ⓒ). worker 소비 루프가 replay(restoreTo) 동안
     * 블로킹되지 않도록 createSnapshot을 inline 실행하지 않고 즉시 위임한다.
     *
     * <p>{@link SnapshotService#createSnapshot}는 Spring 빈(프록시)이라 executor 스레드에서 호출해도
     * 자체 {@code @Transactional}이 새 TX로 정상 적용된다. best-effort로 try-catch로 감싸 실패해도
     * projection 흐름을 막지 않고 WARN만 남긴다(FR-P2-8: 스냅샷 실패는 projection 비중단).
     */
    private void submitSnapshot(UUID sessionId, long upToSeq) {
        snapshotExecutor.submit(() -> {
            try {
                snapshotService.createSnapshot(sessionId, upToSeq);
            } catch (RuntimeException ex) {
                log.warn("Snapshot creation failed for session={}, upToSeq={}, projection continues (FR-P2-8)",
                        sessionId, upToSeq, ex);
            }
        });
    }
}
