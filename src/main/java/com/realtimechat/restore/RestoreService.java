package com.realtimechat.restore;

import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventStore;
import com.realtimechat.event.StoredEvent;
import com.realtimechat.restore.SnapshotDao.Snapshot;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 시점 복원 오케스트레이션(설계서 §4.3, §10 Q3).
 *
 * <p>알고리즘(§4.3): 최근접 스냅샷을 base로, 그 이후 (baseSeq, atSeq] 이벤트만 replay하여
 * {@link Fold}로 누적한다. 복원 비용 상한이 스냅샷 주기 N으로 고정된다.
 *
 * <p>이 서비스는 동기다(Phase 1). gap-fill/스트림 로직은 두지 않는다.
 */
@Service
public class RestoreService {

    private final SnapshotDao snapshotDao;
    private final EventStore eventStore;

    public RestoreService(SnapshotDao snapshotDao, EventStore eventStore) {
        this.snapshotDao = snapshotDao;
        this.eventStore = eventStore;
    }

    /**
     * seq 기준 시점 복원(설계서 §4.3 step2~4).
     *
     * <ol>
     *   <li>base = 최근접 snapshot(up_to_seq &lt;= atSeq) → 있으면 state(JSONB)→SessionState,
     *       없으면 빈 상태(baseSeq=0).</li>
     *   <li>events = (baseSeq, atSeq] 구간 이벤트.</li>
     *   <li>{@link Fold#fold}로 누적.</li>
     * </ol>
     */
    @Transactional(readOnly = true)
    public SessionState restoreTo(UUID sessionId, long atSeq) {
        Optional<Snapshot> snapshot = snapshotDao.findLatestAtOrBefore(sessionId, atSeq);

        SessionState base;
        long baseSeq;
        if (snapshot.isPresent()) {
            Snapshot s = snapshot.get();
            base = JsonUtil.fromNode(s.state(), SnapshotState.class).toSessionState();
            baseSeq = s.upToSeq();
        } else {
            base = new SessionState();
            baseSeq = 0L;
        }

        List<StoredEvent> events = eventStore.findBySeqRange(sessionId, baseSeq, atSeq);
        return Fold.fold(base, events);
    }

    /**
     * timestamp 기준 시점 복원(설계서 §4.3 step1). at 이하 최대 seq를 구해 {@link #restoreTo}로 위임.
     * 해당 시점 이전 이벤트가 없으면 atSeq=0 → 빈 상태가 반환된다.
     */
    @Transactional(readOnly = true)
    public SessionState restoreAt(UUID sessionId, Instant at) {
        long atSeq = eventStore.maxSeqAtOrBefore(sessionId, at).orElse(0L);
        return restoreTo(sessionId, atSeq);
    }

    /**
     * 현재 시점 복원(시계 무관). 현재 maxSeq(seq 권위, 시각 비교 없음)까지 복원한다.
     * at 미지정 timeline 조회의 기본 경로로, 동시 append의 occurred_at 미세 역전과 무관하게
     * 직전 채번된 모든 이벤트를 포함한다(설계서 §4.2 seq 권위, §4.3).
     */
    @Transactional(readOnly = true)
    public SessionState restoreCurrent(UUID sessionId) {
        return restoreTo(sessionId, eventStore.maxSeq(sessionId));
    }
}
