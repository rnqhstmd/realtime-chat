package com.realtimechat.restore;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.common.error.SessionNotFoundException;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventStore;
import com.realtimechat.session.SessionDao;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 스냅샷 생성(설계서 §3.3, §5 Snapshot 자동화, §10 Q3).
 *
 * <p>현재 maxSeq까지 상태를 복원한 뒤 "최근 N개" 메시지만 담아 멱등 저장한다.
 * 복원 비용 상한을 스냅샷 주기로 고정하기 위한 컴포넌트다(설계서 §2.2, §4.3).
 */
@Service
public class SnapshotService {

    private final EventStore eventStore;
    private final RestoreService restoreService;
    private final SnapshotDao snapshotDao;
    private final SessionDao sessionDao;

    /** 스냅샷에 담을 최근 메시지 수 상한(설계서 §3.3, §4.3: POC 기본 N). */
    private final int recentMessages;

    public SnapshotService(
            EventStore eventStore,
            RestoreService restoreService,
            SnapshotDao snapshotDao,
            SessionDao sessionDao,
            @Value("${chat.snapshot.recent-messages:200}") int recentMessages) {
        this.eventStore = eventStore;
        this.restoreService = restoreService;
        this.snapshotDao = snapshotDao;
        this.sessionDao = sessionDao;
        this.recentMessages = recentMessages;
    }

    /**
     * 현재 시점까지의 스냅샷을 생성한다.
     *
     * <p>세션이 없으면 {@link SessionNotFoundException}(404)으로 거부한다. maxSeq = 현재까지의 최대
     * seq(시각 무관, seq 권위 기준). 존재하나 이벤트가 없으면(maxSeq=0) no-op. 있으면 maxSeq까지
     * 복원한 상태를 §3.3 포맷(최근 N)으로 직렬화하여 멱등 저장한다(같은 up_to_seq 재생성 무해).
     * occurred_at 비교가 아닌 seq 상한을 쓰므로 동시 append의 시각 미세 역전과 무관하게 직전 채번된
     * 모든 이벤트를 포함한다.
     */
    @Transactional
    public void createSnapshot(UUID sessionId) {
        if (!sessionDao.exists(sessionId)) {
            throw new SessionNotFoundException(sessionId);
        }
        long maxSeq = eventStore.maxSeq(sessionId);
        if (maxSeq == 0L) {
            return;
        }

        SessionState state = restoreService.restoreTo(sessionId, maxSeq);
        SnapshotState snapshotState = SnapshotState.from(state, recentMessages);
        JsonNode stateJson = JsonUtil.toJsonNode(snapshotState);
        snapshotDao.save(sessionId, maxSeq, stateJson);
    }
}
