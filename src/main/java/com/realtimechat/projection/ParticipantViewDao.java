package com.realtimechat.projection;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * participant_view 읽기 모델 접근 DAO (설계서 §3.2).
 *
 * <p>모든 적용(apply*) 메서드는 호출자(ProjectionUpdater)의 트랜잭션 안에서 실행되며,
 * {@code last_applied_seq} seq-guard로 멱등성을 보장한다(설계서 §4.1 계층2). 동일 이벤트가
 * at-least-once로 재전달되어도 {@code seq <= last_applied_seq}면 적용을 건너뛴다.
 *
 * <p><b>전이(transition) 판정</b>: applyJoined/applyLeft는 같은 트랜잭션 안에서 현재 행을
 * {@code FOR UPDATE}로 잠그고 이전 status를 본 뒤, <b>실제 상태 전이가 일어난 경우에만</b>
 * {@code true}를 반환한다. ProjectionUpdater는 이 반환값으로 session_view.participant_count를
 * 증감하므로, 반복/중복 이벤트나 이미 같은 상태인 참여자에 대해서는 카운터가 변하지 않는다.
 */
@Repository
public class ParticipantViewDao {

    /** 현재 행을 잠그고(FOR UPDATE) 전이 판정에 필요한 status/last_applied_seq를 읽는다. */
    private static final String SELECT_FOR_UPDATE_SQL =
            "SELECT status, last_applied_seq FROM participant_view "
                    + "WHERE session_id = :sid AND participant_id = :pid FOR UPDATE";

    /** 신규 참여자 INSERT(status=JOINED / joined_seq=seq / presence='ONLINE'). */
    private static final String INSERT_JOINED_SQL =
            "INSERT INTO participant_view "
                    + "(session_id, participant_id, status, presence, joined_seq, left_seq, last_applied_seq) "
                    + "VALUES (:sid, :pid, 'JOINED', 'ONLINE', :seq, NULL, :seq)";

    /**
     * 기존 행을 JOINED로 갱신(재가입 포함). left_seq는 NULL로 되돌리고 joined_seq는 새 seq로 갱신한다.
     * LEFT→JOINED 재가입 시 joined_seq를 새 seq로 옮겨, 복원 모델({@code Fold.applyParticipantJoined}가
     * joinedSeq=event.seq()로 설정)과 동기 projection의 joined_seq를 일치시킨다.
     */
    private static final String UPDATE_JOINED_SQL =
            "UPDATE participant_view SET status = 'JOINED', joined_seq = :seq, last_applied_seq = :seq, left_seq = NULL "
                    + "WHERE session_id = :sid AND participant_id = :pid";

    private static final String UPDATE_LEFT_SQL =
            "UPDATE participant_view SET status = 'LEFT', left_seq = :seq, last_applied_seq = :seq "
                    + "WHERE session_id = :sid AND participant_id = :pid";

    private static final String APPLY_PRESENCE_SQL =
            "UPDATE participant_view SET presence = :presence, last_applied_seq = :seq "
                    + "WHERE session_id = :sid AND participant_id = :pid AND :seq > last_applied_seq";

    private static final String COUNT_ACTIVE_SQL =
            "SELECT count(*) FROM participant_view WHERE session_id = :sid AND status = 'JOINED'";

    private final NamedParameterJdbcTemplate jdbc;

    public ParticipantViewDao(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 참여자 가입을 적용한다(전이 판정).
     *
     * <p>같은 트랜잭션에서 현재 행을 {@code FOR UPDATE}로 잠그고 이전 status를 본다.
     * <ul>
     *   <li>행 없음 → INSERT(JOINED). 활성화 전이 → {@code true}.</li>
     *   <li>행 있고 {@code seq > last_applied_seq} → UPDATE(JOINED). 이전이 JOINED가 아니면
     *       (LEFT→JOINED 재가입) {@code true}, 이미 JOINED면 {@code false}.</li>
     *   <li>stale seq(재전달) → no-op, {@code false}.</li>
     * </ul>
     *
     * @return 활성 참여자가 새로 늘어난 경우({@code becameActive}) {@code true}. ProjectionUpdater는
     *     이 값으로 session_view.participant_count 증가를 결정한다.
     */
    public boolean applyJoined(UUID sessionId, UUID participantId, long seq) {
        Row current = selectForUpdate(sessionId, participantId);
        MapSqlParameterSource params = transitionParams(sessionId, participantId, seq);
        if (current == null) {
            jdbc.update(INSERT_JOINED_SQL, params);
            return true;
        }
        if (seq > current.lastAppliedSeq()) {
            jdbc.update(UPDATE_JOINED_SQL, params);
            return !"JOINED".equals(current.status());
        }
        return false;
    }

    /**
     * 참여자 퇴장을 적용한다(전이 판정).
     *
     * <p>같은 트랜잭션에서 현재 행을 {@code FOR UPDATE}로 잠그고 이전 status를 본다.
     * <ul>
     *   <li>행 없음 → no-op(가입 이력 없는 leave 무시), {@code false}.</li>
     *   <li>행 있고 {@code seq > last_applied_seq} → UPDATE(LEFT). 이전이 JOINED일 때만
     *       (JOINED→LEFT) {@code true}, 그 외 {@code false}.</li>
     *   <li>stale seq(재전달) → no-op, {@code false}.</li>
     * </ul>
     *
     * @return 활성 참여자가 줄어든 경우({@code becameInactive}) {@code true}. ProjectionUpdater는
     *     이 값으로 session_view.participant_count 감소를 결정한다.
     */
    public boolean applyLeft(UUID sessionId, UUID participantId, long seq) {
        Row current = selectForUpdate(sessionId, participantId);
        if (current == null) {
            return false;
        }
        if (seq > current.lastAppliedSeq()) {
            jdbc.update(UPDATE_LEFT_SQL, transitionParams(sessionId, participantId, seq));
            return "JOINED".equals(current.status());
        }
        return false;
    }

    private Row selectForUpdate(UUID sessionId, UUID participantId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("pid", participantId);
        return jdbc.query(SELECT_FOR_UPDATE_SQL, params,
                        (rs, rowNum) -> new Row(rs.getString("status"), rs.getLong("last_applied_seq")))
                .stream()
                .findFirst()
                .orElse(null);
    }

    private static MapSqlParameterSource transitionParams(UUID sessionId, UUID participantId, long seq) {
        return new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("pid", participantId)
                .addValue("seq", seq);
    }

    /** 전이 판정용으로 잠근 현재 행의 status/last_applied_seq. */
    private record Row(String status, long lastAppliedSeq) {
    }

    /**
     * presence 변경을 적용한다(seq-guard).
     *
     * <p>늦게 도착한 과거 PRESENCE_CHANGED 이벤트는 {@code last_applied_seq} seq-guard
     * ({@code :seq > last_applied_seq})에 의해 silently drop될 수 있다. 이는 의도된 동작이다:
     * <ol>
     *   <li>presence는 휘발성·최신 우선 데이터다. Redis TTL이 권위 있는 상태이며, 이벤트는 감사
     *       목적으로 기록된다.</li>
     *   <li>정확한 과거 presence 복원이 필요한 경우 replay 경로({@code Fold})가 BR-6 기준으로
     *       제공된다.</li>
     *   <li>presence 전용 seq 컬럼을 별도로 두는 것은 과설계다. {@code last_applied_seq} 공유로
     *       충분하다.</li>
     * </ol>
     *
     * @param sessionId     세션 ID
     * @param participantId 참여자 ID
     * @param presence      변경된 presence 값 (예: "ONLINE", "OFFLINE")
     * @param seq           이벤트 seq. {@code last_applied_seq} 이하면 적용을 건너뛴다.
     */
    public void applyPresence(UUID sessionId, UUID participantId, String presence, long seq) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("pid", participantId)
                .addValue("presence", presence)
                .addValue("seq", seq);
        jdbc.update(APPLY_PRESENCE_SQL, params);
    }

    /** 현재 활성(JOINED) 참여자 수(설계서 §3.2 ix_part_active). */
    public int countActive(UUID sessionId) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("sid", sessionId);
        Integer count = jdbc.queryForObject(COUNT_ACTIVE_SQL, params, Integer.class);
        return count == null ? 0 : count;
    }
}
