package com.realtimechat.projection;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * message_view 읽기 모델 접근 DAO (설계서 §3.2).
 *
 * <p>적용(apply*) 메서드는 호출자(ProjectionUpdater)의 트랜잭션에서 실행되며,
 * {@code last_updated_seq} seq-guard로 멱등성을 보장한다(설계서 §4.1 계층2,
 * §4.3 last-edit-wins). 최근 조회는 ix_msg_recent (session_id, seq DESC)를 활용한다(§10 Q2).
 */
@Repository
public class MessageViewDao {

    /** 메시지 생성 INSERT. 동일 (session_id, message_id) 충돌 시 DO NOTHING(멱등). */
    private static final String APPLY_SENT_SQL =
            "INSERT INTO message_view "
                    + "(session_id, message_id, seq, sender_id, content, state, created_at, last_updated_seq) "
                    + "VALUES (:sid, :mid, :seq, :sender, :content, 'SENT', :createdAt, :seq) "
                    + "ON CONFLICT (session_id, message_id) DO NOTHING";

    private static final String APPLY_EDITED_SQL =
            "UPDATE message_view SET content = :content, state = 'EDITED', last_updated_seq = :seq "
                    + "WHERE session_id = :sid AND message_id = :mid "
                    + "AND :seq > last_updated_seq AND state <> 'DELETED'";

    private static final String APPLY_DELETED_SQL =
            "UPDATE message_view SET state = 'DELETED', last_updated_seq = :seq "
                    + "WHERE session_id = :sid AND message_id = :mid AND :seq > last_updated_seq";

    private static final String FIND_RECENT_SQL =
            "SELECT message_id, seq, sender_id, content, state, created_at FROM message_view "
                    + "WHERE session_id = :sid AND state <> 'DELETED' ORDER BY seq DESC LIMIT :limit";

    private final NamedParameterJdbcTemplate jdbc;
    private final RowMapper<MessageRow> rowMapper = new MessageRowMapper();

    public MessageViewDao(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 메시지 생성을 적용한다.
     *
     * @return 실제 신규 행이 INSERT된 경우 {@code true}, 충돌(중복 재전달)로 DO NOTHING이면
     *     {@code false}. ProjectionUpdater는 이 값으로 message_count 중복 증가를 방지한다.
     */
    public boolean applySent(UUID sessionId, UUID messageId, long seq, UUID senderId,
                             String content, Instant createdAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("mid", messageId)
                .addValue("seq", seq)
                .addValue("sender", senderId)
                .addValue("content", content)
                .addValue("createdAt", createdAt == null ? null : Timestamp.from(createdAt));
        return jdbc.update(APPLY_SENT_SQL, params) > 0;
    }

    public void applyEdited(UUID sessionId, UUID messageId, String content, long seq) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("mid", messageId)
                .addValue("content", content)
                .addValue("seq", seq);
        jdbc.update(APPLY_EDITED_SQL, params);
    }

    public void applyDeleted(UUID sessionId, UUID messageId, long seq) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("mid", messageId)
                .addValue("seq", seq);
        jdbc.update(APPLY_DELETED_SQL, params);
    }

    /** 삭제되지 않은 최근 메시지 N개를 seq 내림차순으로 조회한다(§10 Q2, ix_msg_recent). */
    public List<MessageRow> findRecent(UUID sessionId, int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("limit", limit);
        return jdbc.query(FIND_RECENT_SQL, params, rowMapper);
    }

    /**
     * message_view 한 행의 읽기 표현.
     *
     * @param messageId message_id
     * @param seq       생성 seq(정렬 키)
     * @param senderId  sender_id
     * @param content   본문(EDITED 시 갱신)
     * @param state     메시지 상태 문자열(SENT/EDITED/DELETED)
     * @param createdAt 생성 시각
     */
    public record MessageRow(
            UUID messageId,
            long seq,
            UUID senderId,
            String content,
            String state,
            Instant createdAt) {
    }

    private static final class MessageRowMapper implements RowMapper<MessageRow> {
        @Override
        public MessageRow mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new MessageRow(
                    rs.getObject("message_id", UUID.class),
                    rs.getLong("seq"),
                    rs.getObject("sender_id", UUID.class),
                    rs.getString("content"),
                    rs.getString("state"),
                    toInstant(rs.getTimestamp("created_at")));
        }
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
