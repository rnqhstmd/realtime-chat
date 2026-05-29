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
import java.util.Optional;
import java.util.UUID;

/**
 * session_view 읽기 모델 접근 DAO (설계서 §3.2).
 *
 * <p>카운터·활동시각 갱신은 호출자(ProjectionUpdater)의 트랜잭션에서 실행된다. 행 생성
 * (insertIfAbsent)·종료(markEnded)는 B4 SessionService가 호출한다(이 DAO는 메서드만 제공).
 * 목록 조회는 ix_session_list (status, created_at DESC)를 활용한다(설계서 §11, §3.2).
 */
@Repository
public class SessionViewDao {

    /** 세션 생성 시 행 삽입. 이미 있으면 DO NOTHING(멱등). counts는 0에서 시작. */
    private static final String INSERT_IF_ABSENT_SQL =
            "INSERT INTO session_view "
                    + "(session_id, status, participant_count, message_count, created_at, last_activity_at, ended_at) "
                    + "VALUES (:sid, 'ACTIVE', 0, 0, :createdAt, :createdAt, NULL) "
                    + "ON CONFLICT (session_id) DO NOTHING";

    private static final String INCREMENT_PARTICIPANTS_SQL =
            "UPDATE session_view SET participant_count = participant_count + 1 WHERE session_id = :sid";

    private static final String DECREMENT_PARTICIPANTS_SQL =
            "UPDATE session_view SET participant_count = participant_count - 1 "
                    + "WHERE session_id = :sid AND participant_count > 0";

    private static final String INCREMENT_MESSAGES_SQL =
            "UPDATE session_view SET message_count = message_count + 1 WHERE session_id = :sid";

    // :at이 NULL이면 last_activity_at을 덮어쓰지 않도록 방어(가드). 현 호출부는 비-null을 넘기지만,
    // NULL 전달 시 기존 활동시각을 NULL로 지우는 사고를 막는다.
    // WHERE 절의 :at은 CAST(... AS timestamptz)로 타입을 명시해야 PostgreSQL이 NULL 파라미터의
    // 데이터 타입을 추론할 수 있다('? IS NOT NULL' 단독은 untyped → 42P18 오류).
    private static final String TOUCH_ACTIVITY_SQL =
            "UPDATE session_view SET last_activity_at = :at "
                    + "WHERE session_id = :sid AND CAST(:at AS timestamptz) IS NOT NULL";

    private static final String MARK_ENDED_SQL =
            "UPDATE session_view SET status = 'ENDED', ended_at = :endedAt WHERE session_id = :sid";

    private static final String SELECT_COLUMNS =
            "session_id, status, participant_count, message_count, created_at, last_activity_at, ended_at";

    private static final String FIND_SQL =
            "SELECT " + SELECT_COLUMNS + " FROM session_view WHERE session_id = :sid";

    private static final String LIST_ALL_SQL =
            "SELECT " + SELECT_COLUMNS + " FROM session_view ORDER BY created_at DESC";

    private static final String LIST_BY_STATUS_SQL =
            "SELECT " + SELECT_COLUMNS + " FROM session_view WHERE status = :status ORDER BY created_at DESC";

    private final NamedParameterJdbcTemplate jdbc;
    private final RowMapper<SessionRow> rowMapper = new SessionRowMapper();

    public SessionViewDao(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insertIfAbsent(UUID sessionId, Instant createdAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("createdAt", createdAt == null ? null : Timestamp.from(createdAt));
        jdbc.update(INSERT_IF_ABSENT_SQL, params);
    }

    public void incrementParticipants(UUID sessionId) {
        jdbc.update(INCREMENT_PARTICIPANTS_SQL, sidParams(sessionId));
    }

    public void decrementParticipants(UUID sessionId) {
        jdbc.update(DECREMENT_PARTICIPANTS_SQL, sidParams(sessionId));
    }

    public void incrementMessages(UUID sessionId) {
        jdbc.update(INCREMENT_MESSAGES_SQL, sidParams(sessionId));
    }

    public void touchActivity(UUID sessionId, Instant at) {
        // NULL 가드는 TOUCH_ACTIVITY_SQL의 CAST(:at AS timestamptz) IS NOT NULL에서 처리한다.
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("at", at == null ? null : Timestamp.from(at));
        jdbc.update(TOUCH_ACTIVITY_SQL, params);
    }

    public void markEnded(UUID sessionId, Instant endedAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("endedAt", endedAt == null ? null : Timestamp.from(endedAt));
        jdbc.update(MARK_ENDED_SQL, params);
    }

    public Optional<SessionRow> find(UUID sessionId) {
        List<SessionRow> rows = jdbc.query(FIND_SQL, sidParams(sessionId), rowMapper);
        return rows.stream().findFirst();
    }

    /** 세션 목록(설계서 §11 GET /sessions). status가 null이면 전체, 아니면 해당 상태만. created_at DESC. */
    public List<SessionRow> list(String status) {
        if (status == null) {
            return jdbc.query(LIST_ALL_SQL, new MapSqlParameterSource(), rowMapper);
        }
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("status", status);
        return jdbc.query(LIST_BY_STATUS_SQL, params, rowMapper);
    }

    private static MapSqlParameterSource sidParams(UUID sessionId) {
        return new MapSqlParameterSource().addValue("sid", sessionId);
    }

    /**
     * session_view 한 행의 읽기 표현.
     *
     * @param sessionId        session_id
     * @param status           ACTIVE / ENDED
     * @param participantCount 활성 참여자 수
     * @param messageCount     누적 메시지 수
     * @param createdAt        생성 시각
     * @param lastActivityAt   마지막 활동 시각(nullable)
     * @param endedAt          종료 시각(nullable)
     */
    public record SessionRow(
            UUID sessionId,
            String status,
            int participantCount,
            long messageCount,
            Instant createdAt,
            Instant lastActivityAt,
            Instant endedAt) {
    }

    private static final class SessionRowMapper implements RowMapper<SessionRow> {
        @Override
        public SessionRow mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new SessionRow(
                    rs.getObject("session_id", UUID.class),
                    rs.getString("status"),
                    rs.getInt("participant_count"),
                    rs.getLong("message_count"),
                    toInstant(rs.getTimestamp("created_at")),
                    toInstant(rs.getTimestamp("last_activity_at")),
                    toInstant(rs.getTimestamp("ended_at")));
        }
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
