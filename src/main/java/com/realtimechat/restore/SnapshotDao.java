package com.realtimechat.restore;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.common.json.JsonUtil;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * snapshot 테이블 접근(설계서 §3.3, §4.3, §10 Q3).
 *
 * <p>스냅샷은 멱등 생성한다 — 같은 {@code (session_id, up_to_seq)} 재생성은 무해(설계서 §3.3).
 * state JSONB는 EventStore와 동일하게 {@code CAST(:state AS jsonb)}로 바인딩한다.
 */
@Repository
public class SnapshotDao {

    /** 스냅샷 1행. snapshot 테이블과 1:1 대응. */
    public record Snapshot(UUID sessionId, long upToSeq, JsonNode state, Instant takenAt) {
    }

    private static final String SELECT_LATEST_AT_OR_BEFORE_SQL =
            "SELECT session_id, up_to_seq, state, taken_at FROM snapshot "
                    + "WHERE session_id = :sid AND up_to_seq <= :upToSeq "
                    + "ORDER BY up_to_seq DESC LIMIT 1";

    /** 멱등 INSERT: 동일 PK 충돌 시 DO NOTHING. taken_at은 DB now() DEFAULT. */
    private static final String INSERT_SNAPSHOT_SQL =
            "INSERT INTO snapshot (session_id, up_to_seq, state) "
                    + "VALUES (:sid, :upToSeq, CAST(:state AS jsonb)) "
                    + "ON CONFLICT (session_id, up_to_seq) DO NOTHING";

    private final NamedParameterJdbcTemplate jdbc;
    private final RowMapper<Snapshot> rowMapper = new SnapshotRowMapper();

    public SnapshotDao(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** {@code up_to_seq <= upToSeq} 중 최근접 스냅샷(설계서 §4.3 step2, §10 Q3). */
    public Optional<Snapshot> findLatestAtOrBefore(UUID sessionId, long upToSeq) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("upToSeq", upToSeq);
        List<Snapshot> rows = jdbc.query(SELECT_LATEST_AT_OR_BEFORE_SQL, params, rowMapper);
        return rows.stream().findFirst();
    }

    /** 스냅샷 저장(멱등). state는 SessionState 직렬화 결과(설계서 §3.3 포맷). */
    public void save(UUID sessionId, long upToSeq, JsonNode state) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("upToSeq", upToSeq)
                .addValue("state", JsonUtil.toJson(state));
        jdbc.update(INSERT_SNAPSHOT_SQL, params);
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }

    /** snapshot 한 행 → Snapshot. state(JSONB)는 JsonUtil.readTree로 JsonNode 매핑. */
    private static final class SnapshotRowMapper implements RowMapper<Snapshot> {
        @Override
        public Snapshot mapRow(ResultSet rs, int rowNum) throws SQLException {
            String stateJson = rs.getString("state");
            JsonNode state = stateJson == null ? null : JsonUtil.readTree(stateJson);
            return new Snapshot(
                    rs.getObject("session_id", UUID.class),
                    rs.getLong("up_to_seq"),
                    state,
                    toInstant(rs.getTimestamp("taken_at")));
        }
    }
}
