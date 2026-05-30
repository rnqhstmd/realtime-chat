package com.realtimechat.outbox;

import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
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
 * outbox 테이블 접근 DAO (설계서 §8).
 *
 * <p>트랜잭션은 호출자(EventStore 등)가 관리한다. {@link #insert(StoredEvent)}는
 * 이벤트 저장과 동일 TX에서 호출되어 아웃박스 패턴의 원자성을 보장한다.
 * {@link #findUnpublished(int)}는 ix_outbox_unpub 부분 인덱스를 활용하여
 * 미발행 레코드를 id ASC(발행 순서) 기준으로 조회한다(설계서 §8).
 */
@Repository
public class OutboxDao {

    private static final String INSERT_SQL =
            "INSERT INTO outbox "
                    + "(event_id, session_id, seq, published, event_type, payload, idempotency_key, actor_id, occurred_at) "
                    + "VALUES (:eventId, :sessionId, :seq, FALSE, :eventType, CAST(:payload AS jsonb), "
                    + ":idempotencyKey, :actorId, :occurredAt)";

    private static final String FIND_UNPUBLISHED_SQL =
            "SELECT id, event_id, session_id, seq, event_type, payload, idempotency_key, actor_id, occurred_at "
                    + "FROM outbox WHERE published = FALSE ORDER BY id ASC LIMIT :batch";

    private static final String MARK_PUBLISHED_SQL =
            "UPDATE outbox SET published = TRUE WHERE id IN (:ids)";

    private final NamedParameterJdbcTemplate jdbc;
    private final RowMapper<OutboxRecord> rowMapper = new OutboxRowMapper();

    public OutboxDao(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * StoredEvent를 outbox에 INSERT한다.
     *
     * <p>id는 BIGSERIAL로 DB 채번되므로 생략한다. published는 FALSE로 고정.
     * payload는 {@link JsonUtil#toJson(Object)}로 직렬화 후 CAST(:payload AS jsonb)로 저장한다.
     */
    public void insert(StoredEvent e) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("eventId", e.eventId())
                .addValue("sessionId", e.sessionId())
                .addValue("seq", e.seq())
                .addValue("eventType", e.type().name())
                .addValue("payload", JsonUtil.toJson(e.payload()))
                .addValue("idempotencyKey", e.idempotencyKey())
                .addValue("actorId", e.actorId())
                .addValue("occurredAt", e.occurredAt() == null ? null : Timestamp.from(e.occurredAt()));
        jdbc.update(INSERT_SQL, params);
    }

    /**
     * 미발행(published = FALSE) 레코드를 id ASC 순으로 최대 {@code batch}건 조회한다.
     *
     * <p>ix_outbox_unpub 부분 인덱스(WHERE published = FALSE)를 활용한다.
     */
    public List<OutboxRecord> findUnpublished(int batch) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("batch", batch);
        return jdbc.query(FIND_UNPUBLISHED_SQL, params, rowMapper);
    }

    /**
     * 지정한 id 목록을 published = TRUE로 갱신한다.
     *
     * <p>ids가 비어 있으면 빈 IN 절 생성을 방지하기 위해 즉시 반환한다.
     */
    public void markPublished(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("ids", ids);
        jdbc.update(MARK_PUBLISHED_SQL, params);
    }

    private static final class OutboxRowMapper implements RowMapper<OutboxRecord> {
        @Override
        public OutboxRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            String actorIdStr = rs.getString("actor_id");
            UUID actorId = actorIdStr == null ? null : UUID.fromString(actorIdStr);

            return new OutboxRecord(
                    rs.getLong("id"),
                    rs.getObject("event_id", UUID.class),
                    rs.getObject("session_id", UUID.class),
                    rs.getLong("seq"),
                    EventType.valueOf(rs.getString("event_type")),
                    JsonUtil.readTree(rs.getString("payload")),
                    rs.getString("idempotency_key"),
                    actorId,
                    toInstant(rs.getTimestamp("occurred_at")));
        }
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
