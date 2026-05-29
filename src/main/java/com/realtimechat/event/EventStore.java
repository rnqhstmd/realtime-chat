package com.realtimechat.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.common.error.SessionNotFoundException;
import com.realtimechat.common.json.JsonUtil;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * append-only Event Store (설계서 §2.2, §3.1, §4).
 *
 * <p>모든 메서드는 호출자(CommandHandler)의 트랜잭션 안에서 실행된다고 가정한다.
 * 따라서 자체 {@code @Transactional}을 두지 않는다. 멱등성·seq 채번 규칙은 §4.1/§4.2,
 * §10 Q1을 그대로 따른다.
 */
@Repository
public class EventStore {

    private static final String SELECT_COLUMNS =
            "event_id, session_id, seq, event_type, payload, idempotency_key, actor_id, occurred_at";

    /** seq 채번: session.last_seq를 1 증가시키고 채번된 값을 돌려받는다(§10 Q1). */
    private static final String BUMP_SEQ_SQL =
            "UPDATE session SET last_seq = last_seq + 1 WHERE id = :sid RETURNING last_seq";

    /**
     * event INSERT. 동일 (session_id, idempotency_key) 충돌 시 DO NOTHING.
     * 삽입에 성공한 경우에만 RETURNING으로 occurred_at을 돌려받는다(충돌 시 빈 결과).
     */
    private static final String INSERT_EVENT_SQL =
            "INSERT INTO event (event_id, session_id, seq, event_type, payload, idempotency_key, actor_id) "
                    + "VALUES (:eid, :sid, :seq, :type, CAST(:payload AS jsonb), :idem, :actor) "
                    + "ON CONFLICT (session_id, idempotency_key) DO NOTHING "
                    + "RETURNING occurred_at";

    private static final String SELECT_BY_IDEM_SQL =
            "SELECT " + SELECT_COLUMNS + " FROM event "
                    + "WHERE session_id = :sid AND idempotency_key = :idem";

    private static final String SELECT_BY_SEQ_RANGE_SQL =
            "SELECT " + SELECT_COLUMNS + " FROM event "
                    + "WHERE session_id = :sid AND seq > :from AND seq <= :to ORDER BY seq";

    private static final String SELECT_MAX_SEQ_AT_OR_BEFORE_SQL =
            "SELECT max(seq) FROM event WHERE session_id = :sid AND occurred_at <= :at";

    private static final String SELECT_MAX_SEQ_SQL =
            "SELECT COALESCE(max(seq), 0) FROM event WHERE session_id = :sid";

    private final NamedParameterJdbcTemplate jdbc;
    private final RowMapper<StoredEvent> rowMapper = new StoredEventRowMapper();

    public EventStore(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 이벤트를 append한다. 2계층 멱등성 중 1계층(수집 멱등성)을 구현한다(§4.1).
     *
     * <p>규칙(§4.1, §4.2, §10 Q1):
     * <ol>
     *   <li>(a) 먼저 (session_id, idempotency_key)로 기존 이벤트 조회 → 있으면 그대로 반환.
     *       이 경로는 seq 채번 자체를 하지 않으므로 seq 소비가 없다.</li>
     *   <li>(b) 없으면 session.last_seq를 채번하고 INSERT ... ON CONFLICT DO NOTHING.</li>
     *   <li>(c) INSERT 반영행이 0이면(동시 race로 동일 idempotency_key가 먼저 커밋됨) → 기존 이벤트를
     *       재조회하여 반환. 단 이 경로에서는 (b)에서 이미 채번한 seq가 어느 행에도 쓰이지 못하고
     *       버려지므로 seq에 gap이 생길 수 있다. Phase 1은 seq를 단조증가하는 정렬 권위로만
     *       보장하며 연속성(빈틈 없음)은 보장하지 않는다 — 복원/replay는 seq 순서에만 의존한다.</li>
     *   <li>(d) occurred_at은 DB now() DEFAULT, event_id는 앱에서 생성.</li>
     * </ol>
     *
     * <p>반환값({@link AppendResult})은 신규 INSERT 여부를 포함한다 — (a)/(c) 멱등 재유입은
     * {@code isNew=false}, (b) 신규 INSERT만 {@code isNew=true}. 호출자(CommandHandler)는 이 값으로
     * projection·broadcast의 중복 수행을 막는다(§4.1 계층2).
     */
    public AppendResult append(AppendCommand cmd) {
        // (a) 기존 이벤트가 있으면 seq 소비 없이 그대로 반환(멱등 재유입 → isNew=false)
        Optional<StoredEvent> existing = findByIdempotencyKey(cmd.sessionId(), cmd.idempotencyKey());
        if (existing.isPresent()) {
            return new AppendResult(existing.get(), false);
        }

        // (b) seq 채번 후 INSERT
        long seq = bumpSeq(cmd.sessionId());
        UUID eventId = UUID.randomUUID();

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("eid", eventId)
                .addValue("sid", cmd.sessionId())
                .addValue("seq", seq)
                .addValue("type", cmd.type().name())
                .addValue("payload", JsonUtil.toJson(cmd.payload()))
                .addValue("idem", cmd.idempotencyKey())
                .addValue("actor", cmd.actorId());

        List<Instant> occurredAt = jdbc.query(INSERT_EVENT_SQL, params,
                (rs, rowNum) -> toInstant(rs.getTimestamp("occurred_at")));

        if (!occurredAt.isEmpty()) {
            // 삽입 성공: 채번된 seq와 DB 생성 occurred_at으로 결과 구성(신규 → isNew=true)
            StoredEvent stored = new StoredEvent(
                    eventId,
                    cmd.sessionId(),
                    seq,
                    cmd.type(),
                    cmd.payload(),
                    cmd.idempotencyKey(),
                    cmd.actorId(),
                    occurredAt.get(0));
            return new AppendResult(stored, true);
        }

        // (c) 충돌(동시 race): 먼저 삽입한 트랜잭션의 이벤트를 재조회하여 반환(멱등 재유입 → isNew=false)
        StoredEvent raced = findByIdempotencyKey(cmd.sessionId(), cmd.idempotencyKey())
                .orElseThrow(() -> new IllegalStateException(
                        "Append conflicted but no existing event found for session="
                                + cmd.sessionId() + " idempotencyKey=" + cmd.idempotencyKey()));
        return new AppendResult(raced, false);
    }

    public Optional<StoredEvent> findByIdempotencyKey(UUID sessionId, String key) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("idem", key);
        List<StoredEvent> rows = jdbc.query(SELECT_BY_IDEM_SQL, params, rowMapper);
        return rows.stream().findFirst();
    }

    /** seq 범위 (fromExclusive, toInclusive] 조회 — resume/replay용(§4.2, §4.3). */
    public List<StoredEvent> findBySeqRange(UUID sessionId, long fromExclusive, long toInclusive) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("from", fromExclusive)
                .addValue("to", toInclusive);
        return jdbc.query(SELECT_BY_SEQ_RANGE_SQL, params, rowMapper);
    }

    /** at 시각 이하 최대 seq — 시점 복원에서 ts→seq 매핑(§4.3, §10 Q3). */
    public Optional<Long> maxSeqAtOrBefore(UUID sessionId, Instant at) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("at", Timestamp.from(at));
        Long maxSeq = jdbc.queryForObject(SELECT_MAX_SEQ_AT_OR_BEFORE_SQL, params, Long.class);
        return Optional.ofNullable(maxSeq);
    }

    /**
     * 세션의 현재 최대 seq(시각 무관). 이벤트가 없으면 0.
     *
     * <p>스냅샷 생성은 "지금까지 채번된 모든 이벤트"를 포함해야 하므로 occurred_at 비교가 아니라
     * seq 권위(§4.2)로 상한을 잡는다. 동시 append의 occurred_at 미세 역전과 무관하게 결정적이다.
     */
    public long maxSeq(UUID sessionId) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("sid", sessionId);
        Long maxSeq = jdbc.queryForObject(SELECT_MAX_SEQ_SQL, params, Long.class);
        return maxSeq == null ? 0L : maxSeq;
    }

    private long bumpSeq(UUID sessionId) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("sid", sessionId);
        try {
            Long seq = jdbc.queryForObject(BUMP_SEQ_SQL, params, Long.class);
            if (seq == null) {
                throw new SessionNotFoundException(sessionId);
            }
            return seq;
        } catch (EmptyResultDataAccessException e) {
            // UPDATE ... RETURNING이 0행이면 해당 세션이 없음
            throw new SessionNotFoundException(sessionId);
        }
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }

    /** event 한 행 → StoredEvent. payload(JSONB)는 JsonUtil.readTree로 JsonNode 매핑. */
    private static final class StoredEventRowMapper implements RowMapper<StoredEvent> {
        @Override
        public StoredEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
            String payloadJson = rs.getString("payload");
            JsonNode payload = payloadJson == null ? null : JsonUtil.readTree(payloadJson);
            UUID actorId = (UUID) rs.getObject("actor_id");
            return new StoredEvent(
                    rs.getObject("event_id", UUID.class),
                    rs.getObject("session_id", UUID.class),
                    rs.getLong("seq"),
                    EventType.valueOf(rs.getString("event_type")),
                    payload,
                    rs.getString("idempotency_key"),
                    actorId,
                    toInstant(rs.getTimestamp("occurred_at")));
        }
    }
}
