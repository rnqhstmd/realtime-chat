package com.realtimechat.session;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * 쓰기 측 {@code session} 테이블 접근 DAO (설계서 §3.1).
 *
 * <p>{@code session}은 세션의 권위 상태(ACTIVE/ENDED)와 seq 채번 카운터(last_seq)를 보유한다.
 * seq 채번(last_seq 증가)은 append 핫패스라 {@code EventStore}가 직접 수행하고, 이 DAO는
 * 세션 행의 생성/상태조회/종료만 담당한다. 모든 메서드는 호출자(SessionService/CommandHandler)의
 * 트랜잭션 안에서 실행된다고 가정한다.
 */
@Repository
public class SessionDao {

    /** 세션 생성. last_seq=0으로 시작하여 이후 append가 1부터 채번한다(§3.1, §10 Q1). */
    private static final String CREATE_SQL =
            "INSERT INTO session (id, status, last_seq, created_at) "
                    + "VALUES (:id, 'ACTIVE', 0, :createdAt)";

    private static final String EXISTS_SQL =
            "SELECT EXISTS(SELECT 1 FROM session WHERE id = :id)";

    private static final String STATUS_SQL =
            "SELECT status FROM session WHERE id = :id";

    private static final String MARK_ENDED_SQL =
            "UPDATE session SET status = 'ENDED', ended_at = :endedAt WHERE id = :id";

    private final NamedParameterJdbcTemplate jdbc;

    public SessionDao(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void create(UUID id, Instant createdAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("createdAt", createdAt == null ? null : Timestamp.from(createdAt));
        jdbc.update(CREATE_SQL, params);
    }

    public boolean exists(UUID id) {
        Boolean result = jdbc.queryForObject(EXISTS_SQL, idParams(id), Boolean.class);
        return Boolean.TRUE.equals(result);
    }

    /** 세션 상태(ACTIVE/ENDED). 세션이 없으면 비어 있는 Optional. */
    public Optional<String> status(UUID id) {
        List<String> rows = jdbc.queryForList(STATUS_SQL, idParams(id), String.class);
        return rows.stream().findFirst();
    }

    public void markEnded(UUID id, Instant endedAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("endedAt", endedAt == null ? null : Timestamp.from(endedAt));
        jdbc.update(MARK_ENDED_SQL, params);
    }

    private static MapSqlParameterSource idParams(UUID id) {
        return new MapSqlParameterSource().addValue("id", id);
    }
}
