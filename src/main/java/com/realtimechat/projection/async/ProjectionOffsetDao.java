package com.realtimechat.projection.async;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

/**
 * projection_offset 테이블 접근 DAO (설계서 §10).
 *
 * <p>비동기 파이프라인에서 gap 감지·스냅샷 카운팅에 사용된다.
 * 모든 쓰기는 호출자(ProjectionApplier) 트랜잭션 내에서 실행된다.
 */
@Repository
public class ProjectionOffsetDao {

    /** 행이 없을 때 초기화(ON CONFLICT DO NOTHING으로 멱등 보장). */
    private static final String INSERT_IF_ABSENT_SQL =
            "INSERT INTO projection_offset (session_id)"
                    + " VALUES (:sid)"
                    + " ON CONFLICT (session_id) DO NOTHING";

    /** FOR UPDATE로 행을 잠그고 last_applied_seq 반환. */
    private static final String SELECT_FOR_UPDATE_SQL =
            "SELECT last_applied_seq FROM projection_offset"
                    + " WHERE session_id = :sid FOR UPDATE";

    /** events_since_snapshot 현재값 조회(행은 selectForUpdateOrInit의 FOR UPDATE로 같은 TX에서 잠겨 있음). */
    private static final String SNAPSHOT_COUNTER_SQL =
            "SELECT events_since_snapshot FROM projection_offset"
                    + " WHERE session_id = :sid";

    /** last_applied_seq와 events_since_snapshot을 절대값으로 SET. */
    private static final String UPDATE_OFFSET_SQL =
            "UPDATE projection_offset"
                    + "   SET last_applied_seq      = :seq,"
                    + "       events_since_snapshot = :since,"
                    + "       updated_at             = now()"
                    + " WHERE session_id = :sid";

    private final NamedParameterJdbcTemplate jdbc;

    public ProjectionOffsetDao(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 세션의 last_applied_seq를 FOR UPDATE로 잠근 뒤 반환한다.
     *
     * <p>행이 없으면 INSERT(초기값 last_applied_seq=0, events_since_snapshot=0)한 후 0을 반환한다.
     * 호출자 트랜잭션 내에서 실행되어야 한다.
     *
     * @param sessionId 대상 세션 ID
     * @return 현재 last_applied_seq (행 최초 생성 시 0)
     */
    public long selectForUpdateOrInit(UUID sessionId) {
        MapSqlParameterSource params = sidParams(sessionId);
        jdbc.update(INSERT_IF_ABSENT_SQL, params);
        Long seq = jdbc.queryForObject(SELECT_FOR_UPDATE_SQL, params, Long.class);
        return seq != null ? seq : 0L;
    }

    /**
     * 세션의 현재 events_since_snapshot 카운터 값을 조회한다.
     *
     * <p>행은 {@link #selectForUpdateOrInit}의 FOR UPDATE로 같은 트랜잭션에서 이미 잠겨 있으므로
     * 일반 SELECT로 현재값을 조회한다. 항상 {@code [0, triggerInterval)} 범위의 잔여 건수다.
     *
     * @param sessionId 대상 세션 ID
     * @return 현재 events_since_snapshot (행/값이 없으면 0)
     */
    public long snapshotCounter(UUID sessionId) {
        Long since = jdbc.queryForObject(SNAPSHOT_COUNTER_SQL, sidParams(sessionId), Long.class);
        return since != null ? since : 0L;
    }

    /**
     * last_applied_seq와 events_since_snapshot을 절대값으로 갱신한다.
     *
     * <p>누적(+=)이 아닌 절대값 SET이므로, 호출자(ProjectionApplier)가 스냅샷 트리거 시점을
     * 적용 범위 내에서 직접 계산한 뒤 마지막 스냅샷 이후 잔여 건수를 그대로 기록한다.
     *
     * @param sessionId      대상 세션 ID
     * @param lastAppliedSeq last_applied_seq로 설정할 값(이번 incoming의 seq)
     * @param sinceSnapshot  events_since_snapshot으로 설정할 잔여 건수
     */
    public void updateOffset(UUID sessionId, long lastAppliedSeq, long sinceSnapshot) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("sid", sessionId)
                .addValue("seq", lastAppliedSeq)
                .addValue("since", sinceSnapshot);
        jdbc.update(UPDATE_OFFSET_SQL, params);
    }

    private static MapSqlParameterSource sidParams(UUID sessionId) {
        return new MapSqlParameterSource().addValue("sid", sessionId);
    }
}
