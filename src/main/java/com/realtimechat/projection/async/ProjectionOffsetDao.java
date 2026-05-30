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

    /**
     * projection_offset의 현재 오프셋 상태를 담는 값 객체.
     *
     * @param lastAppliedSeq      마지막으로 적용된 seq
     * @param eventsSinceSnapshot 마지막 스냅샷 이후 누적 이벤트 수 (잔여 카운터)
     */
    public record Offset(long lastAppliedSeq, long eventsSinceSnapshot) {}

    /**
     * 단일 upsert로 행 잠금과 현재값 조회를 동시에 수행한다.
     *
     * <p>ON CONFLICT DO UPDATE(no-op self-update)는 충돌 행에 FOR UPDATE 등가의 행 잠금을 획득하고,
     * RETURNING이 INSERT/UPDATE 양쪽 모두에서 현재값을 반환한다. 신규 INSERT 시 DB 기본값(0, 0)을 반환한다.
     */
    private static final String LOCK_OR_INIT_SQL =
            "INSERT INTO projection_offset (session_id)"
                    + " VALUES (:sid)"
                    + " ON CONFLICT (session_id) DO UPDATE SET session_id = EXCLUDED.session_id"
                    + " RETURNING last_applied_seq, events_since_snapshot";

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
     * 단일 SQL로 행 잠금과 현재 오프셋 조회를 원자적으로 수행한다.
     *
     * <p>행이 없으면 INSERT(초기값 last_applied_seq=0, events_since_snapshot=0)하면서 잠금을 획득하고,
     * 행이 있으면 ON CONFLICT DO UPDATE로 충돌 행에 행 잠금을 획득한 뒤 현재값을 RETURNING으로 반환한다.
     * 기존 INSERT ON CONFLICT DO NOTHING + 별도 SELECT FOR UPDATE 2회 왕복과
     * 그 사이의 락 공백을 제거한다. 호출자 트랜잭션 내에서 실행되어야 한다.
     *
     * @param sessionId 대상 세션 ID
     * @return 현재 오프셋 상태 (행 최초 생성 시 lastAppliedSeq=0, eventsSinceSnapshot=0)
     */
    public Offset lockOrInit(UUID sessionId) {
        MapSqlParameterSource params = sidParams(sessionId);
        return jdbc.queryForObject(LOCK_OR_INIT_SQL, params,
                (rs, rowNum) -> new Offset(
                        rs.getLong("last_applied_seq"),
                        rs.getLong("events_since_snapshot")));
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
