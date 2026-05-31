package com.realtimechat.presence;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * presence liveness 추적(설계서 §5-B, FR-P3-2, BR-2, BR-3).
 *
 * <p>presence의 권위는 <b>Redis 키 존재 여부(liveness)</b>이며 read model이 아니다.
 * heartbeat는 이벤트가 아닌 경량 ping으로 {@link #touch}만 수행한다(SETEX + SADD).
 *
 * <p>키 스키마:
 * <ul>
 *   <li>liveness 키: {@code presence:{sessionId}:{participantId}}, TTL = {@code ttlSeconds}, 값=epoch-milli(디버그용).</li>
 *   <li>sweep 대상 Set: {@link #TRACKED_SET}, 멤버 = {@code {sessionId}:{participantId}}(TTL 없음, OFFLINE 처리 시 SREM 제거).</li>
 * </ul>
 *
 * <p><b>BR-3 중복 방지</b>: OFFLINE 발행 가드는 전적으로 {@link #claimExpired}의 SREM 원자 반환값으로 보장한다.
 * 반환값이 1이면 이 호출이 멤버 제거에 성공한 유일 주체이므로 그 주체만 OFFLINE을 발행한다(read model lag 무관).
 *
 * <p>{@code @Transactional} 없음: 호출자 TX 가정이며 Redis 연산은 TX와 무관하다.
 */
@Component
public class PresenceTracker {

    /** sweep 대상 멤버 집합(Redis Set) 키. 멤버 = {@code {sessionId}:{participantId}}. */
    public static final String TRACKED_SET = "presence:tracked";

    private final StringRedisTemplate redis;
    private final PresenceProps props;

    public PresenceTracker(StringRedisTemplate redis, PresenceProps props) {
        this.redis = redis;
        this.props = props;
    }

    /**
     * heartbeat ping 수신 시 liveness 갱신: SETEX로 TTL을 재설정하고 sweep 대상 Set에 등록(멱등).
     */
    public void touch(UUID sessionId, UUID participantId) {
        String key = presenceKey(sessionId, participantId);
        redis.opsForValue().set(key, Long.toString(System.currentTimeMillis()),
                Duration.ofSeconds(props.ttlSeconds()));
        redis.opsForSet().add(TRACKED_SET, member(sessionId, participantId));
    }

    /** liveness 키 존재 여부(EXISTS). 만료되었으면 false. */
    public boolean isAlive(UUID sessionId, UUID participantId) {
        return Boolean.TRUE.equals(redis.hasKey(presenceKey(sessionId, participantId)));
    }

    /** sweep 대상 멤버 전체(SMEMBERS). null 안전. */
    public Set<String> trackedMembers() {
        Set<String> members = redis.opsForSet().members(TRACKED_SET);
        return members == null ? Set.of() : members;
    }

    /**
     * BR-3 핵심: sweep 대상 Set에서 멤버를 원자적으로 제거(SREM)하고, 이 호출이 제거에 성공한 유일
     * 주체인지(반환값 1) 반환한다. 동시 sweep/disconnect 경쟁도 SREM 원자성으로 1회 수렴한다.
     */
    public boolean claimExpired(String member) {
        Long removed = redis.opsForSet().remove(TRACKED_SET, member);
        return removed != null && removed == 1L;
    }

    /** 멤버 문자열에서 sessionId 파싱(PresenceSweeper 사용). */
    public static UUID sessionIdOf(String member) {
        return UUID.fromString(member.substring(0, member.indexOf(':')));
    }

    /** 멤버 문자열에서 participantId 파싱(PresenceSweeper 사용). */
    public static UUID participantIdOf(String member) {
        return UUID.fromString(member.substring(member.indexOf(':') + 1));
    }

    private static String presenceKey(UUID sessionId, UUID participantId) {
        return "presence:" + sessionId + ":" + participantId;
    }

    private static String member(UUID sessionId, UUID participantId) {
        return sessionId + ":" + participantId;
    }
}
