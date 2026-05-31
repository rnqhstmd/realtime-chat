package com.realtimechat.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.realtimechat.command.CommandHandler;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventStore;
import com.realtimechat.event.EventType;
import com.realtimechat.event.payload.ParticipantJoinedPayload;
import com.realtimechat.presence.PresenceTracker;
import com.realtimechat.session.SessionService;
import com.realtimechat.support.AbstractIntegrationTest;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * presence TTL 만료·OFFLINE 자동 수집 통합 테스트(설계서 §5-B, FR-P3-2, BR-2, BR-3, AC-3/4/5).
 *
 * <p>실제 PostgreSQL + Redis Testcontainer 위에서 {@code PresenceTracker}(SETEX/SADD liveness),
 * {@code PresenceSweeper}(@Scheduled 폴링), {@code CommandHandler} OFFLINE 발행, 비동기
 * projection이 동작하는 {@code @SpringBootTest} 컨텍스트로 heartbeat→TTL 만료→OFFLINE 경로를
 * 검증한다. test 프로파일은 {@code chat.presence.ttl-seconds=3}, {@code expiry-poll-ms=500}이므로
 * 키 만료/sweep 감지를 테스트 시간 내(Awaitility 최대 10s)에 관찰할 수 있다.
 *
 * <p>각 테스트는 randomUUID 세션/참여자로 데이터를 격리한다(공유 컨테이너). OFFLINE 자동 전환을
 * 관찰하려면 join으로 read model presence=ONLINE 행을 먼저 만들고, heartbeat로 liveness 키를
 * 생성해야 한다(OFFLINE 적용은 기존 participant_view 행의 presence 컬럼을 갱신하기 때문).
 */
class PresenceTtlIntegrationTest extends AbstractIntegrationTest {

    @Autowired private SessionService sessionService;
    @Autowired private CommandHandler commandHandler;
    @Autowired private StringRedisTemplate redisTemplate;
    @Autowired private EventStore eventStore;
    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private PresenceTracker presenceTracker;
    @Autowired private TestRestTemplate rest;

    /** liveness 키({@code presence:{sid}:{pid}}). PresenceTracker와 동일한 스키마. */
    private static String presenceKey(UUID sessionId, UUID participantId) {
        return "presence:" + sessionId + ":" + participantId;
    }

    /** sweep 대상 Set 멤버({@code {sid}:{pid}}). */
    private static String member(UUID sessionId, UUID participantId) {
        return sessionId + ":" + participantId;
    }

    /** heartbeat REST 호출({@code POST /sessions/{id}/heartbeat}, body{participantId}). 204를 단언. */
    private void heartbeat(UUID sessionId, UUID participantId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> req =
                new HttpEntity<>(Map.of("participantId", participantId.toString()), headers);
        ResponseEntity<Void> resp =
                rest.postForEntity("/sessions/" + sessionId + "/heartbeat", req, Void.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    /**
     * join으로 read model에 presence=ONLINE 참여자 행을 만든다(비동기 projection 반영 대기).
     * OFFLINE 자동 전환은 이 행의 presence 컬럼을 갱신하므로 사전에 행이 존재해야 한다.
     */
    private void joinAndAwaitOnline(UUID sessionId, UUID participantId) {
        commandHandler.handle(sessionId, EventType.PARTICIPANT_JOINED,
                JsonUtil.toJsonNode(new ParticipantJoinedPayload(participantId)),
                "join-" + sessionId + "-" + participantId, participantId);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(presenceOf(sessionId, participantId)).isEqualTo("ONLINE"));
    }

    /** participant_view.presence 직접 조회. 행이 없으면 null. */
    private String presenceOf(UUID sessionId, UUID participantId) {
        return jdbcTemplate.query(
                "SELECT presence FROM participant_view WHERE session_id = ? AND participant_id = ?",
                rs -> rs.next() ? rs.getString("presence") : null,
                sessionId, participantId);
    }

    /** 이 세션의 PRESENCE_CHANGED(OFFLINE) 이벤트 건수(event 테이블 직접 조회). */
    private int offlineEventCount(UUID sessionId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM event WHERE session_id = ? AND event_type = 'PRESENCE_CHANGED' "
                        + "AND payload->>'presence' = 'OFFLINE'",
                Integer.class, sessionId);
        return count == null ? 0 : count;
    }

    // =====================================================================
    // AC-3: heartbeat ping → liveness 키 생성(TTL>0) → ttl(3s) 경과 후 자동 소멸
    // =====================================================================
    @Test
    @DisplayName("AC-3: heartbeat ping → liveness 키 TTL 생성, 갱신 중단 시 ttl 경과 후 키 소멸")
    void ac3_heartbeatCreatesKeyAndExpires() {
        UUID sessionId = sessionService.createSession();
        UUID participantId = UUID.randomUUID();

        heartbeat(sessionId, participantId);

        // heartbeat 직후 liveness 키가 살아있고 TTL(>0)이 설정됨.
        assertThat(presenceTracker.isAlive(sessionId, participantId)).isTrue();
        Long ttl = redisTemplate.getExpire(presenceKey(sessionId, participantId));
        assertThat(ttl).isGreaterThan(0L);

        // 갱신을 중단하면 ttl(3s) 경과 후 키가 사라진다(EXISTS false).
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(presenceTracker.isAlive(sessionId, participantId)).isFalse());
    }

    // =====================================================================
    // AC-4: 키 만료 → PresenceSweeper가 OFFLINE 1건 발행 + read model presence=OFFLINE 반영
    // =====================================================================
    @Test
    @DisplayName("AC-4: liveness 키 만료 → sweeper가 PRESENCE_CHANGED(OFFLINE) 발행 + participant_view OFFLINE")
    void ac4_expiryTriggersOfflineEventAndProjection() {
        UUID sessionId = sessionService.createSession();
        UUID participantId = UUID.randomUUID();

        // 참여자를 join시켜 presence=ONLINE 행을 만든다.
        joinAndAwaitOnline(sessionId, participantId);

        // heartbeat로 liveness 키 생성 → 이후 갱신 중단 → ttl(3s) 만료 → sweeper(poll 500ms)가 OFFLINE 발행.
        heartbeat(sessionId, participantId);
        assertThat(presenceTracker.isAlive(sessionId, participantId)).isTrue();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            // (a) event 테이블에 PRESENCE_CHANGED(OFFLINE) 1건 적재.
            assertThat(offlineEventCount(sessionId)).isEqualTo(1);
            // (b) 비동기 projection으로 participant_view.presence='OFFLINE' 반영.
            assertThat(presenceOf(sessionId, participantId)).isEqualTo("OFFLINE");
        });
    }

    // =====================================================================
    // AC-5(중복 미수집): OFFLINE 1회 발행 후 추가 sweep 주기가 지나도 정확히 1건 유지(SREM 원자 보장)
    // =====================================================================
    @Test
    @DisplayName("AC-5: 만료 OFFLINE 1회 발행 후 추가 sweep에도 PRESENCE_CHANGED(OFFLINE) 정확히 1건 유지")
    void ac5_offlineEmittedExactlyOnce() {
        UUID sessionId = sessionService.createSession();
        UUID participantId = UUID.randomUUID();

        joinAndAwaitOnline(sessionId, participantId);

        heartbeat(sessionId, participantId);

        // 먼저 OFFLINE 1건이 적재될 때까지 대기.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(offlineEventCount(sessionId)).isEqualTo(1));

        // 이후 추가로 충분히 기다려도(sweep poll 500ms가 여러 번 돌아도) OFFLINE 이벤트는 정확히 1건.
        // claimExpired의 SREM 원자 제거로 만료 멤버는 1회만 선점되어 중복 발행되지 않는다(BR-3).
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(4)).untilAsserted(() ->
                assertThat(offlineEventCount(sessionId)).isEqualTo(1));
    }

    // =====================================================================
    // AC-5(동시성, claimExpired): SADD한 멤버를 claimExpired 2회 호출 시 첫 true / 둘째 false
    // =====================================================================
    @Test
    @DisplayName("AC-5: claimExpired는 SREM 원자성으로 같은 멤버를 1회만 선점(첫 true, 둘째 false)")
    void ac5_claimExpiredAtomicSingleWinner() {
        UUID sessionId = UUID.randomUUID();
        UUID participantId = UUID.randomUUID();
        String member = member(sessionId, participantId);

        // 사전에 sweep 대상 Set에 멤버를 직접 등록(SADD).
        redisTemplate.opsForSet().add(PresenceTracker.TRACKED_SET, member);

        // 첫 claim은 제거에 성공(반환 true), 둘째 claim은 이미 제거됐으므로 실패(false).
        assertThat(presenceTracker.claimExpired(member)).isTrue();
        assertThat(presenceTracker.claimExpired(member)).isFalse();
    }
}
