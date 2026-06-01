package com.realtimechat.realtime;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
import com.realtimechat.event.payload.MessageSentPayload;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * {@link RedisPubSubSessionBroadcaster} 단위 테스트(설계서 §5-A, BR-7, AC-14).
 *
 * <p>publish 실패 격리(BR-7)는 Spring 컨텍스트 없이 Mockito 단위 테스트로 검증한다.
 * {@code StringRedisTemplate}를 전역 SpyBean으로 모킹하면 공유 통합 컨텍스트의 redis 연산이
 * 깨지므로, 여기서는 plain mock으로 격리해 검증한다. {@code FanoutProps}는 실제 record를
 * 생성해 {@code channelPrefix()}가 "chat.fanout."를 반환하도록 한다(생성자 바인딩 record).
 */
class RedisPubSubSessionBroadcasterTest {

    /** EventBroadcast.from이 읽는 모든 필드를 채운 최소 StoredEvent. */
    private static StoredEvent storedEvent() {
        UUID sender = UUID.randomUUID();
        JsonNode payload = JsonUtil.toJsonNode(
                new MessageSentPayload(UUID.randomUUID(), sender, "br7-body"));
        return new StoredEvent(
                UUID.randomUUID(),       // eventId
                UUID.randomUUID(),       // sessionId
                1L,                      // seq
                EventType.MESSAGE_SENT,  // type
                payload,                 // payload
                "br7-idem-key",          // idempotencyKey
                sender,                  // actorId
                Instant.now());          // occurredAt
    }

    // =====================================================================
    // AC-14: convertAndSend가 예외를 던져도 broadcast는 예외를 전파하지 않는다(BR-7 swallow)
    // =====================================================================
    @Test
    @DisplayName("AC-14: redis publish 실패(예외) 시 broadcast는 예외를 삼키고 전파하지 않음(BR-7)")
    void ac14_broadcastSwallowsPublishFailure() {
        StringRedisTemplate stringRedisTemplate = mock(StringRedisTemplate.class);
        // mode=redis, channelPrefix는 실제 값("chat.fanout.")으로 바인딩.
        FanoutProps fanoutProps = new FanoutProps("redis", "chat.fanout.");

        // publish가 항상 실패하도록 설정(redis 장애 시뮬레이션).
        doThrow(new RuntimeException("redis down"))
                .when(stringRedisTemplate).convertAndSend(anyString(), anyString());

        RedisPubSubSessionBroadcaster broadcaster =
                new RedisPubSubSessionBroadcaster(stringRedisTemplate, fanoutProps);

        StoredEvent event = storedEvent();

        // BR-7: publish 실패는 진실의 원천을 오염시키지 않으므로 WARN 후 무시 → 호출자에게 예외 전파 없음.
        assertThatCode(() -> broadcaster.broadcast(event.sessionId(), event))
                .doesNotThrowAnyException();
    }
}
