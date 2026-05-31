package com.realtimechat.realtime;

import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.StoredEvent;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * {@link SessionBroadcaster}의 Phase 3 Redis Pub/Sub 구현체(설계서 §2-A, §5-A).
 *
 * <p>{@code chat.fanout.mode=redis}일 때만 등록되어 {@link SimpSessionBroadcaster}를 대체한다.
 * 커밋된 이벤트를 {@code chat.fanout.{sessionId}} 채널로 publish만 하고, 로컬 STOMP 전달은
 * 하지 않는다(<b>이중 전달 금지 불변식</b>). 다중 인스턴스는 각자 {@link SessionFanoutListener}로
 * 이 채널을 구독하며, publish 노드 자신도 자기 구독 echo 경로로만 로컬 전달하여 노드 대칭·중복 없음을 보장한다.
 *
 * <p>채널·페이로드는 {@code "chat.fanout." + sessionId}와 {@link EventBroadcast}(JSON)이며,
 * 구독측이 동일 {@code EventBroadcast}로 복원해 STOMP {@code /topic/session.{id}}로 전달한다 —
 * Pub/Sub·STOMP 페이로드 형태가 일치한다.
 *
 * <p>publish 실패는 진실의 원천(Event Store)을 오염시키지 않으므로 WARN 로그 후 무시한다(BR-7).
 */
@Component
@ConditionalOnProperty(name = "chat.fanout.mode", havingValue = "redis")
public class RedisPubSubSessionBroadcaster implements SessionBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(RedisPubSubSessionBroadcaster.class);

    private final StringRedisTemplate stringRedisTemplate;
    private final FanoutProps fanoutProps;

    public RedisPubSubSessionBroadcaster(StringRedisTemplate stringRedisTemplate, FanoutProps fanoutProps) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.fanoutProps = fanoutProps;
    }

    @Override
    public void broadcast(UUID sessionId, StoredEvent event) {
        String channel = fanoutProps.channelPrefix() + sessionId;
        String json = JsonUtil.toJson(EventBroadcast.from(event));
        try {
            stringRedisTemplate.convertAndSend(channel, json);
        } catch (RuntimeException e) {
            log.warn("fanout publish 실패(무시): sid={}, seq={}", sessionId, event.seq(), e);
        }
    }
}
