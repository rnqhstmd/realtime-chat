package com.realtimechat.realtime;

import com.realtimechat.common.json.JsonUtil;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Redis Pub/Sub 팬아웃 채널 구독자(설계서 §2-A, §5-A) — {@code chat.fanout.mode=redis}에서만 등록.
 *
 * <p>{@code RedisMessageListenerContainer}가 {@code PatternTopic("chat.fanout.*")}로 라우팅한
 * 메시지를 받아, 채널명에서 sessionId를 파싱하고 본문을 {@link EventBroadcast}로 복원한 뒤
 * STOMP {@code /topic/session.{id}}로 로컬 구독자에게 전달한다. 이로써 publish 노드와 구독 노드가
 * 동일 경로로 로컬 전달하여 노드 대칭을 이룬다.
 *
 * <p>한 메시지의 역직렬화·전달 실패가 컨테이너를 죽이지 않도록 WARN 로그 후 무시한다.
 */
@Component
@ConditionalOnProperty(name = "chat.fanout.mode", havingValue = "redis")
public class SessionFanoutListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(SessionFanoutListener.class);

    /** {@link SimpSessionBroadcaster}와 동일한 구독 토픽 prefix(설계서 §6). */
    private static final String TOPIC_PREFIX = "/topic/session.";

    private final SimpMessagingTemplate messagingTemplate;
    private final FanoutProps fanoutProps;

    public SessionFanoutListener(SimpMessagingTemplate messagingTemplate, FanoutProps fanoutProps) {
        this.messagingTemplate = messagingTemplate;
        this.fanoutProps = fanoutProps;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String channel = new String(message.getChannel(), StandardCharsets.UTF_8);
        try {
            String sessionId = channel.substring(fanoutProps.channelPrefix().length());
            // chat.fanout.* 임의 채널 publish 시 비-UUID sessionId가 /topic/session.{임의}로
            // 전달되는 것을 방어한다. 정상 경로는 chat.fanout.{UUID}이므로 항상 통과한다.
            UUID.fromString(sessionId);
            String body = new String(message.getBody(), StandardCharsets.UTF_8);
            EventBroadcast broadcast = JsonUtil.mapper().readValue(body, EventBroadcast.class);
            messagingTemplate.convertAndSend(TOPIC_PREFIX + sessionId, broadcast);
        } catch (IllegalArgumentException e) {
            log.warn("fanout 수신 처리 실패(비-UUID sessionId 무시): channel={}", channel);
        } catch (Exception e) {
            log.warn("fanout 수신 처리 실패(무시): channel={}", channel, e);
        }
    }
}
