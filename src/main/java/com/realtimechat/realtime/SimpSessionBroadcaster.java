package com.realtimechat.realtime;

import com.realtimechat.event.StoredEvent;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * {@link SessionBroadcaster}의 Phase 1 STOMP 구현체(설계서 §6, §13).
 *
 * <p>커밋된 이벤트를 {@link SimpMessagingTemplate}를 통해 인메모리 SimpleBroker의
 * {@code /topic/session.{id}}로 직접 전달한다. 이 빈이 CommandHandler가 주입받는
 * {@link SessionBroadcaster}를 충족하여 런타임 발행 경로를 완성한다.
 *
 * <p><b>단일 발행 경로</b>: CommandHandler afterCommit → 이 구현체 → {@code /topic/session.{id}}.
 * REST {@code EventController}와 STOMP {@code StompEventController}는 모두 수집만 하고, 발행은
 * 이 경로 하나로만 일어나 중복 전달을 방지한다(설계서 §6).
 *
 * <p>다중 인스턴스 팬아웃(Redis Pub/Sub backplane)은 Phase 3 확장 항목이다(설계서 §7).
 *
 * <p>Phase 3 모드 스위치(설계서 §2-A, §5-A): {@code chat.fanout.mode=local}(기본,
 * {@code matchIfMissing})일 때만 이 빈이 등록되어 직접 STOMP 전달을 수행한다.
 * {@code redis} 모드에서는 {@link RedisPubSubSessionBroadcaster}가 대신 등록되어
 * publish만 수행한다(이중 전달 금지 불변식). 한 시점에 한 구현체만 등록되어 주입 모호성이 없다.
 */
@Component
@ConditionalOnProperty(name = "chat.fanout.mode", havingValue = "local", matchIfMissing = true)
public class SimpSessionBroadcaster implements SessionBroadcaster {

    /** 구독 토픽 prefix. {@code WebSocketConfig}의 SimpleBroker prefix(/topic)와 일관(설계서 §6). */
    private static final String TOPIC_PREFIX = "/topic/session.";

    private final SimpMessagingTemplate messagingTemplate;

    public SimpSessionBroadcaster(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    @Override
    public void broadcast(UUID sessionId, StoredEvent event) {
        messagingTemplate.convertAndSend(TOPIC_PREFIX + sessionId, EventBroadcast.from(event));
    }
}
