package com.realtimechat.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.command.CommandHandler;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventType;
import com.realtimechat.event.payload.MessageSentPayload;
import com.realtimechat.realtime.EventBroadcast;
import com.realtimechat.session.SessionService;
import com.realtimechat.support.AbstractIntegrationTest;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

/**
 * Redis Pub/Sub 팬아웃 백플레인 통합 테스트(설계서 §2-A, §5-A, FR-P3-1, BR-7, AC-1/2).
 *
 * <p>클래스 단위로 {@code chat.fanout.mode=redis}를 오버라이드하여 {@code RedisPubSubSessionBroadcaster}
 * + {@code SessionFanoutListener} + {@code RedisMessageListenerContainer}(fanout 구독)이 활성인
 * 별도 컨텍스트로 부팅한다. 이벤트 커밋 후 broadcast가 {@code chat.fanout.{sid}} 채널로 publish →
 * listener가 수신 → 로컬 STOMP {@code /topic/session.{id}}로 전달하는 redis 백플레인 경로를 검증한다.
 *
 * <p><b>AC-1 한계</b>: 단일 JVM 테스트이므로 진짜 2-인스턴스 팬아웃이 아니라 redis 백플레인 동작
 * (publish→listener 수신→로컬 STOMP 전달, 즉 publish 노드의 자기 echo)과 채널 격리만 검증한다.
 * 진짜 다중 노드(노드 A publish → 노드 B 로컬 구독자 수신) 검증은 수동 데모로 남긴다.
 */
@TestPropertySource(properties = "chat.fanout.mode=redis")
class FanoutRedisIntegrationTest extends AbstractIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired private SessionService sessionService;
    @Autowired private CommandHandler commandHandler;
    @Autowired private StringRedisTemplate redisTemplate;

    private WebSocketStompClient newStompClient() {
        WebSocketStompClient client = new WebSocketStompClient(new StandardWebSocketClient());
        client.setMessageConverter(new MappingJackson2MessageConverter());
        return client;
    }

    // =====================================================================
    // AC-1: redis 모드에서 broadcast publish → listener 수신 → 로컬 STOMP 전달 → 구독자 1회 수신
    // =====================================================================
    @Test
    @DisplayName("AC-1: redis 팬아웃 — 이벤트 발생 시 publish→listener→로컬 STOMP로 구독자가 MESSAGE_SENT 수신")
    void ac1_redisBackplaneDeliversToSubscriber() throws Exception {
        UUID sessionId = sessionService.createSession();

        WebSocketStompClient client = newStompClient();
        String url = "ws://localhost:" + port + "/ws";

        StompSession session = client
                .connectAsync(url, new StompSessionHandlerAdapter() { })
                .get(5, TimeUnit.SECONDS);

        try {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Map<String, Object>> received = new AtomicReference<>();

            session.subscribe("/topic/session." + sessionId, new StompFrameHandler() {
                @Override
                public Type getPayloadType(StompHeaders headers) {
                    return Map.class;
                }

                @Override
                @SuppressWarnings("unchecked")
                public void handleFrame(StompHeaders headers, Object payload) {
                    received.set((Map<String, Object>) payload);
                    latch.countDown();
                }
            });

            // 구독 등록 + redis 채널 구독이 반영될 시간을 잠시 준다.
            Thread.sleep(500);

            // 같은 세션에 이벤트 발생 → CommandHandler afterCommit broadcast가 redis로 publish된다.
            UUID sender = UUID.randomUUID();
            commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                    JsonUtil.toJsonNode(new MessageSentPayload(UUID.randomUUID(), sender, "hello-redis-fanout")),
                    "fanout-ac1-key", sender);

            boolean got = latch.await(10, TimeUnit.SECONDS);
            assertThat(got)
                    .as("redis 백플레인 경유로 구독자가 10초 내 broadcast를 수신해야 한다")
                    .isTrue();
            assertThat(received.get()).isNotNull();
            assertThat(received.get().get("type")).isEqualTo(EventType.MESSAGE_SENT.name());
        } finally {
            session.disconnect();
            client.stop();
        }
    }

    // =====================================================================
    // AC-2: 채널 격리 — 세션 A 구독자는 세션 B 채널에 직접 publish된 메시지를 수신하지 않는다
    // =====================================================================
    @Test
    @DisplayName("AC-2: 채널 격리 — 세션 B 채널에 직접 publish해도 세션 A 구독자는 수신하지 않음")
    void ac2_channelIsolation() throws Exception {
        UUID sessionA = sessionService.createSession();
        UUID sessionB = sessionService.createSession();

        WebSocketStompClient client = newStompClient();
        String url = "ws://localhost:" + port + "/ws";

        StompSession session = client
                .connectAsync(url, new StompSessionHandlerAdapter() { })
                .get(5, TimeUnit.SECONDS);

        try {
            CountDownLatch latch = new CountDownLatch(1);

            // 세션 A만 구독한다.
            session.subscribe("/topic/session." + sessionA, new StompFrameHandler() {
                @Override
                public Type getPayloadType(StompHeaders headers) {
                    return Map.class;
                }

                @Override
                public void handleFrame(StompHeaders headers, Object payload) {
                    latch.countDown();
                }
            });

            // 구독 + redis 채널 구독 반영 대기.
            Thread.sleep(500);

            // 세션 B 채널(chat.fanout.{B})에 유효한 EventBroadcast JSON을 직접 publish한다.
            // EventBroadcast는 RedisPubSubSessionBroadcaster가 publish하는 실제 페이로드 구조다.
            JsonNode payload = JsonUtil.toJsonNode(
                    new MessageSentPayload(UUID.randomUUID(), UUID.randomUUID(), "for-session-b"));
            EventBroadcast broadcastB = new EventBroadcast(
                    UUID.randomUUID(), sessionB, 1L, EventType.MESSAGE_SENT, payload, Instant.now());
            redisTemplate.convertAndSend("chat.fanout." + sessionB, JsonUtil.toJson(broadcastB));

            // 세션 A 구독자는 세션 B 채널 메시지를 수신하지 않아야 한다(짧은 타임아웃 내 count down 안 됨).
            boolean got = latch.await(2, TimeUnit.SECONDS);
            assertThat(got)
                    .as("세션 A 구독자는 세션 B 채널 메시지를 수신하면 안 된다(채널 격리)")
                    .isFalse();
        } finally {
            session.disconnect();
            client.stop();
        }
    }
}
