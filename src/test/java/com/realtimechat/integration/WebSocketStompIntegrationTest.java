package com.realtimechat.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventType;
import com.realtimechat.realtime.StompEventController.WsEventMessage;
import com.realtimechat.session.SessionService;
import com.realtimechat.support.AbstractIntegrationTest;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

/**
 * WebSocket/STOMP 실시간 송수신 통합 테스트(설계서 §6, PRD R7 / 수용 기준 7).
 *
 * <p>{@link WebSocketStompClient}로 {@code ws://localhost:{port}/ws}에 접속해
 * {@code /topic/session.{id}}를 구독하고, STOMP inbound SEND({@code /app/session/{id}/events})로
 * MESSAGE_SENT를 보낸 뒤 broadcast 수신을 {@link CountDownLatch}(5초 타임아웃)로 검증한다.
 *
 * <p>발행 단일 경로(설계서 §6): inbound 수집 → CommandHandler afterCommit → SimpSessionBroadcaster.
 * 따라서 구독자는 자신이 보낸 메시지의 broadcast를 정확히 1회 수신한다.
 */
class WebSocketStompIntegrationTest extends AbstractIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private SessionService sessionService;

    private WebSocketStompClient newStompClient() {
        WebSocketStompClient client = new WebSocketStompClient(new StandardWebSocketClient());
        client.setMessageConverter(new MappingJackson2MessageConverter());
        return client;
    }

    @Test
    @DisplayName("AC7: STOMP 구독자가 같은 세션의 새 MESSAGE_SENT를 실시간 수신")
    void subscriberReceivesBroadcastMessage() throws Exception {
        // (1) 세션 생성: 동기 SessionService 빈 대신 REST 흐름과 무관하게 inbound 경로만 검증하기
        //     위해, 컨테이너 DB에 세션이 존재해야 하므로 SessionService로 생성한다.
        UUID sessionId = createSessionViaContext();

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

            // 구독 등록이 브로커에 반영될 시간을 잠시 준다.
            Thread.sleep(300);

            UUID sender = UUID.randomUUID();
            UUID messageId = UUID.randomUUID();
            WsEventMessage msg = new WsEventMessage(
                    EventType.MESSAGE_SENT,
                    JsonUtil.toJsonNode(Map.of(
                            "messageId", messageId.toString(),
                            "senderId", sender.toString(),
                            "content", "hello over stomp")),
                    sender,
                    "ws-key-" + messageId);

            session.send("/app/session/" + sessionId + "/events", msg);

            boolean got = latch.await(5, TimeUnit.SECONDS);
            assertThat(got)
                    .as("구독자가 5초 내 broadcast를 수신해야 한다")
                    .isTrue();
            assertThat(received.get()).isNotNull();
            assertThat(received.get().get("type")).isEqualTo(EventType.MESSAGE_SENT.name());
        } finally {
            session.disconnect();
            client.stop();
        }
    }

    /**
     * 컨테이너 DB에 세션 행을 만든다. inbound 수집(CommandHandler)이 세션 상태를 검증하므로
     * 사전에 ACTIVE 세션이 존재해야 한다. {@link SessionService}로 직접 생성한다.
     */
    private UUID createSessionViaContext() {
        return sessionService.createSession();
    }
}
