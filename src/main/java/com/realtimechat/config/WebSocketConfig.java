package com.realtimechat.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * STOMP over WebSocket 설정(설계서 §6, §11).
 *
 * <p>실시간 송수신의 전송 계층을 구성한다. 구독자는 {@code /topic/session.{id}}를 구독하고,
 * inbound SEND는 {@code /app} prefix로 들어와 {@code StompEventController}가 REST와 동일한
 * command handler로 수렴시킨다(단일 진입 로직 → 일관성, 설계서 §6, §11).
 *
 * <p>Phase 1은 인메모리 {@code SimpleBroker}로 충분하다. 다중 인스턴스 팬아웃을 위한 외부 브로커
 * /Redis relay는 Phase 3 확장 항목이다(설계서 §7, §13).
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        // 기본(raw WebSocket) 엔드포인트.
        registry.addEndpoint("/ws")
                // Phase 1 인증 없음; 인증 도입(Phase 2+) 시 실제 도메인으로 제한 필요(CSWSH 방지).
                .setAllowedOriginPatterns("*");
        // SockJS 폴백 엔드포인트(WebSocket 미지원 클라이언트 호환). 동일 경로 핸들러 매핑
        // 충돌을 피하기 위해 별도 경로(/ws-sockjs)로 분리한다.
        registry.addEndpoint("/ws-sockjs")
                // Phase 1 인증 없음; 인증 도입(Phase 2+) 시 실제 도메인으로 제한 필요(CSWSH 방지).
                .setAllowedOriginPatterns("*")
                .withSockJS();
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        // 구독 토픽 prefix: /topic/session.{id} (설계서 §6, §11). Phase 1 인메모리 SimpleBroker.
        registry.enableSimpleBroker("/topic");
        // inbound SEND prefix: 클라이언트는 /app/session/{id}/events 로 전송.
        registry.setApplicationDestinationPrefixes("/app");
    }
}
