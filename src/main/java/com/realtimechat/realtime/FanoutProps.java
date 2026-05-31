package com.realtimechat.realtime;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code chat.fanout.*} 설정 바인딩(설계서 §2-A, §5-A).
 *
 * <p>Spring Boot 3.x 생성자 바인딩(record)을 사용한다. relaxed binding 으로
 * {@code channel-prefix} → {@code channelPrefix} 가 자동 매핑된다.
 *
 * <p>{@code mode}는 팬아웃 백플레인 스위치다: 기본 {@code local}(직접 STOMP 전달),
 * 다중 인스턴스 프로파일에서 {@code redis}(Pub/Sub publish)로 명시 오버라이드한다.
 * 두 모드는 한 시점에 한 구현체만 빈으로 등록되어 이중 전달이 발생하지 않는다.
 *
 * <p>{@code @ConfigurationPropertiesScan}이 {@code RealtimeChatApplication}에 활성이므로
 * {@code StreamProps}와 동일하게 자동 스캔·바인딩된다.
 */
@ConfigurationProperties(prefix = "chat.fanout")
public record FanoutProps(
        String mode,
        String channelPrefix) {

    public FanoutProps {
        if (mode == null || mode.isBlank())                   mode = "local";
        if (channelPrefix == null || channelPrefix.isBlank()) channelPrefix = "chat.fanout.";
    }
}
