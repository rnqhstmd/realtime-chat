package com.realtimechat.async;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code chat.redis.*} 설정 바인딩.
 *
 * <p>Spring Boot 3.x 생성자 바인딩(record)을 사용한다. relaxed binding 으로
 * {@code dlq-stream} → {@code dlqStream}, {@code stream-maxlen} → {@code streamMaxlen}
 * 이 자동 매핑된다.
 *
 * <p>{@code @ConfigurationPropertiesScan} 활성화는 애플리케이션 설정 단계(13단계)에서 수행한다.
 */
@ConfigurationProperties(prefix = "chat.redis")
public record StreamProps(
        String stream,
        String group,
        String dlqStream,
        String consumer,
        int maxAttempts,
        long claimMinIdleMs,
        long readBlockMs,
        int readCount,
        long streamMaxlen) {

    public StreamProps {
        if (stream == null || stream.isBlank())     stream = "events";
        if (group == null || group.isBlank())       group = "proj";
        if (dlqStream == null || dlqStream.isBlank()) dlqStream = "events:dlq";
        if (maxAttempts <= 0)                       maxAttempts = 3;
        if (claimMinIdleMs <= 0)                    claimMinIdleMs = 30_000L;
        if (readBlockMs <= 0)                       readBlockMs = 2_000L;
        if (readCount <= 0)                         readCount = 50;
        if (streamMaxlen <= 0)                      streamMaxlen = 100_000L;
    }
}
