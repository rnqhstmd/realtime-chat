package com.realtimechat.presence;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code chat.presence.*} 설정 바인딩(설계서 §5-B, §7).
 *
 * <p>Spring Boot 3.x 생성자 바인딩(record)을 사용한다. relaxed binding 으로
 * {@code ttl-seconds} → {@code ttlSeconds},
 * {@code heartbeat-interval-seconds} → {@code heartbeatIntervalSeconds},
 * {@code expiry-poll-ms} → {@code expiryPollMs} 가 자동 매핑된다.
 *
 * <ul>
 *   <li>{@code ttlSeconds}: liveness 키 TTL(기본 90). heartbeat ping마다 SETEX로 갱신.</li>
 *   <li>{@code heartbeatIntervalSeconds}: 클라이언트 ping 주기 가이드(기본 30, 서버는 touch만).</li>
 *   <li>{@code expiryPollMs}: PresenceSweeper 폴링 주기(기본 5000).</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "chat.presence")
public record PresenceProps(
        long ttlSeconds,
        int heartbeatIntervalSeconds,
        long expiryPollMs) {

    public PresenceProps {
        if (ttlSeconds <= 0)                 ttlSeconds = 90L;
        if (heartbeatIntervalSeconds <= 0)   heartbeatIntervalSeconds = 30;
        if (expiryPollMs <= 0)               expiryPollMs = 5_000L;
    }
}
