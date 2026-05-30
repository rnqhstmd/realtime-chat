package com.realtimechat.async;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Redis Bean 설정(설계서 §4, §9).
 *
 * <p>Stream 연산(XADD/XREADGROUP)에 {@link StringRedisTemplate}을 사용한다.
 * Lettuce 자동구성된 {@link RedisConnectionFactory}를 주입받아 단순 위임한다.
 */
@Configuration
public class RedisConfig {

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory cf) {
        return new StringRedisTemplate(cf);
    }
}
