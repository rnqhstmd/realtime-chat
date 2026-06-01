package com.realtimechat.async;

import com.realtimechat.realtime.FanoutProps;
import com.realtimechat.realtime.SessionFanoutListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Redis Bean 설정(설계서 §4, §9, §5-A).
 *
 * <p>Stream 연산(XADD/XREADGROUP)에 {@link StringRedisTemplate}을 사용한다.
 * Lettuce 자동구성된 {@link RedisConnectionFactory}를 주입받아 단순 위임한다.
 *
 * <p>{@code chat.fanout.mode=redis}일 때만 팬아웃 구독 컨테이너를 등록한다(설계서 §2-A, §5-A).
 */
@Configuration
public class RedisConfig {

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory cf) {
        return new StringRedisTemplate(cf);
    }

    /**
     * 팬아웃 채널({@code chat.fanout.*}) 구독 컨테이너(설계서 §5-A) — redis 모드만.
     *
     * <p>{@code PatternTopic(channelPrefix + "*")} 단일 구독으로 모든 세션 채널을 받는다.
     * BR-1 세션 격리는 채널명 sessionId로 자연 보장되며, {@link StringRedisTemplate}와 동일한
     * {@link RedisConnectionFactory}를 공유한다.
     */
    @Bean
    @ConditionalOnProperty(name = "chat.fanout.mode", havingValue = "redis")
    RedisMessageListenerContainer fanoutContainer(
            RedisConnectionFactory connectionFactory,
            SessionFanoutListener listener,
            FanoutProps props) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(listener, new PatternTopic(props.channelPrefix() + "*"));
        return container;
    }
}
