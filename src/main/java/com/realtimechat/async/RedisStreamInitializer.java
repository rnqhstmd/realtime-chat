package com.realtimechat.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.stereotype.Component;

/**
 * 애플리케이션 기동 시 Redis Stream consumer group을 생성한다(설계서 §4, §9).
 *
 * <p>XGROUP CREATE ... MKSTREAM 에 해당하는 초기화를 수행한다.
 * 스트림이 존재하지 않으면 createGroup 호출 시 자동 생성(MKSTREAM 효과)된다.
 * 그룹이 이미 존재하면(BUSYGROUP) 예외를 무시한다.
 *
 * <p>DLQ 스트림({@code props.dlqStream()})은 첫 XADD 시 자동 생성되므로 별도 초기화 불요.
 */
@Component
public class RedisStreamInitializer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RedisStreamInitializer.class);

    private final StringRedisTemplate redisTemplate;
    private final StreamProps props;

    public RedisStreamInitializer(StringRedisTemplate redisTemplate, StreamProps props) {
        this.redisTemplate = redisTemplate;
        this.props = props;
    }

    @Override
    public void run(ApplicationArguments args) {
        createGroupIfAbsent(props.stream(), props.group());
    }

    private void createGroupIfAbsent(String stream, String group) {
        try {
            redisTemplate.opsForStream().createGroup(stream, ReadOffset.from("0"), group);
            log.info("Redis Stream consumer group 생성 완료: stream={}, group={}", stream, group);
        } catch (RedisSystemException | InvalidDataAccessApiUsageException e) {
            if (isBusyGroup(e)) {
                log.debug("Redis Stream consumer group 이미 존재(무시): stream={}, group={}", stream, group);
            } else {
                log.warn("Redis Stream consumer group 생성 중 예외 발생: stream={}, group={}", stream, group, e);
                throw e;
            }
        }
    }

    /**
     * BUSYGROUP(그룹 이미 존재) 여부를 cause 체인 전체에서 판정한다.
     * Spring Data Redis는 Lettuce {@code RedisBusyException}을 {@code RedisSystemException}으로 감싸므로
     * "BUSYGROUP" 문자열이 최상위가 아닌 cause 메시지에 위치할 수 있다.
     */
    private static boolean isBusyGroup(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            String msg = t.getMessage();
            if (msg != null && msg.contains("BUSYGROUP")) {
                return true;
            }
        }
        return false;
    }
}
