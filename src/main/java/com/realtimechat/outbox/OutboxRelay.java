package com.realtimechat.outbox;

import com.realtimechat.async.EventStreamCodec;
import com.realtimechat.async.StreamProps;
import com.realtimechat.event.StoredEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Outbox 릴레이 (설계서 §8, FR-P2-3, BR-4).
 *
 * <p>폴링 주기마다 미발행 레코드를 조회하여 Redis Stream에 XADD한 뒤,
 * 성공한 레코드만 markPublished로 표시한다(XADD-then-markPublished, 유실<중복).
 * Redis 불가 시 해당 레코드를 건너뛰고 WARN 로그를 남기며,
 * 다음 폴링에서 재시도한다(BR-4).
 *
 * <p>@Transactional 없음: at-least-once, 중복은 소비측 seq-guard가 흡수.
 * @EnableScheduling 활성화는 13단계에서 수행한다.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxDao outboxDao;
    private final EventStreamCodec codec;
    private final StreamProps props;
    private final StringRedisTemplate stringRedisTemplate;

    @Value("${chat.outbox.relay.batch-size:100}")
    private int batchSize;

    public OutboxRelay(OutboxDao outboxDao,
                       EventStreamCodec codec,
                       StreamProps props,
                       StringRedisTemplate stringRedisTemplate) {
        this.outboxDao = outboxDao;
        this.codec = codec;
        this.props = props;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    /**
     * 미발행 레코드를 Redis Stream에 릴레이한다.
     *
     * <p>각 레코드를 {@link StoredEvent}로 변환 후 {@link EventStreamCodec#toFields}로
     * 직렬화하여 XADD한다. 성공분만 markPublished 처리하며, 배치 완료 후 MAXLEN trim을 수행한다.
     */
    @Scheduled(fixedDelayString = "${chat.outbox.relay.poll-interval-ms:500}")
    public void relay() {
        int batch = batchSize;
        List<OutboxRecord> records = outboxDao.findUnpublished(batch);
        if (records.isEmpty()) {
            return;
        }

        List<Long> successIds = new ArrayList<>(records.size());

        for (OutboxRecord record : records) {
            StoredEvent stored = toStoredEvent(record);
            Map<String, String> fields = codec.toFields(stored);
            try {
                stringRedisTemplate.opsForStream()
                        .add(StreamRecords.mapBacked(fields).withStreamKey(props.stream()));
                successIds.add(record.id());
            } catch (Exception e) {
                log.warn("OutboxRelay: XADD 실패 — record.id={} eventId={} cause={}",
                        record.id(), record.eventId(), e.getMessage());
            }
        }

        if (!successIds.isEmpty()) {
            outboxDao.markPublished(successIds);
        }

        try {
            stringRedisTemplate.opsForStream()
                    .trim(props.stream(), props.streamMaxlen(), true);
        } catch (Exception e) {
            log.warn("OutboxRelay: stream trim 실패 — stream={} cause={}",
                    props.stream(), e.getMessage());
        }
    }

    /**
     * {@link OutboxRecord} → {@link StoredEvent} 1:1 매핑.
     *
     * <p>OutboxRecord.eventType() 은 StoredEvent.type() 에 대응한다.
     */
    private static StoredEvent toStoredEvent(OutboxRecord r) {
        return new StoredEvent(
                r.eventId(),
                r.sessionId(),
                r.seq(),
                r.eventType(),
                r.payload(),
                r.idempotencyKey(),
                r.actorId(),
                r.occurredAt());
    }
}
