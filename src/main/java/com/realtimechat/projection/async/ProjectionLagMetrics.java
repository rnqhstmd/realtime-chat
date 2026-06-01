package com.realtimechat.projection.async;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Projection lag 메트릭(BR-7) + 비동기 파이프라인 관측성 메트릭(설계서 §5-D, BR-6).
 *
 * <p>이벤트 occurred_at과 read model 반영 시각의 차이(lag)를 기록하고, Micrometer
 * {@link MeterRegistry}에 Gauge/Counter로 노출한다. lag 값은 기존 {@link AtomicLong}에
 * 보관하고 그 스냅샷을 Gauge 소스로 등록하므로(값 보관은 AtomicLong 유지) 기존 getter/호출부는
 * 무변경이다.
 *
 * <p>BR-6: 카운터/게이지는 모두 인메모리 기반이라 애플리케이션 재시작 시 0부터 시작한다
 * (영속화하지 않음). 이벤트 자체의 진실의 원천은 event store다.
 */
@Component
public class ProjectionLagMetrics {

    private static final Logger log = LoggerFactory.getLogger(ProjectionLagMetrics.class);

    private final AtomicLong lastLagMillis = new AtomicLong(0L);
    private final AtomicLong maxLagMillis = new AtomicLong(0L);
    private final AtomicLong dlqWriteFailures = new AtomicLong(0L);

    private final Counter dlqFailuresCounter;
    private final Counter streamProcessedCounter;
    private final Counter outboxRelayedCounter;

    public ProjectionLagMetrics(MeterRegistry registry) {
        // lag 게이지: 값은 AtomicLong에 보관하고 스냅샷을 Gauge 소스로 노출한다(state 태그로 구분).
        Gauge.builder("chat.projection.lag.millis", lastLagMillis, AtomicLong::get)
                .tag("state", "last")
                .register(registry);
        Gauge.builder("chat.projection.lag.millis", maxLagMillis, AtomicLong::get)
                .tag("state", "max")
                .register(registry);

        this.dlqFailuresCounter = Counter.builder("chat.projection.dlq.failures").register(registry);
        this.streamProcessedCounter = Counter.builder("chat.stream.processed").register(registry);
        this.outboxRelayedCounter = Counter.builder("chat.outbox.relayed").register(registry);
    }

    /** 이벤트가 read model에 반영된 시점에 호출. lag = now - occurredAt. */
    public void record(Instant occurredAt, Instant appliedAt) {
        if (occurredAt == null || appliedAt == null) {
            return;
        }
        long lag = Duration.between(occurredAt, appliedAt).toMillis();
        if (lag < 0L) {
            lag = 0L;
        }
        lastLagMillis.set(lag);
        maxLagMillis.accumulateAndGet(lag, Math::max);
        log.debug("projection lag: {} ms (occurredAt={}, appliedAt={})", lag, occurredAt, appliedAt);
    }

    /** DLQ XADD 실패 시 호출. 운영 알림(Phase 3 관측성)의 기반 카운터. */
    public void recordDlqWriteFailure() {
        dlqWriteFailures.incrementAndGet();
        dlqFailuresCounter.increment();
    }

    /** 스트림 이벤트 1건 처리 완료 시 호출(처리량 관측). */
    public void recordStreamProcessed() {
        streamProcessedCounter.increment();
    }

    /** Outbox 릴레이로 Redis Stream에 발행된 레코드 수만큼 증가(릴레이 처리량 관측). */
    public void recordOutboxRelayed(long n) {
        outboxRelayedCounter.increment(n);
    }

    public long lastLagMillis() { return lastLagMillis.get(); }
    public long maxLagMillis() { return maxLagMillis.get(); }
    public long dlqWriteFailures() { return dlqWriteFailures.get(); }
}
