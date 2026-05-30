package com.realtimechat.projection.async;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Projection lag 메트릭(BR-7). 이벤트 occurred_at과 read model 반영 시각의 차이를 기록한다.
 * 외부 메트릭 라이브러리(Micrometer) 미도입 환경의 경량 기반 — Phase 3 관측성 고도화 시 교체.
 */
@Component
public class ProjectionLagMetrics {

    private static final Logger log = LoggerFactory.getLogger(ProjectionLagMetrics.class);

    private final AtomicLong lastLagMillis = new AtomicLong(0L);
    private final AtomicLong maxLagMillis = new AtomicLong(0L);
    private final AtomicLong dlqWriteFailures = new AtomicLong(0L);

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
    }

    public long lastLagMillis() { return lastLagMillis.get(); }
    public long maxLagMillis() { return maxLagMillis.get(); }
    public long dlqWriteFailures() { return dlqWriteFailures.get(); }
}
