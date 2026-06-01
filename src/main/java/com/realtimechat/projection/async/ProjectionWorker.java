package com.realtimechat.projection.async;

import com.realtimechat.async.EventStreamCodec;
import com.realtimechat.async.StreamConstants;
import com.realtimechat.async.StreamProps;
import com.realtimechat.event.StoredEvent;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * 비동기 파이프라인의 단일 소비 루프(설계서 §9; Q1, FR-P2-4~7, BR-2, BR-3, AC-5/6).
 *
 * <p><b>수동 XREADGROUP 단일 루프</b>로 신규 소비·재청구·DLQ를 한 컴포넌트에서 순차 제어한다.
 * {@code StreamMessageListenerContainer}를 쓰지 않는 이유는 소비와 재청구가 별도 스레드에서
 * 동시에 도는 2-track race를 차단하기 위함이다(Q1). 한 스레드가 1) 신규 소비, 2) idle pending
 * 재청구/DLQ를 순서대로 수행하므로 같은 메시지가 두 경로에서 동시에 처리될 수 없다.
 *
 * <p>{@link SmartLifecycle}로 라이프사이클을 제어한다. {@code getPhase()}가 큰 값을 반환해
 * 다른 컴포넌트보다 늦게 시작·먼저 종료한다(소비 시작 전에 {@code RedisStreamInitializer}가
 * consumer group을 만들어둔 상태를 전제). 그룹 미존재(NOGROUP)는 짧게 대기 후 다음 루프에서
 * 재시도한다.
 *
 * <p><b>XACK 타이밍(중요)</b>: {@link ProjectionApplier#apply(StoredEvent)}의 {@code @Transactional}
 * 커밋이 성공한 후(=예외 없이 정상 반환)에만 XACK 한다. 커밋 전 XACK 금지(유실 위험). 재전달·중복은
 * ProjectionApplier의 seq-guard가 흡수한다(AC-6).
 *
 * <p>재청구는 Spring Data Redis 3.3.x 고수준 API에 XAUTOCLAIM 직접 지원이 없어
 * <b>XPENDING + XCLAIM 조합</b>으로 구현했다. {@code pending(...)}으로 idle pending 목록을 조회하고,
 * {@code claim(...)}으로 청구해 재처리하거나 DLQ로 격리한다.
 */
@Component
public class ProjectionWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ProjectionWorker.class);

    /** 다른 SmartLifecycle보다 늦게 시작·먼저 종료하도록 큰 phase 값을 사용한다. */
    private static final int PHASE = Integer.MAX_VALUE - 1024;

    /** Redis 연결 예외 등 read 전체 실패 시 백오프 시간(ms). BR-4: 복구 시 따라잡음. */
    private static final long ERROR_BACKOFF_MS = 1_000L;

    /** stop() 시 소비 스레드 종료를 기다리는 최대 시간(ms). */
    private static final long JOIN_TIMEOUT_MS = 10_000L;

    private final StringRedisTemplate redisTemplate;
    private final StreamProps props;
    private final EventStreamCodec codec;
    private final ProjectionApplier projectionApplier;
    private final ProjectionLagMetrics lagMetrics;

    private volatile boolean running = false;
    private Thread worker;

    /** 마지막 reclaim 패스 시각(ms). throttle로 reclaim 패스를 claimMinIdleMs마다 1회로 제한한다. */
    private long lastReclaimAtMs = 0L;

    public ProjectionWorker(StringRedisTemplate redisTemplate,
                            StreamProps props,
                            EventStreamCodec codec,
                            ProjectionApplier projectionApplier,
                            ProjectionLagMetrics lagMetrics) {
        this.redisTemplate = redisTemplate;
        this.props = props;
        this.codec = codec;
        this.projectionApplier = projectionApplier;
        this.lagMetrics = lagMetrics;
    }

    // ---- SmartLifecycle ----

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        worker = new Thread(this::runLoop, "projection-worker");
        worker.setDaemon(true);
        worker.start();
        log.info("ProjectionWorker 시작: stream={}, group={}, consumer={}",
                props.stream(), props.group(), props.consumer());
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        Thread t = this.worker;
        if (t != null) {
            t.interrupt();
            try {
                t.join(JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            this.worker = null;
        }
        log.info("ProjectionWorker 종료");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    // ---- 소비 루프 ----

    private void runLoop() {
        while (running) {
            try {
                consumeNew();
                reclaimPending();
            } catch (DataAccessException e) {
                // Redis 연결 예외 등 read 전체 실패: WARN 후 짧게 대기하고 루프 지속(BR-4).
                log.warn("ProjectionWorker 루프에서 Redis 접근 예외 발생, {}ms 후 재시도", ERROR_BACKOFF_MS, e);
                sleepQuietly(ERROR_BACKOFF_MS);
            } catch (RuntimeException e) {
                // 개별 메시지 처리 예외가 루프를 죽이지 않도록 방어.
                log.warn("ProjectionWorker 루프에서 예외 발생, 루프 지속", e);
                sleepQuietly(ERROR_BACKOFF_MS);
            }
        }
    }

    /**
     * 1) 신규 소비: XREADGROUP ... STREAMS events &gt; (미전달 신규 메시지).
     *
     * <p>BLOCK 타임아웃으로 빈 결과면 자연 대기 효과가 발생한다.
     */
    private void consumeNew() {
        List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream().read(
                Consumer.from(props.group(), props.consumer()),
                StreamReadOptions.empty()
                        .count(props.readCount())
                        .block(Duration.ofMillis(props.readBlockMs())),
                StreamOffset.create(props.stream(), ReadOffset.lastConsumed()));

        if (records == null || records.isEmpty()) {
            return;
        }

        for (MapRecord<String, Object, Object> record : records) {
            applyAndAck(record);
        }
    }

    /**
     * 단일 record를 apply하고 커밋 성공 시에만 XACK. 실패 시 XACK 생략(pending 잔류 →
     * 다음 사이클의 재청구 단계가 처리).
     */
    private void applyAndAck(MapRecord<String, Object, Object> record) {
        StoredEvent event;
        try {
            event = codec.toStoredEvent(toStringMap(record.getValue()));
        } catch (RuntimeException e) {
            // 디코딩 자체가 불가능한 메시지는 재처리해도 동일 실패다. pending에 남겨두면
            // 재청구 단계가 deliveryCount 누적 후 DLQ로 격리한다.
            log.warn("이벤트 디코딩 실패, pending 유지: id={}", record.getId(), e);
            return;
        }

        try {
            projectionApplier.apply(event);
        } catch (RuntimeException e) {
            // apply TX 미커밋: XACK 생략. 재청구 단계가 처리.
            log.warn("projection apply 실패, XACK 생략(pending 유지): event_id={}, session_id={}, seq={}",
                    event.eventId(), event.sessionId(), event.seq(), e);
            return;
        }

        // apply TX 커밋 성공 후에만 XACK.
        redisTemplate.opsForStream().acknowledge(props.stream(), props.group(), record.getId());
        // 스트림에서 소비·ACK한 메시지 1건 처리 완료(처리량 관측, FR-P3-4). DLQ 경로는 별도 카운터.
        lagMetrics.recordStreamProcessed();
    }

    /**
     * 2) 재청구: idle pending 재처리 + DLQ 격리(XPENDING + XCLAIM).
     *
     * <p>이 그룹의 pending 목록을 조회해 idle 시간이 {@code claimMinIdleMs}(기본 30s) 이상인 것만
     * 처리한다(idle 미달은 skip하여 ~30s 백오프 근사). deliveryCount가 maxAttempts 이하면 청구·재처리하고,
     * 초과하면 DLQ로 격리한다(BR-3/AC-5).
     *
     * <p><b>throttle</b>: idle 게이트({@code claimMinIdleMs})가 이미 메시지별 재청구 주기를 제한하므로,
     * 전체 reclaim 패스 자체도 {@code claimMinIdleMs}마다 1회로 제한한다. 재처리 지연 없이 매 루프의
     * XPENDING 풀스캔 왕복만 줄인다.
     *
     * <p><b>단일 윈도우</b>: 가장 오래된 {@code readCount}건만 조회한다(유계라 안정적). 오래된 항목이
     * 먼저 DLQ로 빠지며 백로그가 패스마다 자연 배수된다.
     */
    private void reclaimPending() {
        // throttle: claimMinIdleMs마다 1회만 reclaim 패스 실행(XPENDING 풀스캔 빈도 제한).
        long now = System.currentTimeMillis();
        if (now - lastReclaimAtMs < props.claimMinIdleMs()) {
            return;
        }
        lastReclaimAtMs = now;

        PendingMessages pending = redisTemplate.opsForStream()
                .pending(props.stream(), props.group(), Range.unbounded(), props.readCount());
        if (pending == null || pending.isEmpty()) {
            return;
        }
        for (PendingMessage message : pending) {
            long idleMs = message.getElapsedTimeSinceLastDelivery().toMillis();
            if (idleMs < props.claimMinIdleMs()) {
                // idle 미달: 아직 원소비자가 처리 중일 수 있으므로 skip(백오프 근사).
                continue;
            }
            RecordId id = message.getId();
            long deliveryCount = message.getTotalDeliveryCount();
            if (deliveryCount > props.maxAttempts()) {
                moveToDlq(id, deliveryCount);
            } else {
                reprocess(id);
            }
        }
    }

    /**
     * deliveryCount &lt;= maxAttempts: 청구 후 재처리. 성공 시 XACK, 실패 시 pending 유지
     * (다음 사이클에 deliveryCount 증가).
     */
    private void reprocess(RecordId id) {
        List<MapRecord<String, Object, Object>> claimed = claim(id);
        if (claimed.isEmpty()) {
            // 다른 곳에서 이미 처리/ack됐을 수 있다(null/empty 방어).
            return;
        }

        for (MapRecord<String, Object, Object> record : claimed) {
            applyAndAck(record);
        }
    }

    /**
     * deliveryCount &gt; maxAttempts: DLQ로 격리(BR-3/AC-5). 청구로 원본 내용을 확보해
     * events:dlq에 XADD한 뒤 본 스트림에서 XACK로 제거한다. DLQ 적재 후 본 파이프라인은 계속 처리한다.
     *
     * <p><b>XADD 실패 시 정체 방지</b>: DLQ XADD가 실패하더라도 finally 블록에서 본 스트림 XACK를
     * 반드시 실행한다. 이벤트는 event store가 진실의 원천이므로(BR-6) 복원 가능하며,
     * XACK 생략 시 매 재청구 사이클마다 XADD 실패가 반복되어 파이프라인이 정체된다.
     */
    private void moveToDlq(RecordId id, long deliveryCount) {
        List<MapRecord<String, Object, Object>> claimed = claim(id);
        if (claimed.isEmpty()) {
            // 이미 사라진 메시지: 본 스트림에서도 제거 시도 후 종료.
            redisTemplate.opsForStream().acknowledge(props.stream(), props.group(), id);
            return;
        }

        for (MapRecord<String, Object, Object> record : claimed) {
            Map<String, String> fields = toStringMap(record.getValue());
            fields.put("dlq_reason", "max_attempts_exceeded");
            fields.put("attempts", String.valueOf(deliveryCount));

            MapRecord<String, String, String> dlqRecord =
                    StreamRecords.<String, String, String>mapBacked(fields).withStreamKey(props.dlqStream());

            try {
                redisTemplate.opsForStream().add(dlqRecord);
                log.error("DLQ 격리(BR-3/AC-5): event_id={}, session_id={}, seq={}, deliveryCount={}, dlqStream={}",
                        fields.get(StreamConstants.FIELD_EVENT_ID),
                        fields.get(StreamConstants.FIELD_SESSION_ID),
                        fields.get(StreamConstants.FIELD_SEQ),
                        deliveryCount,
                        props.dlqStream());
            } catch (RuntimeException ex) {
                // DLQ 적재 실패를 카운터로 추적(운영 알림 기반). 정체 방지(finally XACK)는 그대로.
                lagMetrics.recordDlqWriteFailure();
                log.error("DLQ XADD 실패 — 본 스트림 XACK로 정체 방지(event store 복원 가능): "
                                + "event_id={}, session_id={}, seq={}",
                        fields.get(StreamConstants.FIELD_EVENT_ID),
                        fields.get(StreamConstants.FIELD_SESSION_ID),
                        fields.get(StreamConstants.FIELD_SEQ),
                        ex);
            } finally {
                redisTemplate.opsForStream().acknowledge(props.stream(), props.group(), record.getId());
            }
        }
    }

    /** XCLAIM: 지정 id를 현재 consumer로 청구. 결과는 비어 있을 수 있다(이미 처리/ack됨). */
    private List<MapRecord<String, Object, Object>> claim(RecordId id) {
        List<MapRecord<String, Object, Object>> claimed = redisTemplate.opsForStream().claim(
                props.stream(),
                props.group(),
                props.consumer(),
                XClaimOptions.minIdle(Duration.ofMillis(props.claimMinIdleMs())).ids(id));
        return claimed == null ? List.of() : claimed;
    }

    /**
     * {@code StringRedisTemplate.opsForStream()}의 정적 타입이 {@code StreamOperations<String,Object,Object>}라
     * record value가 {@code Map<Object,Object>}로 들어온다(런타임 값은 StringRedisTemplate이라 실제 String).
     * EventStreamCodec이 받는 {@code Map<String,String>}으로 변환한다.
     */
    private static Map<String, String> toStringMap(Map<Object, Object> raw) {
        Map<String, String> m = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> e : raw.entrySet()) {
            m.put(String.valueOf(e.getKey()), e.getValue() == null ? null : String.valueOf(e.getValue()));
        }
        return m;
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
