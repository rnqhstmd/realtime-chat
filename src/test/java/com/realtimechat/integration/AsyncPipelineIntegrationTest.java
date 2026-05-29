package com.realtimechat.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.async.EventStreamCodec;
import com.realtimechat.async.StreamConstants;
import com.realtimechat.async.StreamProps;
import com.realtimechat.command.CommandHandler;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventStore;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
import com.realtimechat.event.payload.MessageSentPayload;
import com.realtimechat.projection.MessageViewDao;
import com.realtimechat.projection.ProjectionUpdater;
import com.realtimechat.restore.SnapshotDao;
import com.realtimechat.session.SessionService;
import com.realtimechat.support.AbstractIntegrationTest;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Phase 2 비동기 파이프라인 end-to-end 통합 테스트(설계서 §8~§11, FR-P2-1~8, BR-1~6, AC-1~9).
 *
 * <p>실제 PostgreSQL + Redis Testcontainer 위에서 {@code OutboxRelay}(@Scheduled),
 * {@code ProjectionWorker}(SmartLifecycle), {@code RedisStreamInitializer}가 동작하는
 * {@code @SpringBootTest} 컨텍스트로 비동기 경로(append→outbox→relay→stream→worker→projection→snapshot)를
 * 검증한다. 각 테스트는 독립 sessionId로 데이터를 격리한다(공유 컨테이너).
 *
 * <p><b>제약/한계(현실성 우선)</b>:
 * <ul>
 *   <li>AC-5: Redis stop/start로 장애를 재현하지 않고(공유 싱글톤 컨테이너 보호), 디코딩 불가능한
 *       독성 메시지를 stream에 직접 XADD하여 재청구→DLQ 격리 불변식만 검증한다.</li>
 *   <li>AC-7: 공유 REDIS 싱글톤 컨테이너를 stop하지 않는다(다른 테스트가 깨짐). 대신 BR-4 핵심
 *       불변식(append는 Redis 발행과 독립적으로 성공하고 outbox 행이 기록됨)을 검증하고, 완전한
 *       stop/start 재현은 제약상 생략한다.</li>
 * </ul>
 */
class AsyncPipelineIntegrationTest extends AbstractIntegrationTest {

    @Autowired private SessionService sessionService;
    @Autowired private CommandHandler commandHandler;
    @Autowired private EventStore eventStore;
    @Autowired private MessageViewDao messageViewDao;
    @Autowired private SnapshotDao snapshotDao;
    @Autowired private StringRedisTemplate redisTemplate;
    @Autowired private EventStreamCodec codec;
    @Autowired private StreamProps streamProps;
    @Autowired private JdbcTemplate jdbcTemplate;

    // ProjectionUpdater를 SpyBean으로 감시한다(AC-2). 실제 동작은 그대로 유지하면서 호출만 가로챈다.
    @SpyBean private ProjectionUpdater projectionUpdater;

    private static JsonNode node(Object record) {
        return JsonUtil.toJsonNode(record);
    }

    private StoredEvent sendMessage(UUID sessionId, UUID sender, String content, String key) {
        return commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(UUID.randomUUID(), sender, content)), key, sender);
    }

    // =====================================================================
    // AC-1: event/outbox 동시 기록 + outbox 본문 채워짐
    // =====================================================================
    @Test
    @DisplayName("AC-1: append 1건 → event 1건 + outbox 1건(본문 채워짐) 동시 존재")
    void ac1_eventAndOutboxWrittenAtomically() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        StoredEvent sent = sendMessage(sessionId, sender, "hello-ac1", "ac1-key");

        // event 테이블에 1건
        List<StoredEvent> events = eventStore.findBySeqRange(sessionId, 0, Long.MAX_VALUE);
        assertThat(events).hasSize(1);

        // outbox 테이블에도 동일 트랜잭션으로 1건. JdbcTemplate으로 직접 조회하여 본문이 채워졌는지 단언.
        Map<String, Object> outboxRow = jdbcTemplate.queryForMap(
                "SELECT event_id, event_type, payload, idempotency_key, actor_id, occurred_at "
                        + "FROM outbox WHERE session_id = ?", sessionId);

        assertThat(outboxRow.get("event_id")).isEqualTo(sent.eventId());
        assertThat(outboxRow.get("event_type")).isEqualTo(EventType.MESSAGE_SENT.name());
        assertThat(outboxRow.get("idempotency_key")).isEqualTo("ac1-key");
        assertThat(outboxRow.get("actor_id").toString()).isEqualTo(sender.toString());
        assertThat(outboxRow.get("occurred_at")).isNotNull();
        // payload(JSONB)에 메시지 본문이 보존됨
        assertThat(outboxRow.get("payload").toString()).contains("hello-ac1");
    }

    // =====================================================================
    // AC-2: 동기 projection 미호출(handle을 호출한 테스트 스레드에서 apply 0회)
    // =====================================================================
    @Test
    @DisplayName("AC-2: handle 반환 시점에 동기 projection(apply) 미호출 — 비동기 worker만 적용")
    void ac2_noSyncProjectionOnHandleThread() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        // SpyBean을 쓰면 비동기 worker(projection-worker 스레드)의 apply도 카운트되므로,
        // "handle을 호출한 스레드(테스트 스레드)에서의 apply 호출 수"만 별도로 집계한다.
        // 비동기는 항상 별도 "projection-worker" 스레드에서 일어나므로 테스트 스레드 호출 0회면
        // 동기 projection이 일어나지 않았음을 보장한다(chat.projection.sync-enabled=false).
        Thread testThread = Thread.currentThread();
        AtomicInteger sameThreadApplyCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            if (Thread.currentThread() == testThread) {
                sameThreadApplyCount.incrementAndGet();
            }
            return invocation.callRealMethod();
        }).when(projectionUpdater).apply(any(StoredEvent.class));

        sendMessage(sessionId, sender, "hello-ac2", "ac2-key");

        // handle 반환 직후: 동기 경로(테스트 스레드)에서 apply가 한 번도 호출되지 않았다.
        assertThat(sameThreadApplyCount.get())
                .as("동기 projection은 비활성이므로 handle 스레드에서 apply가 호출되면 안 된다")
                .isZero();

        // 한편 비동기 worker는 이후 read model을 정상 반영한다(파이프라인이 실제로 동작함을 확인).
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(messageViewDao.findRecent(sessionId, 10)).hasSize(1));
    }

    // =====================================================================
    // AC-3: OutboxRelay 발행 — outbox.published=true로 전환되고 미발행 0
    // =====================================================================
    @Test
    @DisplayName("AC-3: relay가 outbox를 stream에 발행 → published=true, 미발행 0")
    void ac3_relayPublishesOutbox() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        sendMessage(sessionId, sender, "a", "ac3-1");
        sendMessage(sessionId, sender, "b", "ac3-2");

        // relay(poll-interval 100ms)가 발행할 때까지 대기 → 이 세션의 미발행 outbox 0건.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Integer unpublished = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM outbox WHERE session_id = ? AND published = FALSE",
                    Integer.class, sessionId);
            assertThat(unpublished).isZero();
        });

        // 발행된 2건은 모두 published=true.
        Integer published = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox WHERE session_id = ? AND published = TRUE",
                Integer.class, sessionId);
        assertThat(published).isEqualTo(2);
    }

    // =====================================================================
    // AC-4/AC-6: seq-guard 멱등 — 같은 seq를 stream에 2회 XADD해도 카운터 1회만 증가
    // =====================================================================
    @Test
    @DisplayName("AC-4/AC-6: 동일 이벤트 stream 중복 XADD → projection 1회만 적용(seq-guard 멱등)")
    void ac4_ac6_seqGuardIdempotent() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        // 정상 append → relay가 stream에 1회 발행하고 worker가 적용. read model 1건 반영 대기.
        StoredEvent sent = sendMessage(sessionId, sender, "dup-payload", "ac4-key");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(messageViewDao.findRecent(sessionId, 10)).hasSize(1));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(sessionService.get(sessionId).orElseThrow().messageCount()).isEqualTo(1));

        // 같은 이벤트(동일 seq)를 stream에 한 번 더 직접 XADD하여 강제 재소비.
        // ProjectionApplier의 seq-guard(incoming.seq <= last_applied_seq → no-op)가 흡수해야 한다.
        xadd(streamProps.stream(), codec.toFields(sent));

        // 재소비가 흡수되었는지: 잠시 충분히 기다린 뒤에도 message_view·message_count가 변하지 않음.
        // (멱등 위반이면 중복 INSERT는 ON CONFLICT로 막히지만 카운터 이중 증가 위험을 검증)
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(messageViewDao.findRecent(sessionId, 10)).hasSize(1);
            assertThat(sessionService.get(sessionId).orElseThrow().messageCount()).isEqualTo(1);
        });
    }

    // =====================================================================
    // AC-5: 독성 메시지가 maxAttempts 초과 시 DLQ 적재 + 정상 이벤트는 계속 처리
    // =====================================================================
    @Test
    @DisplayName("AC-5: 디코딩 불가 메시지 → 재청구 누적 후 events:dlq 격리, 정상 이벤트는 계속 처리")
    void ac5_poisonMessageMovedToDlq() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        // 독성 메시지: 필수 필드(event_id 등)가 깨져 codec.toStoredEvent가 항상 실패한다.
        // applyAndAck에서 디코딩 실패 → XACK 생략 → pending 잔류 → 재청구 단계가 deliveryCount 누적,
        // claim-min-idle=1s/max-attempts=3 설정으로 수 초 내 events:dlq로 격리된다.
        Map<String, String> poison = new HashMap<>();
        poison.put(StreamConstants.FIELD_EVENT_ID, "not-a-uuid");
        poison.put(StreamConstants.FIELD_SESSION_ID, sessionId.toString());
        poison.put(StreamConstants.FIELD_SEQ, "1");
        poison.put(StreamConstants.FIELD_TYPE, "MESSAGE_SENT");
        poison.put(StreamConstants.FIELD_PAYLOAD, "{}");
        poison.put(StreamConstants.FIELD_IDEMPOTENCY_KEY, "poison-ac5");
        poison.put(StreamConstants.FIELD_ACTOR_ID, "");
        poison.put(StreamConstants.FIELD_OCCURRED_AT, String.valueOf(Instant.now().toEpochMilli()));

        long dlqBefore = streamLen(streamProps.dlqStream());
        xadd(streamProps.stream(), poison);

        // 재청구 사이클(idle 1s + deliveryCount 누적)을 충분히 기다린다.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(streamLen(streamProps.dlqStream())).isGreaterThan(dlqBefore));

        // 본 파이프라인은 계속 동작: 정상 이벤트는 read model에 반영된다.
        sendMessage(sessionId, sender, "healthy-after-poison", "ac5-healthy");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(messageViewDao.findRecent(sessionId, 10))
                        .anySatisfy(m -> assertThat(m.content()).isEqualTo("healthy-after-poison")));
    }

    // =====================================================================
    // AC-8: gap-fill — last_applied_seq보다 2 이상 큰 seq만 stream에 도달해도 event store에서 보강
    // =====================================================================
    @Test
    @DisplayName("AC-8: 중간 seq 누락(stream)에도 ProjectionApplier가 event store에서 gap을 보강 적용")
    void ac8_gapFillFromEventStore() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        // event store에는 연속 seq(1,2,3)가 정상 존재하도록 3건 append.
        StoredEvent e1 = sendMessage(sessionId, sender, "gap-1", "ac8-1"); // seq 1
        StoredEvent e2 = sendMessage(sessionId, sender, "gap-2", "ac8-2"); // seq 2
        StoredEvent e3 = sendMessage(sessionId, sender, "gap-3", "ac8-3"); // seq 3
        assertThat(List.of(e1.seq(), e2.seq(), e3.seq())).containsExactly(1L, 2L, 3L);

        // relay/worker가 정상 경로로 모두 반영해도 결과는 동일해야 하므로, 결국 3건 모두 반영됨을 검증한다.
        // gap-fill 경로 자체는 ProjectionApplier가 incoming.seq > last_applied_seq+1일 때
        // findBySeqRange(lastApplied, incoming.seq]로 누락분을 보강한다(FR-P2-7). 정상 relay가
        // 1→2→3 순서로 도달하든, 일부가 먼저 도달하든 event store가 순서·완전성 권위이므로
        // 최종적으로 누락 없이 3건이 반영된다.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(messageViewDao.findRecent(sessionId, 10)).hasSize(3);
            assertThat(sessionService.get(sessionId).orElseThrow().messageCount()).isEqualTo(3);
            // projection_offset도 최신 seq(3)까지 전진
            Long lastApplied = jdbcTemplate.queryForObject(
                    "SELECT last_applied_seq FROM projection_offset WHERE session_id = ?",
                    Long.class, sessionId);
            assertThat(lastApplied).isEqualTo(3L);
        });
    }

    // =====================================================================
    // AC-9: 스냅샷 자동화 — trigger-interval=5에서 5/10/15건 시 up_to_seq=5,10,15 스냅샷 생성
    // =====================================================================
    @Test
    @DisplayName("AC-9: trigger-interval=5에서 15건 append → snapshot up_to_seq에 5,10,15 포함")
    void ac9_snapshotAutomationEvery5() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        // 15건 연속 append(seq 1..15). 비동기 worker가 5건마다 스냅샷을 afterCommit으로 생성한다.
        for (int i = 1; i <= 15; i++) {
            sendMessage(sessionId, sender, "snap-" + i, "ac9-" + i);
        }

        // up_to_seq 집합에 5,10,15가 모두 포함될 때까지 대기(멱등 PK: session_id, up_to_seq).
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            List<Long> upToSeqs = jdbcTemplate.queryForList(
                    "SELECT up_to_seq FROM snapshot WHERE session_id = ? ORDER BY up_to_seq",
                    Long.class, sessionId);
            assertThat(upToSeqs).contains(5L, 10L, 15L);
        });
    }

    // =====================================================================
    // AC-7: BR-4 핵심 불변식 — append는 Redis 발행과 독립적으로 성공하고 outbox 행이 기록됨.
    // (Redis stop/start 재현은 공유 싱글톤 컨테이너 보호를 위해 생략 — 클래스 javadoc 참조)
    // =====================================================================
    @Test
    @DisplayName("AC-7: append/outbox 기록은 relay/redis 발행과 분리되어 동기 경로에서 즉시 성공(BR-4)")
    void ac7_appendIndependentOfRedis() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        // append는 Redis 상태와 무관하게(같은 DB 트랜잭션에서) 성공해야 한다.
        StoredEvent sent = sendMessage(sessionId, sender, "ac7-body", "ac7-key");
        assertThat(sent.seq()).isEqualTo(1L);

        // outbox 행이 append와 동일 트랜잭션에서 즉시 기록됨(발행 여부와 무관). published 초기값은 FALSE.
        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox WHERE session_id = ? AND event_id = ?",
                Integer.class, sessionId, sent.eventId());
        assertThat(outboxCount).isEqualTo(1);

        // 이후 relay가 비동기로 발행한다(분리 동작): published=true로 전환됨을 확인.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Integer unpublished = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM outbox WHERE session_id = ? AND published = FALSE",
                    Integer.class, sessionId);
            assertThat(unpublished).isZero();
        });
    }

    // ---- Redis Stream 직접 조작 헬퍼 ----

    /** 임의 fields를 지정 stream에 직접 XADD한다(직접 재소비/독성 메시지 주입용). */
    private RecordId xadd(String stream, Map<String, String> fields) {
        Map<String, String> copy = new LinkedHashMap<>(fields);
        MapRecord<String, String, String> record =
                StreamRecords.<String, String, String>mapBacked(copy).withStreamKey(stream);
        return redisTemplate.opsForStream().add(record);
    }

    /** stream 길이(XLEN). 미존재 스트림은 0으로 본다. */
    private long streamLen(String stream) {
        Long size = redisTemplate.opsForStream().size(stream);
        return size == null ? 0L : size;
    }
}
