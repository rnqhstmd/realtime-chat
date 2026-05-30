package com.realtimechat.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventStore;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
import com.realtimechat.event.payload.MessageSentPayload;
import com.realtimechat.outbox.OutboxRelay;
import com.realtimechat.projection.MessageViewDao;
import com.realtimechat.projection.async.ProjectionApplier;
import com.realtimechat.session.SessionService;
import com.realtimechat.support.AbstractIntegrationTest;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * AC-8/FR-P2-7 gap-fill 직접 검증: 이벤트를 event 테이블에 직접 삽입(outbox 우회)으로 어떤
 * 컨텍스트의 relay도 이 세션을 발행/비동기적용하지 못하게 격리한 뒤, seq=3만 직접 apply하여
 * ProjectionApplier가 누락 seq 1,2를 event store에서 보강하는지 결정적으로 확인한다.
 *
 * <p>완전 격리·결정적 gap-fill 유발 전략:
 * <ol>
 *   <li>{@link OutboxRelay}를 {@code @MockBean}으로 대체하여 자기 컨텍스트의 자동 발행을 차단한다.</li>
 *   <li>3건의 MESSAGE_SENT를 {@code CommandHandler}/{@link EventStore#append} 대신
 *       {@code event} 테이블에 직접 INSERT한다(outbox 행 미생성). outbox 행이 없으면 캐시된
 *       다른 {@code @SpringBootTest} 컨텍스트의 실제 relay도 이 세션을 발행할 게 없으므로
 *       worker가 비동기로 이 세션을 건드릴 수 없다 — 직접 apply와의 레이스가 원천 차단된다.</li>
 *   <li>seq=3 이벤트 1건만 {@code projectionApplier.apply()}로 테스트 스레드에서 직접 호출한다.
 *       이 시점 {@code projection_offset}에 행이 없어 lastApplied=0이므로
 *       incoming.seq(3) > lastApplied+1(1) → gap 조건 성립.</li>
 *   <li>ProjectionApplier 내부에서 {@code findBySeqRange(sessionId, 0, 3)}으로 seq 1,2,3을
 *       전부 조회·적용한 뒤 offset을 3으로 갱신한다.</li>
 *   <li>동기 apply가 완료된 후 await 없이 즉시 단언한다(결정적·동기 경로).</li>
 * </ol>
 */
class GapFillIntegrationTest extends AbstractIntegrationTest {

    /** outbox 행을 만들지 않더라도, 자기 컨텍스트 relay까지 비활성화하여 격리를 이중으로 보장한다. */
    @MockBean
    private OutboxRelay outboxRelay;

    @Autowired
    private SessionService sessionService;

    @Autowired
    private EventStore eventStore;

    @Autowired
    private ProjectionApplier projectionApplier;

    @Autowired
    private MessageViewDao messageViewDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * event 테이블에 1건을 직접 INSERT한다(outbox 우회). EventStore.append가 저장하는 형식과 동일하게
     * event_type은 enum name, payload는 JsonUtil.toJson으로 직렬화한 JSONB로 기록한다.
     */
    private void insertEventDirectly(UUID sessionId, long seq, UUID sender, String content, String key) {
        UUID eventId = UUID.randomUUID();
        String payloadJson = JsonUtil.toJson(new MessageSentPayload(UUID.randomUUID(), sender, content));
        jdbcTemplate.update(
                "INSERT INTO event (event_id, session_id, seq, event_type, payload, idempotency_key, actor_id, occurred_at) "
                        + "VALUES (?, ?, ?, ?, CAST(? AS jsonb), ?, ?, now())",
                eventId, sessionId, seq, EventType.MESSAGE_SENT.name(),
                payloadJson, key, sender);
    }

    /**
     * AC-8/FR-P2-7: event 직접 삽입으로 격리된 세션에서 seq=3만 직접 apply하면
     * ProjectionApplier가 누락 seq 1,2를 event store에서 보강 적용해야 한다.
     *
     * <p>gap 유발: projection_offset 행 없음(lastApplied=0) → incoming.seq(3) > 0+1
     * → gap 분기에서 findBySeqRange(sessionId, 0, 3)으로 seq 1,2,3 전체 보강 적용.
     * apply 완료 후 await 없이 즉시 동기 단언(결정적).
     */
    @Test
    @DisplayName("AC-8: event 직접 삽입(outbox 우회)으로 격리 후 seq=3만 직접 apply → gap-fill로 seq 1,2,3 모두 반영")
    void ac8_gapFillDirectApply() {
        // given: 세션 생성 후 event 테이블에 seq 1,2,3을 직접 INSERT(outbox 미생성 → 어떤 relay도 발행 불가)
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        for (int seq = 1; seq <= 3; seq++) {
            insertEventDirectly(sessionId, seq, sender, "gap-" + seq, "gap-key-" + seq);
        }

        // event store에 seq 1,2,3이 순서대로 기록됐는지 사전 검증
        List<StoredEvent> all = eventStore.findBySeqRange(sessionId, 0L, Long.MAX_VALUE);
        assertThat(all)
                .extracting(StoredEvent::seq)
                .as("직접 삽입한 seq 1,2,3이 event store에 순서대로 존재해야 한다")
                .containsExactly(1L, 2L, 3L);

        // 직접 삽입이므로 outbox 행이 없어야 한다(어떤 컨텍스트의 relay도 이 세션을 발행할 수 없음)
        Integer outboxRowCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox WHERE session_id = ?",
                Integer.class, sessionId);
        assertThat(outboxRowCount)
                .as("event 직접 삽입(outbox 우회)이므로 outbox 행이 0이어야 relay 발행이 원천 차단된다")
                .isZero();

        // 비동기 적용이 한 번도 일어나지 않았으므로 projection_offset 행도 없다(lastApplied=0)
        Integer offsetRowCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM projection_offset WHERE session_id = ?",
                Integer.class, sessionId);
        assertThat(offsetRowCount)
                .as("어떤 relay도 발행하지 못하므로 projection_offset에 아직 행이 없어야 한다(lastApplied=0)")
                .isZero();

        // when: seq=3 이벤트만 ProjectionApplier에 직접 전달
        // findBySeqRange는 (fromExclusive, toInclusive] 반개구간이므로 (2,3]은 seq=3 단건만 반환한다.
        List<StoredEvent> range = eventStore.findBySeqRange(sessionId, 2L, 3L);
        assertThat(range)
                .as("(2,3] 반개구간 조회는 seq=3 단건만 반환해야 한다")
                .hasSize(1);
        StoredEvent seq3Event = range.get(0);
        assertThat(seq3Event.seq()).isEqualTo(3L);

        // incoming.seq(3) > lastApplied+1(1) → gap 분기 → findBySeqRange(sessionId, 0, 3) 내부 호출
        // → seq 1,2,3 전체 보강 적용 후 projection_offset.last_applied_seq=3으로 갱신
        projectionApplier.apply(seq3Event);

        // then: await 없이 즉시 동기 단언 (apply가 동기 @Transactional이므로 완료 보장)

        // message_view에 3건이 모두 반영됐는지 확인
        List<MessageViewDao.MessageRow> messages = messageViewDao.findRecent(sessionId, 10);
        assertThat(messages)
                .as("gap-fill로 seq 1,2,3 모두 message_view에 반영되어야 한다")
                .hasSize(3);

        // seq 1,2,3 메시지 본문이 누락 없이 반영됐는지 확인
        assertThat(messages)
                .extracting(MessageViewDao.MessageRow::content)
                .containsExactlyInAnyOrder("gap-1", "gap-2", "gap-3");

        // projection_offset.last_applied_seq가 3으로 갱신됐는지 확인
        Long lastApplied = jdbcTemplate.queryForObject(
                "SELECT last_applied_seq FROM projection_offset WHERE session_id = ?",
                Long.class, sessionId);
        assertThat(lastApplied)
                .as("gap-fill 완료 후 projection_offset.last_applied_seq = 3이어야 한다")
                .isEqualTo(3L);

        // session_view.message_count: gap-fill로 3건이 적용됐으므로 3이어야 함
        Long messageCount = jdbcTemplate.queryForObject(
                "SELECT message_count FROM session_view WHERE session_id = ?",
                Long.class, sessionId);
        assertThat(messageCount)
                .as("gap-fill로 3건이 적용됐으므로 session_view.message_count = 3이어야 한다")
                .isEqualTo(3L);
    }
}
