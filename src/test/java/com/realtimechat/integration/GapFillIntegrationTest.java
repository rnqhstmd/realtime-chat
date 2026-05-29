package com.realtimechat.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.command.CommandHandler;
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
 * AC-8/FR-P2-7 gap-fill 직접 검증: relay 비활성(@MockBean) 후 seq=3만 직접 적용하여
 * ProjectionApplier가 누락 seq 1,2를 event store에서 보강하는지 결정적으로 확인.
 *
 * <p>결정적 gap-fill 유발 전략:
 * <ol>
 *   <li>{@link OutboxRelay}를 {@code @MockBean}으로 대체하여 자동 발행을 차단한다.
 *       worker는 XADD 없이 이 세션의 stream 메시지를 절대 소비하지 않으므로
 *       {@code projection_offset}은 우리의 {@link ProjectionApplier#apply} 직접 호출만으로
 *       변경된다 — 비동기 경쟁이 없어 결정적이다.</li>
 *   <li>3건의 MESSAGE_SENT를 append하여 event store에 seq=1,2,3을 기록한다.
 *       outbox 행도 기록되나 relay가 mock이라 Redis XADD가 일어나지 않는다.</li>
 *   <li>seq=3 이벤트 1건만 {@code projectionApplier.apply()}로 테스트 스레드에서 직접 호출한다.
 *       이 시점 {@code projection_offset}에 행이 없어 lastApplied=0이므로
 *       incoming.seq(3) > lastApplied+1(1) → gap 조건 성립.</li>
 *   <li>ProjectionApplier 내부에서 {@code findBySeqRange(sessionId, 0, 3)}으로 seq 1,2,3을
 *       전부 조회·적용한 뒤 offset을 3으로 갱신한다.</li>
 *   <li>동기 apply가 완료된 후 await 없이 즉시 단언한다(결정적·동기 경로).</li>
 * </ol>
 *
 * <p>주의: {@code @MockBean OutboxRelay}는 이 클래스의 Spring 컨텍스트를 별도로 캐시한다(의도된 동작).
 * ProjectionWorker, RedisStreamInitializer 등 나머지 빈은 실제로 동작한다.
 */
class GapFillIntegrationTest extends AbstractIntegrationTest {

    /** 자동 발행을 차단하여 비동기 worker가 이 세션을 건드리지 않도록 한다. */
    @MockBean
    private OutboxRelay outboxRelay;

    @Autowired
    private SessionService sessionService;

    @Autowired
    private CommandHandler commandHandler;

    @Autowired
    private EventStore eventStore;

    @Autowired
    private ProjectionApplier projectionApplier;

    @Autowired
    private MessageViewDao messageViewDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static JsonNode node(Object record) {
        return JsonUtil.toJsonNode(record);
    }

    private StoredEvent sendMessage(UUID sessionId, UUID sender, String content, String key) {
        return commandHandler.handle(
                sessionId,
                EventType.MESSAGE_SENT,
                node(new MessageSentPayload(UUID.randomUUID(), sender, content)),
                key,
                sender);
    }

    /**
     * AC-8/FR-P2-7: relay 비활성 상태에서 seq=3만 직접 apply하면
     * ProjectionApplier가 누락 seq 1,2를 event store에서 보강 적용해야 한다.
     *
     * <p>gap 유발: projection_offset 행 없음(lastApplied=0) → incoming.seq(3) > 0+1
     * → gap 분기에서 findBySeqRange(sessionId, 0, 3)으로 seq 1,2,3 전체 보강 적용.
     * apply 완료 후 await 없이 즉시 동기 단언(결정적).
     */
    @Test
    @DisplayName("AC-8: relay 비활성(@MockBean) 후 seq=3만 직접 apply → gap-fill로 seq 1,2,3 모두 반영")
    void ac8_gapFillDirectApply() {
        // given: 세션 생성 및 3건 append(event store에 seq=1,2,3 기록)
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        StoredEvent e1 = sendMessage(sessionId, sender, "gap-direct-1", "gf-1"); // seq=1
        StoredEvent e2 = sendMessage(sessionId, sender, "gap-direct-2", "gf-2"); // seq=2
        StoredEvent e3 = sendMessage(sessionId, sender, "gap-direct-3", "gf-3"); // seq=3

        // event store에 seq 1,2,3이 순서대로 기록됐는지 사전 검증
        assertThat(List.of(e1.seq(), e2.seq(), e3.seq())).containsExactly(1L, 2L, 3L);

        // relay가 mock이므로 projection_offset 행이 없다(worker가 한 번도 이 세션을 처리하지 않음)
        Integer offsetRowCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM projection_offset WHERE session_id = ?",
                Integer.class, sessionId);
        assertThat(offsetRowCount)
                .as("relay가 mock이므로 projection_offset에 아직 행이 없어야 한다(lastApplied=0 초기화 대기)")
                .isZero();

        // when: seq=3 이벤트만 ProjectionApplier에 직접 전달
        // event store에서 seq=3을 명시적으로 조회하여 사용(설계 신뢰성 확보)
        List<StoredEvent> range = eventStore.findBySeqRange(sessionId, 2L, 3L);
        assertThat(range).hasSize(1);
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
                .containsExactlyInAnyOrder("gap-direct-1", "gap-direct-2", "gap-direct-3");

        // projection_offset.last_applied_seq가 3으로 갱신됐는지 확인
        Long lastApplied = jdbcTemplate.queryForObject(
                "SELECT last_applied_seq FROM projection_offset WHERE session_id = ?",
                Long.class, sessionId);
        assertThat(lastApplied)
                .as("gap-fill 완료 후 projection_offset.last_applied_seq = 3이어야 한다")
                .isEqualTo(3L);

        // (선택) message_count: session_view의 messageCount가 3이어야 함
        Long messageCount = jdbcTemplate.queryForObject(
                "SELECT message_count FROM session_view WHERE session_id = ?",
                Long.class, sessionId);
        assertThat(messageCount)
                .as("gap-fill로 3건이 적용됐으므로 message_count = 3이어야 한다")
                .isEqualTo(3L);
    }
}
