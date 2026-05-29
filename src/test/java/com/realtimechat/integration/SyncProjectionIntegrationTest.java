package com.realtimechat.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.command.CommandHandler;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
import com.realtimechat.event.payload.MessageSentPayload;
import com.realtimechat.projection.MessageViewDao;
import com.realtimechat.session.SessionService;
import com.realtimechat.support.AbstractIntegrationTest;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

/**
 * AC-11: {@code chat.projection.sync-enabled=true} 경로 통합 테스트.
 *
 * <p>이 클래스만 {@code @TestPropertySource}로 {@code chat.projection.sync-enabled=true}를
 * 오버라이드하여 별도 Spring 컨텍스트에서 실행된다. 동기 projection이 활성화된 상태에서
 * {@link CommandHandler#handle} 반환 즉시 read model이 반영되는 것을 Awaitility 없이 단언한다.
 *
 * <p>컨테이너는 {@link AbstractIntegrationTest}의 static 싱글톤을 재사용하므로
 * Docker 기동 비용이 추가로 발생하지 않는다. Spring 컨텍스트 캐시 키가 달라지는 것은
 * 설계 의도이다(프로퍼티 값이 다르면 컨텍스트를 분리하는 Spring 표준 동작).
 */
@TestPropertySource(properties = "chat.projection.sync-enabled=true")
class SyncProjectionIntegrationTest extends AbstractIntegrationTest {

    @Autowired private SessionService sessionService;
    @Autowired private CommandHandler commandHandler;
    @Autowired private MessageViewDao messageViewDao;
    @Autowired private JdbcTemplate jdbcTemplate;

    private static JsonNode node(Object record) {
        return JsonUtil.toJsonNode(record);
    }

    // =====================================================================
    // AC-11a: MESSAGE_SENT handle 반환 직후 message_view에 즉시 반영
    // =====================================================================
    @Test
    @DisplayName("AC-11a: sync projection — MESSAGE_SENT handle 반환 직후 message_view 즉시 반영 (Awaitility 없음)")
    void ac11a_syncProjection_messageSentReflectedImmediately() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();

        commandHandler.handle(
                sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(messageId, sender, "sync-hello")),
                "sync-msg-" + messageId, sender);

        // Awaitility 없이 즉시 단언 — sync projection 경로에서 handle 완료 시 이미 반영됨
        List<MessageViewDao.MessageRow> recent = messageViewDao.findRecent(sessionId, 10);
        assertThat(recent).hasSize(1);
        assertThat(recent.get(0).messageId()).isEqualTo(messageId);
        assertThat(recent.get(0).content()).isEqualTo("sync-hello");
        assertThat(recent.get(0).state()).isEqualTo("SENT");
    }

    // =====================================================================
    // AC-11b: PARTICIPANT_JOINED handle 반환 직후 session_view participantCount 즉시 반영
    // =====================================================================
    @Test
    @DisplayName("AC-11b: sync projection — join handle 반환 직후 session_view.participant_count 즉시 반영 (Awaitility 없음)")
    void ac11b_syncProjection_joinReflectedImmediately() {
        UUID sessionId = sessionService.createSession();
        UUID alice = UUID.randomUUID();

        // sessionService.join 내부에서도 CommandHandler를 통해 projection이 수행됨
        StoredEvent joined = sessionService.join(sessionId, alice, "sync-join-" + alice);
        assertThat(joined.type()).isEqualTo(EventType.PARTICIPANT_JOINED);

        // Awaitility 없이 즉시 단언 — handle 완료 시점에 participant_view/session_view 반영됨
        Long participantCount = jdbcTemplate.queryForObject(
                "SELECT participant_count FROM session_view WHERE session_id = ?",
                Long.class, sessionId);
        assertThat(participantCount).isEqualTo(1L);

        Long joinedSeqInView = jdbcTemplate.queryForObject(
                "SELECT joined_seq FROM participant_view WHERE session_id = ? AND participant_id = ?",
                Long.class, sessionId, alice);
        assertThat(joinedSeqInView).isEqualTo(joined.seq());
    }
}
