package com.realtimechat.restore;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
import com.realtimechat.event.payload.MessageDeletedPayload;
import com.realtimechat.event.payload.MessageEditedPayload;
import com.realtimechat.event.payload.MessageSentPayload;
import com.realtimechat.event.payload.ParticipantJoinedPayload;
import com.realtimechat.event.payload.ParticipantLeftPayload;
import com.realtimechat.event.payload.PresenceChangedPayload;
import com.realtimechat.restore.SessionState.MessageState;
import com.realtimechat.restore.SessionState.ParticipantState;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link Fold} 순수 함수 단위 테스트(설계서 §4.3 fold 규칙 표, §12 단위 레벨).
 *
 * <p>DB가 필요 없다 — {@link StoredEvent}를 직접 만들어 입력하고, 결과 {@link SessionState}를
 * 직접 검증한다. fold는 정제된 로그(중복 제거·seq 정렬 완료)를 누적만 한다는 모델(설계서 §4.3)을
 * 각 이벤트 타입 전이로 확인한다.
 */
class FoldTest {

    private static final UUID SESSION = UUID.randomUUID();

    // ---- 이벤트 빌더 헬퍼: payload는 JsonUtil.toJsonNode(record)로 직렬화 ----

    private static StoredEvent event(long seq, EventType type, Object payloadRecord) {
        JsonNode payload = JsonUtil.toJsonNode(payloadRecord);
        return new StoredEvent(
                UUID.randomUUID(),
                SESSION,
                seq,
                type,
                payload,
                "idem-" + seq,
                null,
                Instant.parse("2026-05-28T00:00:00Z").plusSeconds(seq));
    }

    @Test
    @DisplayName("PARTICIPANT_JOINED → 참여자 집합에 status=JOINED로 추가된다")
    void joinedAddsParticipant() {
        UUID p = UUID.randomUUID();
        SessionState state = Fold.fold(new SessionState(), List.of(
                event(1, EventType.PARTICIPANT_JOINED, new ParticipantJoinedPayload(p))));

        ParticipantState ps = state.participant(p);
        assertThat(ps).isNotNull();
        assertThat(ps.status()).isEqualTo("JOINED");
        assertThat(ps.joinedSeq()).isEqualTo(1L);
        assertThat(state.participantList()).hasSize(1);
    }

    @Test
    @DisplayName("PARTICIPANT_LEFT → 기존 참여자 status=LEFT, joinedSeq 유지, leftSeq 기록")
    void leftMarksParticipantLeft() {
        UUID p = UUID.randomUUID();
        SessionState state = Fold.fold(new SessionState(), List.of(
                event(1, EventType.PARTICIPANT_JOINED, new ParticipantJoinedPayload(p)),
                event(2, EventType.PARTICIPANT_LEFT, new ParticipantLeftPayload(p))));

        ParticipantState ps = state.participant(p);
        assertThat(ps.status()).isEqualTo("LEFT");
        assertThat(ps.joinedSeq()).isEqualTo(1L);
        assertThat(ps.leftSeq()).isEqualTo(2L);
    }

    @Test
    @DisplayName("MESSAGE_SENT → 메시지가 state=SENT로 추가된다")
    void sentAddsMessage() {
        UUID m = UUID.randomUUID();
        UUID sender = UUID.randomUUID();
        SessionState state = Fold.fold(new SessionState(), List.of(
                event(1, EventType.MESSAGE_SENT, new MessageSentPayload(m, sender, "hello"))));

        MessageState ms = state.message(m);
        assertThat(ms).isNotNull();
        assertThat(ms.content()).isEqualTo("hello");
        assertThat(ms.state()).isEqualTo("SENT");
        assertThat(ms.senderId()).isEqualTo(sender);
        assertThat(ms.seq()).isEqualTo(1L);
    }

    @Test
    @DisplayName("MESSAGE_EDITED(존재 & 미삭제) → content 갱신 / state=EDITED, 원래 seq 유지")
    void editedUpdatesExistingMessage() {
        UUID m = UUID.randomUUID();
        UUID sender = UUID.randomUUID();
        SessionState state = Fold.fold(new SessionState(), List.of(
                event(1, EventType.MESSAGE_SENT, new MessageSentPayload(m, sender, "v1")),
                event(2, EventType.MESSAGE_EDITED, new MessageEditedPayload(m, "v2"))));

        MessageState ms = state.message(m);
        assertThat(ms.content()).isEqualTo("v2");
        assertThat(ms.state()).isEqualTo("EDITED");
        assertThat(ms.seq()).isEqualTo(1L); // 생성 seq는 보존
    }

    @Test
    @DisplayName("MESSAGE_EDITED(부재) → 무시(윈도우 밖 메시지)")
    void editedOnMissingMessageIsIgnored() {
        UUID missing = UUID.randomUUID();
        SessionState state = Fold.fold(new SessionState(), List.of(
                event(1, EventType.MESSAGE_EDITED, new MessageEditedPayload(missing, "v2"))));

        assertThat(state.message(missing)).isNull();
        assertThat(state.messages()).isEmpty();
    }

    @Test
    @DisplayName("MESSAGE_DELETED → state=DELETED, content 유지")
    void deletedMarksMessageDeleted() {
        UUID m = UUID.randomUUID();
        UUID sender = UUID.randomUUID();
        SessionState state = Fold.fold(new SessionState(), List.of(
                event(1, EventType.MESSAGE_SENT, new MessageSentPayload(m, sender, "bye")),
                event(2, EventType.MESSAGE_DELETED, new MessageDeletedPayload(m))));

        MessageState ms = state.message(m);
        assertThat(ms.state()).isEqualTo("DELETED");
        assertThat(ms.content()).isEqualTo("bye"); // content는 유지, 상태만 전이
    }

    @Test
    @DisplayName("MESSAGE_EDITED(삭제 후) → 무시 (DELETED는 갱신 불가)")
    void editAfterDeleteIsIgnored() {
        UUID m = UUID.randomUUID();
        UUID sender = UUID.randomUUID();
        SessionState state = Fold.fold(new SessionState(), List.of(
                event(1, EventType.MESSAGE_SENT, new MessageSentPayload(m, sender, "v1")),
                event(2, EventType.MESSAGE_DELETED, new MessageDeletedPayload(m)),
                event(3, EventType.MESSAGE_EDITED, new MessageEditedPayload(m, "v2"))));

        MessageState ms = state.message(m);
        assertThat(ms.state()).isEqualTo("DELETED");
        assertThat(ms.content()).isEqualTo("v1"); // 삭제 후 편집은 무시
    }

    @Test
    @DisplayName("PRESENCE_CHANGED(존재) → presence만 갱신, status 유지")
    void presenceUpdatesExistingParticipant() {
        UUID p = UUID.randomUUID();
        SessionState state = Fold.fold(new SessionState(), List.of(
                event(1, EventType.PARTICIPANT_JOINED, new ParticipantJoinedPayload(p)),
                event(2, EventType.PRESENCE_CHANGED, new PresenceChangedPayload(p, "OFFLINE"))));

        ParticipantState ps = state.participant(p);
        assertThat(ps.presence()).isEqualTo("OFFLINE");
        assertThat(ps.status()).isEqualTo("JOINED"); // status는 유지
    }

    @Test
    @DisplayName("last-edit-wins: 같은 메시지 다중 EDITED는 seq 순서로 마지막 값이 이긴다")
    void lastEditWinsBySeqOrder() {
        UUID m = UUID.randomUUID();
        UUID sender = UUID.randomUUID();
        SessionState state = Fold.fold(new SessionState(), List.of(
                event(1, EventType.MESSAGE_SENT, new MessageSentPayload(m, sender, "v1")),
                event(2, EventType.MESSAGE_EDITED, new MessageEditedPayload(m, "v2")),
                event(3, EventType.MESSAGE_EDITED, new MessageEditedPayload(m, "v3"))));

        MessageState ms = state.message(m);
        assertThat(ms.content()).isEqualTo("v3"); // 마지막 seq=3의 편집이 최종
        assertThat(ms.state()).isEqualTo("EDITED");
    }

    @Test
    @DisplayName("fold는 base를 변형하지 않는다(순수성): base 복사본 위에서만 누적")
    void foldDoesNotMutateBase() {
        UUID p = UUID.randomUUID();
        SessionState base = new SessionState();
        base.putParticipant(new ParticipantState(p, "JOINED", "ONLINE", 1L, null));

        UUID newP = UUID.randomUUID();
        SessionState result = Fold.fold(base, List.of(
                event(2, EventType.PARTICIPANT_JOINED, new ParticipantJoinedPayload(newP))));

        // base는 변형되지 않음
        assertThat(base.participants()).hasSize(1);
        // 결과는 base + 새 참여자
        assertThat(result.participants()).hasSize(2);
    }

    @Test
    @DisplayName("복합 시나리오: 참여→메시지→편집→삭제→이탈 전체 fold 결과")
    void compositeScenario() {
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        UUID m1 = UUID.randomUUID();
        UUID m2 = UUID.randomUUID();

        SessionState state = Fold.fold(new SessionState(), List.of(
                event(1, EventType.PARTICIPANT_JOINED, new ParticipantJoinedPayload(alice)),
                event(2, EventType.PARTICIPANT_JOINED, new ParticipantJoinedPayload(bob)),
                event(3, EventType.MESSAGE_SENT, new MessageSentPayload(m1, alice, "hi")),
                event(4, EventType.MESSAGE_SENT, new MessageSentPayload(m2, bob, "yo")),
                event(5, EventType.MESSAGE_EDITED, new MessageEditedPayload(m1, "hi!")),
                event(6, EventType.MESSAGE_DELETED, new MessageDeletedPayload(m2)),
                event(7, EventType.PARTICIPANT_LEFT, new ParticipantLeftPayload(bob))));

        assertThat(state.participantList()).hasSize(2);
        assertThat(state.participant(alice).status()).isEqualTo("JOINED");
        assertThat(state.participant(bob).status()).isEqualTo("LEFT");
        assertThat(state.message(m1).content()).isEqualTo("hi!");
        assertThat(state.message(m1).state()).isEqualTo("EDITED");
        assertThat(state.message(m2).state()).isEqualTo("DELETED");
        // recentMessages는 DELETED 제외, seq 오름차순
        assertThat(state.recentMessages(10)).hasSize(1);
        assertThat(state.recentMessages(10).get(0).messageId()).isEqualTo(m1);
    }
}
