package com.realtimechat.restore;

import com.realtimechat.event.StoredEvent;
import com.realtimechat.event.payload.MessageDeletedPayload;
import com.realtimechat.event.payload.MessageEditedPayload;
import com.realtimechat.event.payload.MessageSentPayload;
import com.realtimechat.event.payload.ParticipantJoinedPayload;
import com.realtimechat.event.payload.ParticipantLeftPayload;
import com.realtimechat.event.payload.PresenceChangedPayload;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.restore.SessionState.MessageState;
import com.realtimechat.restore.SessionState.ParticipantState;
import java.util.List;

/**
 * fold: base 상태 위에 이벤트 로그를 seq 순서로 적용하는 순수 함수(설계서 §4.3).
 *
 * <p><b>순수성</b>: I/O가 전혀 없다(DB/네트워크/시계 접근 없음). 입력 base는 변형하지 않고
 * 복사본을 만들어 그 위에 적용하므로 호출자에게 부작용이 없다. 따라서 단위 테스트(설계서 §12)에서
 * DB 없이 직접 검증할 수 있다.
 *
 * <p><b>정제는 쓰기 시점에 1회 완료</b>(설계서 §4.3): 중복 제거는 append의 멱등 제약, 정렬은
 * seq 권위에서 이미 끝났다. 따라서 fold는 이미 정제된 로그를 그대로 누적만 한다 —
 * 중복·순서 보정 로직이 없다.
 */
public final class Fold {

    private Fold() {
    }

    /**
     * base를 복사한 새 상태에 events를 입력 순서(= seq 오름차순 가정)대로 적용한다.
     *
     * @param base   복원 기준 상태(스냅샷 또는 빈 상태). 변형되지 않는다.
     * @param events 적용할 이벤트(seq 오름차순; {@code EventStore.findBySeqRange} 결과).
     * @return base + events가 누적된 새 {@link SessionState}.
     */
    public static SessionState fold(SessionState base, List<StoredEvent> events) {
        SessionState state = copy(base);
        for (StoredEvent event : events) {
            apply(state, event);
        }
        return state;
    }

    /** 이벤트 타입별 상태 전이(설계서 §4.3 fold 규칙 표). */
    private static void apply(SessionState state, StoredEvent event) {
        switch (event.type()) {
            case PARTICIPANT_JOINED -> applyParticipantJoined(state, event);
            case PARTICIPANT_LEFT -> applyParticipantLeft(state, event);
            case MESSAGE_SENT -> applyMessageSent(state, event);
            case MESSAGE_EDITED -> applyMessageEdited(state, event);
            case MESSAGE_DELETED -> applyMessageDeleted(state, event);
            case PRESENCE_CHANGED -> applyPresenceChanged(state, event);
        }
    }

    private static void applyParticipantJoined(SessionState state, StoredEvent event) {
        ParticipantJoinedPayload payload =
                JsonUtil.fromNode(event.payload(), ParticipantJoinedPayload.class);
        ParticipantState existing = state.participant(payload.participantId());
        String presence = existing == null ? null : existing.presence();
        state.putParticipant(new ParticipantState(
                payload.participantId(),
                "JOINED",
                presence,
                event.seq(),
                null));
    }

    private static void applyParticipantLeft(SessionState state, StoredEvent event) {
        ParticipantLeftPayload payload =
                JsonUtil.fromNode(event.payload(), ParticipantLeftPayload.class);
        ParticipantState existing = state.participant(payload.participantId());
        Long joinedSeq = existing == null ? null : existing.joinedSeq();
        String presence = existing == null ? null : existing.presence();
        state.putParticipant(new ParticipantState(
                payload.participantId(),
                "LEFT",
                presence,
                joinedSeq,
                event.seq()));
    }

    private static void applyMessageSent(SessionState state, StoredEvent event) {
        MessageSentPayload payload =
                JsonUtil.fromNode(event.payload(), MessageSentPayload.class);
        state.putMessage(new MessageState(
                payload.messageId(),
                event.seq(),
                payload.senderId(),
                payload.content(),
                "SENT",
                event.occurredAt()));
    }

    private static void applyMessageEdited(SessionState state, StoredEvent event) {
        MessageEditedPayload payload =
                JsonUtil.fromNode(event.payload(), MessageEditedPayload.class);
        MessageState existing = state.message(payload.messageId());
        // 존재 & 미삭제일 때만 갱신(없으면 윈도우 밖 메시지로 보고 무시 — 설계서 §4.3).
        if (existing == null || "DELETED".equals(existing.state())) {
            return;
        }
        state.putMessage(new MessageState(
                existing.messageId(),
                existing.seq(),
                existing.senderId(),
                payload.content(),
                "EDITED",
                existing.createdAt()));
    }

    private static void applyMessageDeleted(SessionState state, StoredEvent event) {
        MessageDeletedPayload payload =
                JsonUtil.fromNode(event.payload(), MessageDeletedPayload.class);
        MessageState existing = state.message(payload.messageId());
        // 존재하면 DELETED, 없으면 무시(설계서 §4.3).
        if (existing == null) {
            return;
        }
        state.putMessage(new MessageState(
                existing.messageId(),
                existing.seq(),
                existing.senderId(),
                existing.content(),
                "DELETED",
                existing.createdAt()));
    }

    private static void applyPresenceChanged(SessionState state, StoredEvent event) {
        PresenceChangedPayload payload =
                JsonUtil.fromNode(event.payload(), PresenceChangedPayload.class);
        ParticipantState existing = state.participant(payload.participantId());
        // 없으면 status='JOINED'로 등록하고 presence를 반영한다. PRESENCE_CHANGED는 활성 참여자의
        // 온/오프라인 전환 이벤트이므로(JOINED 선행 가정), JOINED 이벤트가 복원 윈도우 밖이라
        // 누락된 경우에도 참여자를 활성으로 보존한다(joined_seq/left_seq는 미상이라 null).
        if (existing == null) {
            state.putParticipant(new ParticipantState(
                    payload.participantId(),
                    "JOINED",
                    payload.presence(),
                    null,
                    null));
            return;
        }
        state.putParticipant(new ParticipantState(
                existing.participantId(),
                existing.status(),
                payload.presence(),
                existing.joinedSeq(),
                existing.leftSeq()));
    }

    /** base의 깊은 복사(컬렉션은 새로 만들고, 원소는 불변 record라 공유 안전). */
    private static SessionState copy(SessionState base) {
        SessionState copy = new SessionState();
        if (base != null) {
            copy.participants().putAll(base.participants());
            copy.messages().putAll(base.messages());
        }
        return copy;
    }
}
