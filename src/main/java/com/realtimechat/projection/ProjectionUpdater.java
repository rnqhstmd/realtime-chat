package com.realtimechat.projection;

import com.realtimechat.event.StoredEvent;
import com.realtimechat.event.payload.MessageDeletedPayload;
import com.realtimechat.event.payload.MessageEditedPayload;
import com.realtimechat.event.payload.MessageSentPayload;
import com.realtimechat.event.payload.ParticipantJoinedPayload;
import com.realtimechat.event.payload.ParticipantLeftPayload;
import com.realtimechat.event.payload.PresenceChangedPayload;
import com.realtimechat.common.json.JsonUtil;
import org.springframework.stereotype.Component;

/**
 * 동기 projection 오케스트레이터 (설계서 §13 Phase 1, §4.1 계층2, §4.2).
 *
 * <p>CommandHandler(B4)가 이벤트를 append한 직후 <b>같은 트랜잭션</b>에서 {@link #apply(StoredEvent)}를
 * 호출한다. 이벤트 타입별로 해당 view DAO에 위임하고, session_view 카운터/활동시각을 갱신한다.
 *
 * <p><b>멱등성(seq-guard)</b>: 모든 적용은 DAO의 {@code seq > last_applied_seq}
 * (또는 {@code last_updated_seq}) WHERE 조건으로 멱등하다(§4.1 계층2). 따라서 동일 이벤트가
 * 중복 적용되어도 view 상태는 변하지 않는다.
 *
 * <p><b>카운터 정확성</b>: participant_count는 {@code applyJoined}/{@code applyLeft}가
 * <b>실제 상태 전이(true)</b>를 보고한 경우에만 증감한다. 즉 활성 전이(신규 가입·LEFT→JOINED
 * 재가입)에만 +1, 비활성 전이(JOINED→LEFT)에만 -1 하므로 중복/반복 이벤트로는 변하지 않는다.
 * message_count 증가는 {@code applySent}가 <b>실제 신규 INSERT(true)</b>를 보고한 경우에만
 * 수행한다(재전달 no-op이면 보존).
 *
 * <p><b>Phase 1 범위</b>: 동기 projection만 구현한다. gap-fill·스트림 재청구는 Phase 2 대상이며
 * 여기서는 다루지 않는다. seq-guard는 오직 멱등성(중복 적용 방지) 목적이다.
 */
@Component
public class ProjectionUpdater {

    private final ParticipantViewDao participantView;
    private final MessageViewDao messageView;
    private final SessionViewDao sessionView;

    public ProjectionUpdater(ParticipantViewDao participantView,
                             MessageViewDao messageView,
                             SessionViewDao sessionView) {
        this.participantView = participantView;
        this.messageView = messageView;
        this.sessionView = sessionView;
    }

    /** 단일 이벤트를 읽기 모델에 동기 반영한다. 호출자의 트랜잭션 안에서 실행된다. */
    public void apply(StoredEvent e) {
        switch (e.type()) {
            case MESSAGE_SENT -> applyMessageSent(e);
            case MESSAGE_EDITED -> applyMessageEdited(e);
            case MESSAGE_DELETED -> applyMessageDeleted(e);
            case PARTICIPANT_JOINED -> applyParticipantJoined(e);
            case PARTICIPANT_LEFT -> applyParticipantLeft(e);
            case PRESENCE_CHANGED -> applyPresenceChanged(e);
        }
    }

    private void applyMessageSent(StoredEvent e) {
        MessageSentPayload p = JsonUtil.fromNode(e.payload(), MessageSentPayload.class);
        boolean inserted = messageView.applySent(
                e.sessionId(), p.messageId(), e.seq(), p.senderId(), p.content(), e.occurredAt());
        // 신규 INSERT일 때만 message_count 증가(중복 재전달 시 카운터 보존).
        if (inserted) {
            sessionView.incrementMessages(e.sessionId());
        }
        sessionView.touchActivity(e.sessionId(), e.occurredAt());
    }

    private void applyMessageEdited(StoredEvent e) {
        MessageEditedPayload p = JsonUtil.fromNode(e.payload(), MessageEditedPayload.class);
        messageView.applyEdited(e.sessionId(), p.messageId(), p.content(), e.seq());
        sessionView.touchActivity(e.sessionId(), e.occurredAt());
    }

    private void applyMessageDeleted(StoredEvent e) {
        MessageDeletedPayload p = JsonUtil.fromNode(e.payload(), MessageDeletedPayload.class);
        messageView.applyDeleted(e.sessionId(), p.messageId(), e.seq());
        sessionView.touchActivity(e.sessionId(), e.occurredAt());
    }

    private void applyParticipantJoined(StoredEvent e) {
        ParticipantJoinedPayload p = JsonUtil.fromNode(e.payload(), ParticipantJoinedPayload.class);
        // 실제 활성 전이(신규 가입 또는 LEFT→JOINED 재가입)일 때만 participant_count 증가.
        if (participantView.applyJoined(e.sessionId(), p.participantId(), e.seq())) {
            sessionView.incrementParticipants(e.sessionId());
        }
        sessionView.touchActivity(e.sessionId(), e.occurredAt());
    }

    private void applyParticipantLeft(StoredEvent e) {
        ParticipantLeftPayload p = JsonUtil.fromNode(e.payload(), ParticipantLeftPayload.class);
        // 실제 비활성 전이(JOINED→LEFT)일 때만 participant_count 감소. 중복/반복 LEFT는 no-op.
        if (participantView.applyLeft(e.sessionId(), p.participantId(), e.seq())) {
            sessionView.decrementParticipants(e.sessionId());
        }
        sessionView.touchActivity(e.sessionId(), e.occurredAt());
    }

    private void applyPresenceChanged(StoredEvent e) {
        PresenceChangedPayload p = JsonUtil.fromNode(e.payload(), PresenceChangedPayload.class);
        participantView.applyPresence(e.sessionId(), p.participantId(), p.presence(), e.seq());
    }
}
