package com.realtimechat.event.payload;

import java.util.UUID;

/** PARTICIPANT_JOINED 이벤트 payload(JSONB 직렬화 대상). */
public record ParticipantJoinedPayload(UUID participantId) {
}
