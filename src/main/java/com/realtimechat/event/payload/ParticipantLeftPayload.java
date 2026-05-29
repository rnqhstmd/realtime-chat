package com.realtimechat.event.payload;

import java.util.UUID;

/** PARTICIPANT_LEFT 이벤트 payload(JSONB 직렬화 대상). */
public record ParticipantLeftPayload(UUID participantId) {
}
