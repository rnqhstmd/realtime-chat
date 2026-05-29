package com.realtimechat.event.payload;

import java.util.UUID;

/** MESSAGE_SENT 이벤트 payload(JSONB 직렬화 대상). */
public record MessageSentPayload(UUID messageId, UUID senderId, String content) {
}
