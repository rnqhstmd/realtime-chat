package com.realtimechat.event.payload;

import java.util.UUID;

/** MESSAGE_DELETED 이벤트 payload(JSONB 직렬화 대상). */
public record MessageDeletedPayload(UUID messageId) {
}
