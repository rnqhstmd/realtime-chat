package com.realtimechat.event.payload;

import java.util.UUID;

/** MESSAGE_EDITED 이벤트 payload(JSONB 직렬화 대상). */
public record MessageEditedPayload(UUID messageId, String content) {
}
