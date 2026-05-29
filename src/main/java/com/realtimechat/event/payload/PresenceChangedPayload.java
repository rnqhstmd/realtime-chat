package com.realtimechat.event.payload;

import java.util.UUID;

/**
 * PRESENCE_CHANGED 이벤트 payload(JSONB 직렬화 대상).
 *
 * @param presence ONLINE / OFFLINE
 */
public record PresenceChangedPayload(UUID participantId, String presence) {
}
