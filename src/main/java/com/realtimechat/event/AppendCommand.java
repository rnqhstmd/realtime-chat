package com.realtimechat.event;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

/**
 * Event Store append 입력 커맨드.
 *
 * <p>payload는 이미 직렬화된 {@link JsonNode}로 전달한다(타입별 payload record →
 * JsonNode 변환은 호출자/CommandHandler 경계에서 완료). seq와 event_id, occurred_at은
 * append 시점에 Event Store가 결정하므로 커맨드에 포함하지 않는다(설계서 §4.2, §10 Q1).
 *
 * @param sessionId      대상 세션
 * @param type           이벤트 유형
 * @param payload        payload(JSONB로 저장)
 * @param idempotencyKey 수집 멱등 키(§4.1 계층1)
 * @param actorId        행위 주체(nullable)
 */
public record AppendCommand(
        UUID sessionId,
        EventType type,
        JsonNode payload,
        String idempotencyKey,
        UUID actorId) {
}
