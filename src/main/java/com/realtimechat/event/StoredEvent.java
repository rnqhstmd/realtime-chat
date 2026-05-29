package com.realtimechat.event;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/**
 * 저장된(또는 append 결과로 반환되는) 이벤트의 불변 표현.
 *
 * <p>event 테이블 한 행과 1:1 대응한다(설계서 §3.1). payload는 JSONB 컬럼을
 * {@link JsonNode}로 매핑하여, 타입별 역직렬화는 소비 측(projection/restore)에서 수행한다.
 *
 * @param eventId        event_id (PK, 앱에서 생성)
 * @param sessionId      session_id
 * @param seq            세션별 단조증가 seq(서버 채번 권위)
 * @param type           event_type
 * @param payload        payload(JSONB)
 * @param idempotencyKey idempotency_key
 * @param actorId        actor_id(nullable)
 * @param occurredAt     occurred_at(DB now() 기본값)
 */
public record StoredEvent(
        UUID eventId,
        UUID sessionId,
        long seq,
        EventType type,
        JsonNode payload,
        String idempotencyKey,
        UUID actorId,
        Instant occurredAt) {
}
