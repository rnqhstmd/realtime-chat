package com.realtimechat.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
import java.time.Instant;
import java.util.UUID;

/**
 * 세션 구독자에게 {@code /topic/session.{id}}로 전달되는 실시간 메시지 DTO(설계서 §6).
 *
 * <p>저장된 {@link StoredEvent}에서 전송에 필요한 필드만 노출한다. 멱등 키는 수집 경계의
 * 내부 관심사이므로 구독자에게 보내지 않고, 구독자는 권위 있는 seq로 순서·resume을 따라간다
 * (설계서 §4.2). 구독자는 어느 수집 경로(REST/WS)로 들어온 이벤트든 동일 토픽에서 수신한다.
 *
 * @param eventId    event_id
 * @param sessionId  session_id
 * @param seq        세션별 단조증가 seq(순서 권위, resume 기준)
 * @param type       이벤트 유형
 * @param payload    payload(JSONB)
 * @param occurredAt occurred_at
 */
public record EventBroadcast(
        UUID eventId,
        UUID sessionId,
        long seq,
        EventType type,
        JsonNode payload,
        Instant occurredAt) {

    /** 저장 이벤트를 전송용 DTO로 변환한다. */
    public static EventBroadcast from(StoredEvent event) {
        return new EventBroadcast(
                event.eventId(),
                event.sessionId(),
                event.seq(),
                event.type(),
                event.payload(),
                event.occurredAt());
    }
}
