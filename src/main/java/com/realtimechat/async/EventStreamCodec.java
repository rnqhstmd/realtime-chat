package com.realtimechat.async;

import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * {@link StoredEvent} ↔ {@code Map<String,String>} 양방향 변환.
 *
 * <p>Redis Stream 엔트리는 모든 필드를 문자열로 저장하므로,
 * 이 코덱이 직렬화·역직렬화를 전담한다. payload 는 {@link JsonUtil#toJson}
 * / {@link JsonUtil#readTree} 를 통해 JSON 문자열로 왕복한다.
 *
 * <p>{@code actorId} 가 {@code null} 인 경우 빈 문자열로 직렬화하고,
 * 역직렬화 시 빈 문자열을 {@code null} 로 복원한다.
 *
 * <p>{@code occurredAt} 은 epoch-milli 문자열로 저장한다.
 */
@Component
public class EventStreamCodec {

    /**
     * StoredEvent → Map&lt;String,String&gt;
     */
    public Map<String, String> toFields(StoredEvent e) {
        Map<String, String> fields = new HashMap<>(16);
        fields.put(StreamConstants.FIELD_EVENT_ID,        e.eventId().toString());
        fields.put(StreamConstants.FIELD_SESSION_ID,      e.sessionId().toString());
        fields.put(StreamConstants.FIELD_SEQ,             String.valueOf(e.seq()));
        fields.put(StreamConstants.FIELD_TYPE,            e.type().name());
        // payload는 event.payload JSONB NOT NULL 보장(스키마)이므로 항상 직렬화. null 가드 불요(round-trip 비대칭 회피).
        fields.put(StreamConstants.FIELD_PAYLOAD, JsonUtil.toJson(e.payload()));
        fields.put(StreamConstants.FIELD_IDEMPOTENCY_KEY,
                e.idempotencyKey() == null ? "" : e.idempotencyKey());
        fields.put(StreamConstants.FIELD_ACTOR_ID,
                e.actorId() != null ? e.actorId().toString() : "");
        fields.put(StreamConstants.FIELD_OCCURRED_AT,
                e.occurredAt() == null ? "0" : String.valueOf(e.occurredAt().toEpochMilli()));
        return fields;
    }

    /**
     * Map&lt;String,String&gt; → StoredEvent
     */
    public StoredEvent toStoredEvent(Map<String, String> fields) {
        UUID eventId        = UUID.fromString(fields.get(StreamConstants.FIELD_EVENT_ID));
        UUID sessionId      = UUID.fromString(fields.get(StreamConstants.FIELD_SESSION_ID));
        long seq            = Long.parseLong(fields.get(StreamConstants.FIELD_SEQ));
        EventType type      = EventType.valueOf(fields.get(StreamConstants.FIELD_TYPE));
        var payload         = JsonUtil.readTree(fields.get(StreamConstants.FIELD_PAYLOAD));
        String idempotencyKey = fields.get(StreamConstants.FIELD_IDEMPOTENCY_KEY);
        String actorIdRaw   = fields.get(StreamConstants.FIELD_ACTOR_ID);
        UUID actorId        = (actorIdRaw == null || actorIdRaw.isBlank())
                              ? null
                              : UUID.fromString(actorIdRaw);
        Instant occurredAt  = Instant.ofEpochMilli(
                Long.parseLong(fields.get(StreamConstants.FIELD_OCCURRED_AT)));

        return new StoredEvent(
                eventId, sessionId, seq, type, payload,
                idempotencyKey, actorId, occurredAt);
    }
}
