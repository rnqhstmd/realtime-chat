package com.realtimechat.async;

/**
 * Redis Stream 엔트리 필드 키 상수.
 *
 * <p>EventStreamCodec 이 Map&lt;String,String&gt; 으로 직렬화할 때 사용하는 필드명을 중앙 관리한다.
 * 스트림 이름·소비자 그룹 등 런타임 값은 {@link StreamProps} 에서 관리한다.
 */
public final class StreamConstants {

    public static final String FIELD_EVENT_ID       = "event_id";
    public static final String FIELD_SESSION_ID     = "session_id";
    public static final String FIELD_SEQ            = "seq";
    public static final String FIELD_TYPE           = "type";
    public static final String FIELD_PAYLOAD        = "payload";
    public static final String FIELD_IDEMPOTENCY_KEY = "idempotency_key";
    public static final String FIELD_ACTOR_ID       = "actor_id";
    public static final String FIELD_OCCURRED_AT    = "occurred_at";

    private StreamConstants() {
    }
}
