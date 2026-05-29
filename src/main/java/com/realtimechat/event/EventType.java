package com.realtimechat.event;

/**
 * 이벤트 유형. event.event_type 컬럼(TEXT)에 enum 이름 그대로 저장된다.
 *
 * <p>append-only 이벤트 로그의 모든 상태 전이는 이 6가지 타입으로 표현된다(설계서 §3.1, §4.3).
 */
public enum EventType {
    MESSAGE_SENT,
    MESSAGE_EDITED,
    MESSAGE_DELETED,
    PARTICIPANT_JOINED,
    PARTICIPANT_LEFT,
    PRESENCE_CHANGED
}
