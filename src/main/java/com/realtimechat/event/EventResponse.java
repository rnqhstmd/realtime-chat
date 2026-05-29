package com.realtimechat.event;

import java.time.Instant;
import java.util.UUID;

/**
 * 이벤트 수집 결과 응답 DTO(설계서 §11 {@code POST /sessions/{id}/events}).
 *
 * <p>{@link StoredEvent}(이벤트 저장 표현, payload·멱등키 포함)를 그대로 노출하지 않고, API
 * 소비자가 필요로 하는 식별자·순서·유형·시각만 추린 외부 표현이다. join({@code POST
 * /sessions/{id}/join}) 응답에서도 동일 형태를 재사용한다(같은 단일 수렴 결과).
 *
 * @param eventId    채번된 이벤트 식별자
 * @param seq        세션별 단조증가 seq(서버 채번 권위)
 * @param type       이벤트 유형
 * @param occurredAt 발생 시각
 */
public record EventResponse(
        UUID eventId,
        long seq,
        EventType type,
        Instant occurredAt) {

    /** {@link StoredEvent}에서 외부 응답 표현으로 매핑한다. */
    public static EventResponse from(StoredEvent event) {
        return new EventResponse(
                event.eventId(),
                event.seq(),
                event.type(),
                event.occurredAt());
    }
}
