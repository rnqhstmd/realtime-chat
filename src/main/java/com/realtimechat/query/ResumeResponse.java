package com.realtimechat.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
import java.time.Instant;
import java.util.List;

/**
 * resume-by-seq 응답(설계서 §5-C / §11 {@code GET /sessions/{id}/events?afterSeq=}, PRD R6, FR-P3-3).
 *
 * <p>timeline({@link TimelineResponse})이 snapshot+replay로 <b>fold된 상태</b>를 돌려주는 것과 달리,
 * resume는 {@code afterSeq} 이후의 <b>raw 이벤트 delta</b>(실제 이벤트 시퀀스, seq 오름차순)를 그대로
 * 노출한다(결정 E). 클라이언트는 재연결 시 마지막 수신 seq를 {@code afterSeq}로 보내 누락 구간만
 * delta로 수신한 뒤 STOMP를 재구독한다.
 *
 * @param events  afterSeq 이후 이벤트(seq 오름차순, 최대 limit개)
 * @param hasMore limit을 초과하는 이벤트가 더 있으면 true(다음 페이지 존재)
 */
public record ResumeResponse(List<ResumeEvent> events, boolean hasMore) {

    /**
     * resume delta의 단일 이벤트 표현. payload는 {@link JsonNode}를 그대로 노출하여
     * 타입별 역직렬화는 클라이언트에 맡긴다({@link StoredEvent}의 payload 매핑과 동일).
     *
     * @param seq        세션별 단조증가 seq
     * @param type       이벤트 유형
     * @param payload    payload(JSONB → JsonNode 그대로)
     * @param occurredAt 발생 시각
     */
    public record ResumeEvent(long seq, EventType type, JsonNode payload, Instant occurredAt) {

        /** 저장 이벤트를 resume delta 표현으로 매핑한다. */
        public static ResumeEvent from(StoredEvent e) {
            return new ResumeEvent(e.seq(), e.type(), e.payload(), e.occurredAt());
        }
    }
}
