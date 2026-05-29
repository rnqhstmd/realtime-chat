package com.realtimechat.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.event.EventType;
import jakarta.validation.constraints.NotNull;
import java.util.UUID;

/**
 * 이벤트 수집 요청 본문(설계서 §11 {@code POST /sessions/{id}/events}).
 *
 * <p>REST/WS 컨트롤러(B5)가 바인딩하여 {@link CommandHandler#collect}로 전달한다. 멱등 키는
 * 본문이 아니라 {@code Idempotency-Key} 헤더로 전달되므로 이 record에 포함하지 않는다(§11).
 * payload는 타입별로 이질적이라 {@link JsonNode}로 받아 CommandHandler 경계에서 검증·보강한다.
 *
 * @param type    이벤트 유형(필수)
 * @param payload 타입별 payload(JSONB로 저장될 본문, nullable — CommandHandler가 보강)
 * @param actorId 행위 주체(nullable)
 */
public record CollectEventRequest(
        @NotNull EventType type,
        JsonNode payload,
        UUID actorId) {
}
