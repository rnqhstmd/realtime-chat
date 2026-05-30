package com.realtimechat.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.event.EventType;

import java.time.Instant;
import java.util.UUID;

/**
 * outbox 테이블 한 행의 불변 표현 (설계서 §8).
 *
 * <p>V2 스키마에서 추가된 본문 컬럼(event_type, payload, idempotency_key, actor_id,
 * occurred_at)을 포함한다. {@link OutboxDao#findUnpublished(int)}의 반환 타입으로 사용하며,
 * 비동기 파이프라인의 릴레이 Publisher가 이 레코드를 읽어 브로드캐스트한다.
 *
 * @param id             outbox PK (BIGSERIAL, 발행 순서 기준)
 * @param eventId        event_id
 * @param sessionId      session_id
 * @param seq            세션별 단조증가 seq
 * @param eventType      이벤트 유형
 * @param payload        payload(JSONB → JsonNode)
 * @param idempotencyKey idempotency_key
 * @param actorId        actor_id(nullable)
 * @param occurredAt     occurred_at
 */
public record OutboxRecord(
        long id,
        UUID eventId,
        UUID sessionId,
        long seq,
        EventType eventType,
        JsonNode payload,
        String idempotencyKey,
        UUID actorId,
        Instant occurredAt) {
}
