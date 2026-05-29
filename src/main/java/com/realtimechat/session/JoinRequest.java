package com.realtimechat.session;

import jakarta.validation.constraints.NotNull;
import java.util.UUID;

/**
 * 세션 참여 요청 본문(설계서 §11 {@code POST /sessions/{id}/join}, PRD R1).
 *
 * <p>참여는 PARTICIPANT_JOINED 이벤트로 표현되며, 멱등 키는 본문이 아니라
 * {@code Idempotency-Key} 헤더로 전달된다(§11). 따라서 본문은 참여자 식별자만 담는다.
 *
 * @param participantId 참여자 식별자(필수)
 */
public record JoinRequest(
        @NotNull UUID participantId) {
}
