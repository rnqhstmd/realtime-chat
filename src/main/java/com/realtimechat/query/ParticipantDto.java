package com.realtimechat.query;

import com.realtimechat.restore.SessionState;
import java.util.UUID;

/**
 * 복원 타임라인의 참여자 표현(설계서 §4.3 반환: 참여자 목록).
 *
 * <p>{@link SessionState.ParticipantState}(복원 누적 상태의 내부 record)를 그대로 노출하지 않고
 * API 표현으로 매핑한다.
 *
 * @param participantId 참여자 식별자
 * @param status        JOINED / LEFT
 * @param presence      ONLINE / OFFLINE (nullable)
 * @param joinedSeq     참여 seq (nullable)
 * @param leftSeq       이탈 seq (nullable)
 */
public record ParticipantDto(
        UUID participantId,
        String status,
        String presence,
        Long joinedSeq,
        Long leftSeq) {

    /** 복원 상태의 참여자 표현을 API DTO로 매핑한다. */
    public static ParticipantDto from(SessionState.ParticipantState p) {
        return new ParticipantDto(
                p.participantId(),
                p.status(),
                p.presence(),
                p.joinedSeq(),
                p.leftSeq());
    }
}
