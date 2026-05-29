package com.realtimechat.session;

import com.realtimechat.projection.SessionViewDao;
import java.time.Instant;
import java.util.UUID;

/**
 * 세션 목록 항목 응답 DTO(설계서 §11 {@code GET /sessions}, PRD R1).
 *
 * <p>읽기 모델 행({@link SessionViewDao.SessionRow})을 그대로 노출하지 않고 외부 표현으로
 * 매핑한다. 목록은 session_view를 직접 조회(CQRS 라이브 조회, §2.2)한 결과를 담는다.
 *
 * @param sessionId        세션 식별자
 * @param status           ACTIVE / ENDED
 * @param participantCount 활성 참여자 수
 * @param messageCount     누적 메시지 수
 * @param createdAt        생성 시각
 * @param lastActivityAt   마지막 활동 시각(nullable)
 * @param endedAt          종료 시각(nullable)
 */
public record SessionSummary(
        UUID sessionId,
        String status,
        int participantCount,
        long messageCount,
        Instant createdAt,
        Instant lastActivityAt,
        Instant endedAt) {

    /** 읽기 모델 행을 목록 응답 표현으로 매핑한다. */
    public static SessionSummary from(SessionViewDao.SessionRow row) {
        return new SessionSummary(
                row.sessionId(),
                row.status(),
                row.participantCount(),
                row.messageCount(),
                row.createdAt(),
                row.lastActivityAt(),
                row.endedAt());
    }
}
