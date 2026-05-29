package com.realtimechat.query;

import com.realtimechat.restore.SessionState;
import java.util.List;

/**
 * 시점 복원 응답(설계서 §4.3 / §11 {@code GET /sessions/{id}/timeline?at=}, PRD R6).
 *
 * <p>복원된 {@link SessionState}를 외부 표현으로 매핑한다: 참여자 목록 + 메시지 목록(최근 N) +
 * 각 메시지 상태. 메시지는 {@link SessionState#recentMessages(int)} 결과(seq 오름차순)를 담는다.
 *
 * @param participants 참여자 목록
 * @param messages     메시지 목록(최근 N, seq 오름차순)
 */
public record TimelineResponse(
        List<ParticipantDto> participants,
        List<MessageDto> messages) {

    /** 복원 상태를 최근 {@code recent}개 메시지를 담은 타임라인 응답으로 매핑한다. */
    public static TimelineResponse from(SessionState state, int recent) {
        List<ParticipantDto> participants = state.participantList().stream()
                .map(ParticipantDto::from)
                .toList();
        List<MessageDto> messages = state.recentMessages(recent).stream()
                .map(MessageDto::from)
                .toList();
        return new TimelineResponse(participants, messages);
    }
}
