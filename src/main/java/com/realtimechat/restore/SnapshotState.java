package com.realtimechat.restore;

import com.realtimechat.restore.SessionState.MessageState;
import com.realtimechat.restore.SessionState.ParticipantState;
import java.util.List;

/**
 * 스냅샷 state JSONB의 직렬화 포맷(설계서 §3.3).
 *
 * <p>형태: {@code { participants:[...], recentMessages:[...최근 N...], counts:{...} }}.
 * {@link SessionState} ↔ JSONB 변환의 안정적 스키마를 record로 고정한다.
 *
 * <p>설계서 §3.3대로 메시지는 "최근 N개"만 담는다 — 전체가 아닌 크기 상한 N. 복원 시 더 과거가
 * 필요하면 event 추가 replay(Phase1은 recent N 시맨틱 허용).
 */
public record SnapshotState(
        List<ParticipantState> participants,
        List<MessageState> recentMessages,
        Counts counts) {

    /**
     * 집계 카운트. session_view(설계서 §3.2)의 복원 표현 일부.
     *
     * <p>{@code participantCount}는 session_view 시맨틱과 일치하도록 <b>JOINED 상태만</b> 센다
     * (ProjectionUpdater의 increment/decrement는 JOINED↔LEFT 전이에만 반응하므로 LEFT 참여자는
     * 제외된다). 전체 참여자(LEFT 포함) 행은 {@code participants} 리스트로 별도 보존된다.
     */
    public record Counts(int participantCount, long messageCount) {
    }

    /** {@link SessionState} → 스냅샷 포맷(메시지는 최근 n개로 상한). */
    public static SnapshotState from(SessionState state, int recentN) {
        List<ParticipantState> participants = state.participantList();
        List<MessageState> recent = state.recentMessages(recentN);
        long messageCount = state.messages().values().stream()
                .filter(m -> !"DELETED".equals(m.state()))
                .count();
        int joinedCount = (int) participants.stream()
                .filter(p -> "JOINED".equals(p.status()))
                .count();
        return new SnapshotState(
                participants,
                recent,
                new Counts(joinedCount, messageCount));
    }

    /** 스냅샷 포맷 → {@link SessionState}(복원 base). recentMessages만 적재(설계서 §3.3). */
    public SessionState toSessionState() {
        SessionState state = new SessionState();
        if (participants != null) {
            for (ParticipantState p : participants) {
                state.putParticipant(p);
            }
        }
        if (recentMessages != null) {
            for (MessageState m : recentMessages) {
                state.putMessage(m);
            }
        }
        return state;
    }
}
