package com.realtimechat.restore;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 시점 복원 상태 누적기(설계서 §4.3 fold 대상 상태).
 *
 * <p>{@link Fold}가 base 위에 이벤트를 seq 순서로 적용하며 갱신하는 가변 컨테이너다.
 * 직렬화 친화 구조(스냅샷 state JSONB ↔ 이 상태 변환)를 위해 컬렉션과 record로만 구성한다.
 *
 * <p>가변(mutable)이지만 I/O는 없다 — fold는 base 복사본 위에서 이 상태만 변형한다(순수성 유지).
 */
public final class SessionState {

    /** 참여자 상태. participant_view(설계서 §3.2)의 복원 표현. */
    public record ParticipantState(
            UUID participantId,
            String status,    // JOINED / LEFT
            String presence,  // ONLINE / OFFLINE (nullable)
            Long joinedSeq,
            Long leftSeq) {
    }

    /** 메시지 상태. message_view(설계서 §3.2)의 복원 표현. */
    public record MessageState(
            UUID messageId,
            long seq,
            UUID senderId,
            String content,
            String state,     // SENT / EDITED / DELETED
            Instant createdAt) {
    }

    /** participantId → 상태. */
    private final Map<UUID, ParticipantState> participants;

    /** messageId → 상태. 삽입(=seq) 순서 보존. */
    private final LinkedHashMap<UUID, MessageState> messages;

    public SessionState() {
        this.participants = new LinkedHashMap<>();
        this.messages = new LinkedHashMap<>();
    }

    public Map<UUID, ParticipantState> participants() {
        return participants;
    }

    public LinkedHashMap<UUID, MessageState> messages() {
        return messages;
    }

    public void putParticipant(ParticipantState participant) {
        participants.put(participant.participantId(), participant);
    }

    public ParticipantState participant(UUID participantId) {
        return participants.get(participantId);
    }

    public void putMessage(MessageState message) {
        messages.put(message.messageId(), message);
    }

    public MessageState message(UUID messageId) {
        return messages.get(messageId);
    }

    /**
     * 삭제되지 않은 메시지 중 seq 오름차순 최근 n개(설계서 §3.3 "최근 N개").
     *
     * <p>seq DESC로 정렬해 상위 n개를 취한 뒤, 결과는 seq 오름차순으로 돌려준다.
     */
    public List<MessageState> recentMessages(int n) {
        return messages.values().stream()
                .filter(m -> !"DELETED".equals(m.state()))
                .sorted(Comparator.comparingLong(MessageState::seq).reversed())
                .limit(Math.max(n, 0))
                .sorted(Comparator.comparingLong(MessageState::seq))
                .toList();
    }

    /** 참여자 목록(삽입 순서). */
    public List<ParticipantState> participantList() {
        return new ArrayList<>(participants.values());
    }
}
