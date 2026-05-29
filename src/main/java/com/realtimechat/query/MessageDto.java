package com.realtimechat.query;

import com.realtimechat.projection.MessageViewDao;
import com.realtimechat.restore.SessionState;
import java.time.Instant;
import java.util.UUID;

/**
 * 메시지 표현(설계서 §4.3 복원 반환 / §10 Q2 라이브 조회 공통).
 *
 * <p>복원 타임라인({@link SessionState.MessageState})과 라이브 최근 조회
 * ({@link MessageViewDao.MessageRow}) 양쪽 모두 동일한 외부 형태로 노출한다. 각 메시지의
 * state(SENT/EDITED/DELETED)를 포함한다(설계서 §4.3 "각 메시지 상태").
 *
 * @param messageId 메시지 식별자
 * @param seq       생성 seq(정렬 키)
 * @param senderId  발신자 식별자
 * @param content   본문(EDITED 시 갱신)
 * @param state     메시지 상태(SENT / EDITED / DELETED)
 * @param createdAt 생성 시각
 */
public record MessageDto(
        UUID messageId,
        long seq,
        UUID senderId,
        String content,
        String state,
        Instant createdAt) {

    /** 복원 상태의 메시지 표현을 API DTO로 매핑한다. */
    public static MessageDto from(SessionState.MessageState m) {
        return new MessageDto(
                m.messageId(),
                m.seq(),
                m.senderId(),
                m.content(),
                m.state(),
                m.createdAt());
    }

    /** 라이브 읽기 모델 행을 API DTO로 매핑한다(§10 Q2). */
    public static MessageDto from(MessageViewDao.MessageRow row) {
        return new MessageDto(
                row.messageId(),
                row.seq(),
                row.senderId(),
                row.content(),
                row.state(),
                row.createdAt());
    }
}
