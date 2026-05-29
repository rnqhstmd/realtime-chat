package com.realtimechat.session;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.command.CommandHandler;
import com.realtimechat.common.error.InvalidEventException;
import com.realtimechat.common.error.SessionNotFoundException;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
import com.realtimechat.event.payload.ParticipantJoinedPayload;
import com.realtimechat.projection.SessionViewDao;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 세션 라이프사이클 서비스(설계서 §11: 생성/참여/종료/조회).
 *
 * <p>생성·종료는 쓰기 모델({@code session})과 읽기 모델({@code session_view})을 한 트랜잭션에서
 * 함께 갱신한다. 참여(join)는 별도 분기 없이 {@link CommandHandler}로 위임하여, 모든 이벤트
 * 수집이 단일 수렴점(§6, §11)을 거치도록 한다. 조회는 읽기 모델(SessionViewDao)에 직접 위임한다.
 */
@Service
public class SessionService {

    private static final String STATUS_ACTIVE = "ACTIVE";
    private static final String STATUS_ENDED = "ENDED";

    private final SessionDao sessionDao;
    private final SessionViewDao sessionViewDao;
    private final CommandHandler commandHandler;

    public SessionService(SessionDao sessionDao,
                          SessionViewDao sessionViewDao,
                          CommandHandler commandHandler) {
        this.sessionDao = sessionDao;
        this.sessionViewDao = sessionViewDao;
        this.commandHandler = commandHandler;
    }

    /** 세션 생성(설계서 §11 {@code POST /sessions}). 쓰기/읽기 모델 행을 함께 만든다. */
    @Transactional
    public UUID createSession() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        sessionDao.create(id, now);
        sessionViewDao.insertIfAbsent(id, now);
        return id;
    }

    /**
     * 참여(설계서 §11 {@code POST /sessions/{id}/join}). PARTICIPANT_JOINED 이벤트로 표현되며
     * 수집·멱등·순서·projection·broadcast 일체를 {@link CommandHandler}에 위임한다.
     */
    public StoredEvent join(UUID sessionId, UUID participantId, String idempotencyKey) {
        JsonNode payload = JsonUtil.toJsonNode(new ParticipantJoinedPayload(participantId));
        return commandHandler.handle(
                sessionId, EventType.PARTICIPANT_JOINED, payload, idempotencyKey, participantId);
    }

    /** 세션 종료(설계서 §11 {@code POST /sessions/{id}/end}). 미존재면 404. */
    @Transactional
    public void end(UUID sessionId) {
        if (!sessionDao.exists(sessionId)) {
            throw new SessionNotFoundException(sessionId);
        }
        Instant now = Instant.now();
        sessionDao.markEnded(sessionId, now);
        sessionViewDao.markEnded(sessionId, now);
    }

    /** 세션 단건 조회(설계서 §11). 읽기 모델 직접 조회. */
    public Optional<SessionViewDao.SessionRow> get(UUID sessionId) {
        return sessionViewDao.find(sessionId);
    }

    /**
     * 세션 목록 조회(설계서 §11 {@code GET /sessions}). status가 null이면 전체.
     * status가 지정됐으나 {@code ACTIVE}/{@code ENDED}가 아니면 {@link InvalidEventException}(400).
     */
    public List<SessionViewDao.SessionRow> list(String status) {
        if (status != null && !STATUS_ACTIVE.equals(status) && !STATUS_ENDED.equals(status)) {
            throw new InvalidEventException(
                    "Invalid 'status': must be " + STATUS_ACTIVE + " or " + STATUS_ENDED + ", got: " + status);
        }
        return sessionViewDao.list(status);
    }
}
