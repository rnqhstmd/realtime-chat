package com.realtimechat.presence;

import com.realtimechat.common.error.SessionNotFoundException;
import com.realtimechat.session.SessionDao;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * heartbeat ping 수신(설계서 §5-B, FR-P3-2).
 *
 * <p>heartbeat는 <b>이벤트가 아닌 경량 ping</b>이다(PRD deviation, 결정 A). 따라서
 * {@link com.realtimechat.command.CommandHandler}/append/broadcast를 경유하지 않고
 * {@link PresenceTracker#touch}로 Redis liveness 키 TTL만 갱신한다. heartbeat를 이벤트화하면
 * 30s마다 참여자 수만큼 event/outbox/stream/팬아웃이 돌고 last_seq 폭증·resume delta 오염이
 * 발생하므로(critic #2/#3), 의도적으로 수집 경로에서 분리한다.
 *
 * <p>REST({@code POST /sessions/{id}/heartbeat})와 STOMP({@code /app/session/{id}/heartbeat})
 * 두 경로 모두 동일한 {@link PresenceTracker#touch}만 호출한다.
 */
@Controller
public class PresenceController {

    private static final Logger log = LoggerFactory.getLogger(PresenceController.class);

    private final PresenceTracker presenceTracker;
    private final SessionDao sessionDao;

    public PresenceController(PresenceTracker presenceTracker, SessionDao sessionDao) {
        this.presenceTracker = presenceTracker;
        this.sessionDao = sessionDao;
    }

    /**
     * heartbeat ping(REST, 설계서 §5-B). 세션 미존재면 404, 그 외에는 liveness 키 TTL을 갱신하고
     * {@code 204 No Content}를 반환한다. 멱등 키·append·broadcast 없이 {@code touch}만 수행한다.
     *
     * @param id      대상 세션
     * @param request 참여자 식별자만 담는 heartbeat 본문(멱등 키 불요 — 이벤트가 아니므로)
     */
    @PostMapping("/sessions/{id}/heartbeat")
    @ResponseBody
    public ResponseEntity<Void> heartbeat(@PathVariable UUID id, @Valid @RequestBody HeartbeatRequest request) {
        if (sessionDao.status(id).isEmpty()) {
            throw new SessionNotFoundException(id);
        }
        presenceTracker.touch(id, request.participantId());
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }

    /**
     * heartbeat ping(STOMP, 설계서 §5-B). {@link StompEventController} 수집 패턴과 동일하게
     * {@code @MessageMapping} + {@code @DestinationVariable}을 사용하되, 이벤트가 아니므로
     * CommandHandler를 경유하지 않고 {@code touch}만 한다. 응답 없음(ping).
     *
     * @param sessionId 대상 세션({@code @DestinationVariable})
     * @param msg       참여자 식별자만 담는 heartbeat 본문
     */
    @MessageMapping("/session/{sessionId}/heartbeat")
    public void heartbeatOverStomp(@DestinationVariable UUID sessionId, HeartbeatRequest msg) {
        // @MessageMapping은 Bean Validation을 자동 적용하지 않아 @NotNull이 강제되지 않는다.
        // null이면 touch에서 NPE/`presence:{sid}:null` 키가 되므로 touch 없이 무시한다.
        if (msg == null || msg.participantId() == null) {
            log.warn("STOMP heartbeat: participantId 누락 무시 sessionId={}", sessionId);
            return;
        }
        // REST heartbeat와 일관되게 미존재 세션은 무시한다. STOMP는 HTTP 응답이 없으므로
        // 404 대신 silent ignore + 로그로 처리한다.
        if (sessionDao.status(sessionId).isEmpty()) {
            log.warn("STOMP heartbeat: 미존재 세션 무시 sessionId={}", sessionId);
            return;
        }
        presenceTracker.touch(sessionId, msg.participantId());
    }

    /**
     * heartbeat ping 본문(설계서 §5-B). heartbeat는 이벤트가 아니므로 멱등 키 없이 참여자
     * 식별자만 담는다(join 요청의 {@code JoinRequest}와 동일한 형태).
     *
     * @param participantId 참여자 식별자(필수)
     */
    public record HeartbeatRequest(
            @NotNull UUID participantId) {
    }
}
