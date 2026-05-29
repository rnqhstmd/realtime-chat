package com.realtimechat.realtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.command.CommandHandler;
import com.realtimechat.common.error.InvalidEventException;
import com.realtimechat.common.error.SessionNotFoundException;
import com.realtimechat.event.EventType;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.annotation.SendToUser;
import org.springframework.stereotype.Controller;

/**
 * STOMP inbound SEND 핸들러(설계서 §6, §11 "REST/WS 단일 command handler 수렴").
 *
 * <p>클라이언트가 {@code /app/session/{sessionId}/events}로 보낸 메시지를 REST
 * {@code POST /sessions/{id}/events}와 <b>동일한</b> {@link CommandHandler}로 수렴시킨다.
 * 수집 경로가 둘이어도 멱등·순서·검증 로직은 한 곳에 있어 일관성이 보장된다(설계서 §11).
 *
 * <p><b>발행하지 않는다</b>: 처리 결과를 {@code /topic}으로 직접 보내지 않는다. 실시간 팬아웃은
 * CommandHandler afterCommit → {@link SimpSessionBroadcaster} 단일 경로로만 수행되어
 * 중복 전달을 방지한다(설계서 §6). 이 컨트롤러는 "수집"만 담당한다.
 */
@Controller
public class StompEventController {

    private static final Logger log = LoggerFactory.getLogger(StompEventController.class);

    private final CommandHandler commandHandler;

    public StompEventController(CommandHandler commandHandler) {
        this.commandHandler = commandHandler;
    }

    /**
     * inbound SEND를 command handler로 위임한다(설계서 §6).
     *
     * @param sessionId 대상 세션({@code @DestinationVariable})
     * @param msg       인바운드 메시지 본문
     */
    @MessageMapping("/session/{sessionId}/events")
    public void collect(@DestinationVariable UUID sessionId, WsEventMessage msg) {
        commandHandler.handle(sessionId, msg.type(), msg.payload(), msg.idempotencyKey(), msg.actorId());
    }

    /**
     * STOMP inbound 처리 중 발생한 검증/세션 예외를 클라이언트에게 전달한다(설계서 §6).
     *
     * <p>{@code @RestControllerAdvice}(ApiExceptionHandler)는 STOMP {@code @MessageMapping} 예외를
     * 잡지 못한다. 이 핸들러가 {@link InvalidEventException}/{@link SessionNotFoundException}을
     * {@code /user/queue/errors}로 보내, 잘못된 입력이 조용히 무시되지 않게 한다. 메시지 본문은
     * 그대로 노출해도 사용자 입력 결함을 알리는 용도이므로 안전하다.
     */
    @MessageExceptionHandler({InvalidEventException.class, SessionNotFoundException.class})
    @SendToUser("/queue/errors")
    public StompError handleClientError(RuntimeException e) {
        String code = e instanceof SessionNotFoundException ? "SESSION_NOT_FOUND" : "INVALID_EVENT";
        return new StompError(code, e.getMessage());
    }

    /**
     * 기타 예외 처리(설계서 §6). 내부 상세(예외 클래스/스택)는 노출하지 않고 고정 메시지만 보낸다.
     * 원문은 서버 로그로만 남긴다.
     */
    @MessageExceptionHandler(Exception.class)
    @SendToUser("/queue/errors")
    public StompError handleUnexpected(Exception e) {
        log.error("Unhandled STOMP inbound exception", e);
        return new StompError("ERROR", "Failed to process message");
    }

    /**
     * STOMP inbound 메시지 본문(설계서 §6).
     *
     * <p>멱등 키를 STOMP 헤더가 아닌 본문으로 받는다 — 헤더 추출 복잡성을 회피하는 Phase 1의
     * 합리적 선택이다(REST는 {@code Idempotency-Key} 헤더 사용, §11). payload는 타입별로
     * 이질적이라 {@link JsonNode}로 받아 CommandHandler 경계에서 검증·보강한다.
     *
     * @param type           이벤트 유형
     * @param payload        타입별 payload(nullable — CommandHandler가 보강)
     * @param actorId        행위 주체(nullable)
     * @param idempotencyKey 수집 멱등 키(§4.1 계층1)
     */
    public record WsEventMessage(
            EventType type,
            JsonNode payload,
            UUID actorId,
            String idempotencyKey) {
    }
}
