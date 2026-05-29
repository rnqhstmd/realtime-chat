package com.realtimechat.realtime;

/**
 * STOMP inbound 처리 중 발생한 오류를 클라이언트에게 전달하는 에러 DTO(설계서 §6).
 *
 * <p>{@code @RestControllerAdvice}는 REST(HTTP) 예외만 처리하고 STOMP {@code @MessageMapping}
 * 예외는 잡지 못한다. {@link StompEventController}의 {@code @MessageExceptionHandler}가 이 DTO를
 * {@code @SendToUser("/queue/errors")}로 보내, 클라이언트가 {@code /user/queue/errors} 구독으로
 * 수신한다. 내부 상세(스택/예외 클래스)는 노출하지 않는다.
 *
 * @param error   오류 분류(예: {@code INVALID_EVENT}, {@code SESSION_NOT_FOUND}, {@code ERROR})
 * @param message 사용자에게 보여줄 메시지(내부 상세 미포함)
 */
public record StompError(String error, String message) {
}
