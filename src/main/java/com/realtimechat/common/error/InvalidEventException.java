package com.realtimechat.common.error;

/**
 * 이벤트 수집 시 요청이 잘못된 경우(누락된 멱등 키, 잘못된 payload 등) 발생.
 * {@code 400 Bad Request}로 매핑된다.
 */
public class InvalidEventException extends RuntimeException {

    public InvalidEventException(String message) {
        super(message);
    }

    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
