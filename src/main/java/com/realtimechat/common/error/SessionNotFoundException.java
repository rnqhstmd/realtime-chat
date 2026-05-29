package com.realtimechat.common.error;

import java.util.UUID;

/**
 * 존재하지 않는 세션에 대한 작업 요청 시 발생. {@code 404 Not Found}로 매핑된다.
 */
public class SessionNotFoundException extends RuntimeException {

    public SessionNotFoundException(UUID sessionId) {
        super("Session not found: " + sessionId);
    }

    public SessionNotFoundException(String message) {
        super(message);
    }
}
