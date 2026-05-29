package com.realtimechat.common.web;

import com.realtimechat.common.error.InvalidEventException;

/**
 * 멱등 키 관련 상수와 검증 헬퍼.
 *
 * <p>실제 HTTP 헤더 바인딩은 컨트롤러(B5)에서 수행하며, 여기서는 헤더명 상수와
 * 공통 검증 규칙(공백 금지)만 제공해 수집 경로(REST/WS) 전반의 일관성을 보장한다.
 */
public final class IdempotencyKeys {

    /** 멱등 키 전달용 HTTP 헤더명(HTTP 표준 관행). */
    public static final String HEADER = "Idempotency-Key";

    /** 멱등 키 최대 길이(설계 가정 보강). 초과 시 400으로 거부해 비정상적으로 긴 키 저장을 막는다. */
    public static final int MAX_LENGTH = 200;

    private IdempotencyKeys() {
    }

    /**
     * 멱등 키가 유효한지 검증한다. null/공백이거나 {@value #MAX_LENGTH}자 초과면
     * {@link InvalidEventException}을 던진다.
     *
     * @param key 검증할 멱등 키
     * @return 입력값 그대로(검증 통과 시)
     */
    public static String require(String key) {
        if (key == null || key.isBlank()) {
            throw new InvalidEventException("Missing or blank header: " + HEADER);
        }
        if (key.length() > MAX_LENGTH) {
            throw new InvalidEventException(HEADER + " too long: max " + MAX_LENGTH + " chars");
        }
        return key;
    }

    public static boolean isValid(String key) {
        return key != null && !key.isBlank();
    }
}
