package com.realtimechat.common.web;

import com.realtimechat.common.error.InvalidEventException;

/**
 * limit 파라미터 공용 검증 유틸. 기존 {@code QueryController}의 검증 로직을 추출해 여러
 * 컨트롤러가 동일 규칙(1~{@value #MAX_LIMIT})을 공유하도록 한다(설계서 §5-C).
 *
 * <p>상한·예외·메시지는 기존 동작과 동일하게 유지한다(회귀 방지).
 */
public final class LimitSupport {

    /** limit 파라미터 상한(RISK 보강). 과도한 N으로 인한 메모리/응답 비대화를 막는다. */
    public static final int MAX_LIMIT = 500;

    private LimitSupport() {
    }

    /**
     * limit 범위 검증(1~{@value #MAX_LIMIT}). 범위 밖이면 {@link InvalidEventException}(400).
     * 미지정 기본값은 호출부에서 결정하므로 여기서는 명시된 값만 검증한다.
     */
    public static int validate(int limit) {
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new InvalidEventException(
                    "Invalid 'limit': must be between 1 and " + MAX_LIMIT + ", got: " + limit);
        }
        return limit;
    }
}
