package com.realtimechat.support;

import static org.awaitility.Awaitility.await;

import java.time.Duration;

/**
 * 비동기 projection 반영 대기 헬퍼(설계서 §12).
 *
 * <p>Awaitility를 얇게 감싸 통합 테스트에서 반복 사용하는 대기 패턴을 단일 메서드로 제공한다.
 * assertion 블록이 예외 없이 통과할 때까지 최대 5초간 폴링한다.
 */
public final class AwaitProjection {

    private AwaitProjection() {}

    /**
     * 비동기 projection이 반영될 때까지 최대 5초 대기. assertion 블록이 통과하면 종료.
     *
     * @param assertion 검증할 단언 블록(AssertionError 또는 예외를 던지면 재시도)
     */
    public static void awaitProjection(Runnable assertion) {
        await().atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(assertion::run);
    }
}
