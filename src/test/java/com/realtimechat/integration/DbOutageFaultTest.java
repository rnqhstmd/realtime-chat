package com.realtimechat.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.realtimechat.event.StoredEvent;
import com.realtimechat.support.AbstractFaultInjectionTest;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionException;

/**
 * FI-2 DB 장애 / 성능 저하 장애 주입 테스트 — 설계서 §9.2.
 *
 * <p>Toxiproxy로 PostgreSQL 연결을 차단하여 DB 장애를 재현한다. 차단 중 이벤트 수집(append)이
 * 실패하되 <b>부분 저장이 발생하지 않고</b>(event INSERT + outbox INSERT가 한 트랜잭션 — BR-1),
 * 복구 후 즉시 정상화되는지 검증한다. seq 채번도 트랜잭션에 묶여 실패 시 소비되지 않으므로(롤백),
 * 복구 후 seq 연속성이 유지된다.
 *
 * <p>설계서 §9.2 "완화: 커넥션풀 상한+statement timeout / 복구: 백프레셔로 유입 조절"을 실증한다.
 * 락 경합 정밀 재현은 타이밍 의존적 flaky 위험이 커 설계서 §9.2 문서 분석으로 갈음한다(설계서 §5.2 주).
 */
class DbOutageFaultTest extends AbstractFaultInjectionTest {

    @Test
    @DisplayName("FI-2: DB 차단 시 append 실패(부분 저장 없음·seq 미소비), 복구 후 정상 append·projection")
    void dbOutageThenRecover() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        // given: 정상 append 1건(seq=1).
        StoredEvent first = sendMessage(sessionId, sender, "before-db-cut", "fi2-before");
        assertThat(first.seq()).isEqualTo(1L);

        // when(장애): DB 연결 차단.
        cutDb();

        // then(감지): append 시도는 DB 접근 실패로 예외를 던진다. cut 타이밍에 따라 커넥션 획득
        // 실패는 DataAccessException(예: CannotGetJdbcConnectionException)으로, 트랜잭션 내 쿼리 실패
        // 후 롤백마저 실패하면 TransactionException(TransactionSystemException: JDBC rollback failed)으로
        // 나타나므로 두 계열을 모두 허용한다.
        assertThatThrownBy(() -> sendMessage(sessionId, sender, "during-db-cut", "fi2-during"))
                .isInstanceOfAny(DataAccessException.class, TransactionException.class);

        // when(복구): DB 연결 복구.
        healDb();

        // then(복구): 끊긴 커넥션이 정리되고 새 커넥션이 확보될 때까지 재시도하며 정상 append를 확인한다.
        // 멱등 키 고정이므로 재시도가 중복을 만들지 않는다(계층1 멱등). 차단 중 실패는 seq를 소비하지
        // 않았으므로(트랜잭션 롤백) 다음 정상 append는 seq=2여야 한다.
        await().atMost(Duration.ofSeconds(15)).ignoreExceptions().untilAsserted(() -> {
            StoredEvent recovered = sendMessage(sessionId, sender, "after-db-heal", "fi2-after");
            assertThat(recovered.seq()).isEqualTo(2L);
        });

        // event store에 정확히 2건(before + after)만 존재 — 차단 중 부분 저장 없음.
        assertThat(eventStore.findBySeqRange(sessionId, 0L, Long.MAX_VALUE)).hasSize(2);

        // 복구 후 비동기 projection도 2건 정상 반영.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(messageViewDao.findRecent(sessionId, 50)).hasSize(2));
    }
}
