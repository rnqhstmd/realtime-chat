package com.realtimechat.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.realtimechat.event.StoredEvent;
import com.realtimechat.support.AbstractFaultInjectionTest;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * FI-3 데이터 유실 / 부분 실패 장애 주입 테스트 — 설계서 §9.3 (최고 임팩트, Outbox 패턴 핵심 가치).
 *
 * <p>Toxiproxy로 Redis 연결을 차단하여 메시징 인프라 장애를 재현한다. 차단 중에도 이벤트 수집(append)은
 * DB 트랜잭션만 사용하므로 <b>성공</b>하고(이벤트 유실 0 — BR-4), outbox에 미발행으로 적체된다. 복구
 * 후 {@code OutboxRelay}가 미발행분을 자동 재발행하여 read model이 event store 기준과 완전히 일치할
 * 때까지 회복되는지 검증한다.
 *
 * <p>설계서 §9.3 "완화: Outbox로 이벤트 유실 방지(커밋=발행 보장) / 복구: event store가 진실의
 * 원천이므로 read model을 언제든 replay로 재구축"을 실증한다. QE-2(Redis 일시 장애 후 outbox 자동
 * 재발행)에 정확히 대응한다.
 *
 * <p>테스트 프로파일은 {@code chat.fanout.mode=local}이라 append 경로의 afterCommit broadcast가 Redis
 * Pub/Sub을 사용하지 않는다(로컬 STOMP). 따라서 Redis 차단이 append를 막지 않는다.
 */
class RedisOutageFaultTest extends AbstractFaultInjectionTest {

    @Test
    @DisplayName("FI-3: Redis 차단 중 append 성공·outbox 적체(유실 0), 복구 후 relay 재발행으로 정합성 회복")
    void redisOutageThenRecover() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        // when(장애): Redis 연결 차단(발행·소비 경로 모두 단절).
        cutRedis();

        // when(부하) + then(유실 0): 차단 중 5건 append. append는 DB만 쓰므로 모두 성공하고 seq가 연속된다.
        int total = 5;
        for (int i = 1; i <= total; i++) {
            StoredEvent e = sendMessage(sessionId, sender, "during-redis-cut-" + i, "fi3-" + i);
            assertThat(e.seq()).isEqualTo((long) i);
        }

        // event store에 5건 모두 기록(유실 0).
        assertThat(eventStore.findBySeqRange(sessionId, 0L, Long.MAX_VALUE)).hasSize(total);

        // then(감지): 차단 중에는 relay XADD가 실패하므로 outbox 미발행이 5건으로 적체되고(1초간 유지),
        // 소비자가 stream을 읽지 못해 read model은 비어 있다 — 결정적.
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(unpublishedOutboxCount(sessionId)).isEqualTo(total));
        assertThat(messageViewDao.findRecent(sessionId, 50)).isEmpty();

        // when(복구): Redis 복구.
        healRedis();

        // then(복구): relay가 미발행분을 재발행 → worker 소비 → read model이 event store 기준 5건으로
        // 완전 일치하고, 미발행 outbox가 0이 된다.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(messageViewDao.findRecent(sessionId, 50)).hasSize(total);
            assertThat(unpublishedOutboxCount(sessionId)).isZero();
            assertThat(lastAppliedSeq(sessionId)).isEqualTo((long) total);
        });
    }
}
