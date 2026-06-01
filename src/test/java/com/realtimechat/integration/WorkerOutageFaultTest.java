package com.realtimechat.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.realtimechat.projection.async.ProjectionWorker;
import com.realtimechat.support.AbstractFaultInjectionTest;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * FI-1 서버 다운(인스턴스 장애) 장애 주입 테스트 — 설계서 §9.1.
 *
 * <p>Projection Worker를 소비 인스턴스로 보고, {@link ProjectionWorker#stop()}으로 "인스턴스 다운"을
 * 재현한다. 다운 중 수집된 이벤트가 재기동({@link ProjectionWorker#start()}) 후 누락 없이 read model에
 * 반영되는지 검증한다. event store가 진실의 원천이므로(원칙 P-5), 복구 성공 = read model이 event store를
 * 따라잡음으로 정의한다.
 *
 * <p>설계서 §9.1 "복구: stateless라 재기동 즉시 합류. 죽은 인스턴스의 stream pending은 XAUTOCLAIM으로
 * 타 인스턴스가 이어받아 처리"를 실증한다. 단일 JVM 내 컴포넌트 재기동으로 근사한다(다중 JVM 페일오버는
 * 비범위).
 */
class WorkerOutageFaultTest extends AbstractFaultInjectionTest {

    @Autowired private ProjectionWorker projectionWorker;

    /** 공유 컨텍스트 보호: 다음 테스트/클래스를 위해 worker를 반드시 재가동 상태로 되돌린다. */
    @AfterEach
    void ensureWorkerRunning() {
        if (!projectionWorker.isRunning()) {
            projectionWorker.start();
        }
    }

    @Test
    @DisplayName("FI-1: worker 다운 중 수집된 이벤트가 재기동 후 누락 없이 read model에 반영(유실 0)")
    void workerDownThenRecoverWithoutLoss() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        // given: worker 정상 가동 상태에서 1건이 정상 반영됨을 확인(파이프라인 살아있음).
        sendMessage(sessionId, sender, "before-outage", "fi1-before");
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(messageViewDao.findRecent(sessionId, 50)).hasSize(1));

        // when(장애): 소비 인스턴스 다운.
        projectionWorker.stop();
        assertThat(projectionWorker.isRunning()).isFalse();

        // when(부하): 다운 중 5건 append. append/outbox 기록은 정상(소비자만 없음).
        int during = 5;
        for (int i = 1; i <= during; i++) {
            sendMessage(sessionId, sender, "during-outage-" + i, "fi1-during-" + i);
        }

        // then(다운 중): worker가 멈췄으므로 1초간 read model이 전진하지 않는다(여전히 1건). relay는
        // stream에 적재하지만 소비자가 없어 미반영이다. 이 부정 단언은 "상한 시간 내 미전진"만 보장하며,
        // worker 멈춤 자체는 위의 isRunning()==false로 직접 단언해 보완한다. atMost는 느린 CI 여유로 4초.
        await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(4)).untilAsserted(() ->
                assertThat(messageViewDao.findRecent(sessionId, 50)).hasSize(1));

        // when(복구): 인스턴스 재기동.
        projectionWorker.start();
        assertThat(projectionWorker.isRunning()).isTrue();

        // then(복구): 다운 중 수집분(5건) + 기존(1건) = 6건이 누락 없이 반영되고 offset이 전진한다.
        // 미전달 메시지는 신규 XREADGROUP `>`로, stop이 소비 도중 끊었다면 재청구(XPENDING/XCLAIM)
        // 경로로 처리되며, 최종 단언은 두 경로의 합집합이다(유실 0).
        long expected = 1L + during;
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(messageViewDao.findRecent(sessionId, 50)).hasSize((int) expected);
            assertThat(lastAppliedSeq(sessionId)).isEqualTo(expected);
        });

        // event store 기준선과 read model 일치(유실 0).
        assertThat(eventStore.findBySeqRange(sessionId, 0L, Long.MAX_VALUE)).hasSize((int) expected);
        // 복구 후 미발행 outbox도 0(relay가 모두 발행 완료).
        assertThat(unpublishedOutboxCount(sessionId)).isZero();
    }
}
