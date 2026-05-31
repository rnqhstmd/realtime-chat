package com.realtimechat.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.realtimechat.support.AbstractIntegrationTest;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Actuator 엔드포인트 통합 테스트(설계서 §5-D 관측성, PRD R7, AC-12/AC-13).
 *
 * <p>{@code management.endpoints.web.exposure.include=health,info,prometheus}와
 * {@code endpoint.health.show-details=always} 설정이 실 환경에서 노출되는지 검증한다.
 *
 * <p>메트릭 이름은 Micrometer→Prometheus 명명 규칙으로 변환됨에 유의한다(점→밑줄, Counter는
 * {@code _total} 접미사). 소스({@code ProjectionLagMetrics})의 등록명 기준:
 * <ul>
 *   <li>{@code chat.projection.lag.millis}(Gauge) → {@code chat_projection_lag_millis}</li>
 *   <li>{@code chat.projection.dlq.failures}(Counter) → {@code chat_projection_dlq_failures_total}</li>
 *   <li>{@code chat.stream.processed}(Counter) → {@code chat_stream_processed_total}</li>
 *   <li>{@code chat.outbox.relayed}(Counter) → {@code chat_outbox_relayed_total}</li>
 * </ul>
 * 카운터/게이지는 {@code ProjectionLagMetrics} 생성자에서 등록되므로 값이 0이어도 노출된다.
 */
class ActuatorEndpointIntegrationTest extends AbstractIntegrationTest {

    @Autowired private TestRestTemplate rest;

    @Test
    @DisplayName("AC-12: GET /actuator/prometheus → 200, chat 메트릭 이름 노출")
    void prometheusExposesChatMetrics() {
        ResponseEntity<String> resp = rest.getForEntity("/actuator/prometheus", String.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        String body = resp.getBody();
        assertThat(body).isNotNull();

        // gauge: 점→밑줄 변환. 라벨 형식은 환경차가 있을 수 있어 메트릭명 substring으로 단언.
        assertThat(body).contains("chat_projection_lag_millis");
        // counter: _total 접미사. 생성자 등록이라 값 0이어도 HELP/TYPE 라인으로 노출.
        assertThat(body).contains("chat_projection_dlq_failures_total");
        assertThat(body).contains("chat_stream_processed_total");
        assertThat(body).contains("chat_outbox_relayed_total");
    }

    @SuppressWarnings("unchecked")
    @Test
    @DisplayName("AC-13: GET /actuator/health → 200, status=UP, components(db/redis) UP")
    void healthShowsDetailsWithComponents() {
        ResponseEntity<Map> resp = rest.getForEntity("/actuator/health", Map.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        Map<String, Object> body = resp.getBody();
        assertThat(body).isNotNull();
        assertThat(body.get("status")).isEqualTo("UP");

        // show-details: always → components 노출
        Map<String, Object> components = (Map<String, Object>) body.get("components");
        assertThat(components).isNotNull();
        assertThat(components).containsKeys("db", "redis");

        Map<String, Object> db = (Map<String, Object>) components.get("db");
        assertThat(db.get("status")).isEqualTo("UP");

        Map<String, Object> redis = (Map<String, Object>) components.get("redis");
        assertThat(redis.get("status")).isEqualTo("UP");
    }
}
