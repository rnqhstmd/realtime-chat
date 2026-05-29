package com.realtimechat.integration;

import static com.realtimechat.support.AwaitProjection.awaitProjection;
import static org.assertj.core.api.Assertions.assertThat;

import com.realtimechat.event.EventType;
import com.realtimechat.support.AbstractIntegrationTest;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * REST 엔드포인트 스모크 통합 테스트(설계서 §11, PRD R1/R2/R6).
 *
 * <p>{@link TestRestTemplate}로 실제 HTTP 경유 주요 엔드포인트의 상태코드·핵심 본문을 검증한다.
 * 컨트롤러 바인딩, {@code Idempotency-Key} 헤더 처리, 전역 예외 매핑(400)이 실 환경에서 동작함을
 * 확인하는 것이 목적이다.
 */
class RestApiIntegrationTest extends AbstractIntegrationTest {

    @Autowired private TestRestTemplate rest;

    private static HttpHeaders json() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return h;
    }

    private static HttpHeaders jsonWithKey(String key) {
        HttpHeaders h = json();
        h.set("Idempotency-Key", key);
        return h;
    }

    @SuppressWarnings("unchecked")
    private UUID createSession() {
        ResponseEntity<Map> resp = rest.postForEntity("/sessions", null, Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return UUID.fromString((String) resp.getBody().get("sessionId"));
    }

    @Test
    @DisplayName("POST /sessions → 201, sessionId 반환")
    void createSessionReturns201() {
        ResponseEntity<Map> resp = rest.postForEntity("/sessions", null, Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(resp.getBody()).containsKey("sessionId");
    }

    @Test
    @DisplayName("POST /sessions/{id}/join → 200, PARTICIPANT_JOINED + seq=1")
    void joinReturnsEvent() {
        UUID sessionId = createSession();
        UUID participant = UUID.randomUUID();

        HttpEntity<Map<String, Object>> req =
                new HttpEntity<>(Map.of("participantId", participant.toString()),
                        jsonWithKey("join-" + participant));

        ResponseEntity<Map> resp = rest.postForEntity(
                "/sessions/" + sessionId + "/join", req, Map.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody().get("type")).isEqualTo(EventType.PARTICIPANT_JOINED.name());
        assertThat(((Number) resp.getBody().get("seq")).longValue()).isEqualTo(1L);
    }

    @Test
    @DisplayName("POST /sessions/{id}/events → 200 (MESSAGE_SENT 수집)")
    void collectEventReturns200() {
        UUID sessionId = createSession();
        UUID sender = UUID.randomUUID();

        Map<String, Object> body = Map.of(
                "type", EventType.MESSAGE_SENT.name(),
                "actorId", sender.toString(),
                "payload", Map.of("senderId", sender.toString(), "content", "hi via rest"));

        HttpEntity<Map<String, Object>> req = new HttpEntity<>(body, jsonWithKey("rest-msg-1"));
        ResponseEntity<Map> resp = rest.postForEntity(
                "/sessions/" + sessionId + "/events", req, Map.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody().get("type")).isEqualTo(EventType.MESSAGE_SENT.name());
    }

    @Test
    @DisplayName("POST /sessions/{id}/events without Idempotency-Key → 400")
    void collectEventMissingKeyReturns400() {
        UUID sessionId = createSession();

        Map<String, Object> body = Map.of(
                "type", EventType.MESSAGE_SENT.name(),
                "payload", Map.of("content", "no key"));

        // Idempotency-Key 헤더 누락
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(body, json());
        ResponseEntity<Map> resp = rest.postForEntity(
                "/sessions/" + sessionId + "/events", req, Map.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("GET /sessions/{id}/timeline → 200, 복원 본문(participants/messages)")
    void timelineReturns200() {
        UUID sessionId = createSession();
        UUID alice = UUID.randomUUID();

        // join + message via REST
        rest.postForEntity("/sessions/" + sessionId + "/join",
                new HttpEntity<>(Map.of("participantId", alice.toString()),
                        jsonWithKey("tj-" + alice)), Map.class);
        rest.postForEntity("/sessions/" + sessionId + "/events",
                new HttpEntity<>(Map.of(
                        "type", EventType.MESSAGE_SENT.name(),
                        "actorId", alice.toString(),
                        "payload", Map.of("senderId", alice.toString(), "content", "tl")),
                        jsonWithKey("tm-1")), Map.class);

        ResponseEntity<Map> resp = rest.getForEntity(
                "/sessions/" + sessionId + "/timeline", Map.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody()).containsKeys("participants", "messages");
        assertThat((List<?>) resp.getBody().get("messages")).hasSize(1);
        assertThat((List<?>) resp.getBody().get("participants")).hasSize(1);
    }

    @Test
    @DisplayName("POST /sessions/{id}/snapshots → 202")
    void snapshotReturns202() {
        UUID sessionId = createSession();
        UUID alice = UUID.randomUUID();
        rest.postForEntity("/sessions/" + sessionId + "/join",
                new HttpEntity<>(Map.of("participantId", alice.toString()),
                        jsonWithKey("sj-" + alice)), Map.class);

        ResponseEntity<Void> resp = rest.postForEntity(
                "/sessions/" + sessionId + "/snapshots", null, Void.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }

    @Test
    @DisplayName("POST /sessions/{unknown}/snapshots → 404 (없는 세션)")
    void snapshotUnknownSessionReturns404() {
        UUID unknown = UUID.randomUUID();
        ResponseEntity<Map> resp = rest.postForEntity(
                "/sessions/" + unknown + "/snapshots", null, Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("GET /timeline?limit=0 / limit=501 → 400 (limit 범위 밖)")
    void timelineLimitOutOfRangeReturns400() {
        UUID sessionId = createSession();

        ResponseEntity<Map> tooLow = rest.getForEntity(
                "/sessions/" + sessionId + "/timeline?limit=0", Map.class);
        assertThat(tooLow.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<Map> tooHigh = rest.getForEntity(
                "/sessions/" + sessionId + "/timeline?limit=501", Map.class);
        assertThat(tooHigh.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("GET /timeline?at=<Long초과 숫자> → 400 (seq out of range)")
    void timelineSeqOverflowReturns400() {
        UUID sessionId = createSession();

        // 20자리 숫자: Long.MAX_VALUE(19자리) 초과 → NumberFormatException → 400(catch-all 500 아님)
        ResponseEntity<Map> resp = rest.getForEntity(
                "/sessions/" + sessionId + "/timeline?at=99999999999999999999", Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("GET /messages?limit=0 / limit=501 → 400 (limit 범위 밖)")
    void messagesLimitOutOfRangeReturns400() {
        UUID sessionId = createSession();

        ResponseEntity<Map> tooLow = rest.getForEntity(
                "/sessions/" + sessionId + "/messages?limit=0", Map.class);
        assertThat(tooLow.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<Map> tooHigh = rest.getForEntity(
                "/sessions/" + sessionId + "/messages?limit=501", Map.class);
        assertThat(tooHigh.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("GET /sessions?status=BOGUS → 400 (허용되지 않는 status)")
    void listInvalidStatusReturns400() {
        ResponseEntity<Map> resp = rest.getForEntity("/sessions?status=BOGUS", Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("POST /events PRESENCE_CHANGED 잘못된 presence → 400")
    void presenceChangedInvalidValueReturns400() {
        UUID sessionId = createSession();
        UUID participant = UUID.randomUUID();
        rest.postForEntity("/sessions/" + sessionId + "/join",
                new HttpEntity<>(Map.of("participantId", participant.toString()),
                        jsonWithKey("pj-" + participant)), Map.class);

        Map<String, Object> body = Map.of(
                "type", EventType.PRESENCE_CHANGED.name(),
                "actorId", participant.toString(),
                "payload", Map.of("participantId", participant.toString(), "presence", "BOGUS"));

        ResponseEntity<Map> resp = rest.postForEntity(
                "/sessions/" + sessionId + "/events",
                new HttpEntity<>(body, jsonWithKey("pres-bad-1")), Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("POST /sessions/{id}/end → 200, GET /sessions?status=ENDED 에 포함")
    void endAndListByStatus() {
        UUID sessionId = createSession();

        ResponseEntity<Void> endResp = rest.exchange(
                "/sessions/" + sessionId + "/end", HttpMethod.POST,
                new HttpEntity<>(null), Void.class);
        assertThat(endResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        // 비동기 projection 반영 대기: session_view status=ENDED 반영 후 목록 조회
        awaitProjection(() -> {
            @SuppressWarnings("unchecked")
            ResponseEntity<List<Map<String, Object>>> listResp = rest.exchange(
                    "/sessions?status=ENDED", HttpMethod.GET, new HttpEntity<>(null),
                    (Class<List<Map<String, Object>>>) (Class<?>) List.class);
            assertThat(listResp.getStatusCode()).isEqualTo(HttpStatus.OK);
            boolean found = listResp.getBody().stream()
                    .anyMatch(m -> sessionId.toString().equals(m.get("sessionId")));
            assertThat(found).isTrue();
        });
    }
}
