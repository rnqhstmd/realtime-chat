package com.realtimechat.integration;

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
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * resume-by-seq 조회 REST 통합 테스트(설계서 §5-C / §11 {@code GET /sessions/{id}/events?afterSeq=},
 * PRD R6, AC-6~AC-11).
 *
 * <p>{@link TestRestTemplate}로 실제 HTTP 경유 resume delta 조회의 상태코드·본문(events/hasMore)과
 * 파라미터 검증(400/404)을 검증한다. 세션마다 새 sessionId로 격리하고, REST로 알려진 seq를 직접
 * 만들어(join=seq1, 이후 MESSAGE_SENT=seq2,3,...) 전역 카운트에 의존하지 않는다.
 */
class ResumeApiIntegrationTest extends AbstractIntegrationTest {

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

    /** join → PARTICIPANT_JOINED, seq=1을 만든다. */
    private void join(UUID sessionId, UUID participant) {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(
                Map.of("participantId", participant.toString()),
                jsonWithKey("join-" + participant));
        ResponseEntity<Map> resp = rest.postForEntity(
                "/sessions/" + sessionId + "/join", req, Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    /** MESSAGE_SENT 1건 수집(seq는 채번 순서대로 증가). 멱등 키는 호출마다 고유해야 한다. */
    private void sendMessage(UUID sessionId, UUID sender, String content, String idempotencyKey) {
        Map<String, Object> body = Map.of(
                "type", EventType.MESSAGE_SENT.name(),
                "actorId", sender.toString(),
                "payload", Map.of("senderId", sender.toString(), "content", content));
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(body, jsonWithKey(idempotencyKey));
        ResponseEntity<Map> resp = rest.postForEntity(
                "/sessions/" + sessionId + "/events", req, Map.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    /** join(seq1) + count건의 MESSAGE_SENT(seq2..)를 수집한 세션을 만든다. maxSeq = 1 + count. */
    private UUID seededSession(int messageCount) {
        UUID sessionId = createSession();
        UUID sender = UUID.randomUUID();
        join(sessionId, sender);
        for (int i = 0; i < messageCount; i++) {
            sendMessage(sessionId, sender, "msg-" + i, "rk-" + sessionId + "-" + i);
        }
        return sessionId;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> events(Map body) {
        return (List<Map<String, Object>>) body.get("events");
    }

    @Test
    @DisplayName("AC-6: afterSeq=2 조회 → 반환 events의 seq가 모두 >2이고 오름차순")
    void afterSeqReturnsOnlyGreaterSeqAscending() {
        // join(seq1) + 5건(seq2..6) → maxSeq=6
        UUID sessionId = seededSession(5);

        ResponseEntity<Map> resp = rest.getForEntity(
                "/sessions/" + sessionId + "/events?afterSeq=2&limit=100", Map.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<Map<String, Object>> evs = events(resp.getBody());
        assertThat(evs).isNotEmpty();
        List<Long> seqs = evs.stream().map(ev -> ((Number) ev.get("seq")).longValue()).toList();
        // 반환 seq는 모두 afterSeq(=2) 초과 + 오름차순.
        assertThat(seqs).allMatch(s -> s > 2L);
        assertThat(seqs).isSorted();
    }

    @Test
    @DisplayName("AC-7: afterSeq>=maxSeq 조회 → events 빈 배열 + hasMore=false")
    void afterSeqBeyondMaxReturnsEmpty() {
        UUID sessionId = seededSession(3); // maxSeq=4

        // 매우 큰 값으로 maxSeq 이상 보장
        ResponseEntity<Map> resp = rest.getForEntity(
                "/sessions/" + sessionId + "/events?afterSeq=1000000&limit=100", Map.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(events(resp.getBody())).isEmpty();
        assertThat((Boolean) resp.getBody().get("hasMore")).isFalse();
    }

    @Test
    @DisplayName("AC-8: afterSeq 이후 이벤트가 limit보다 많을 때 limit=2 조회 → events.size==2 + hasMore=true")
    void smallLimitReturnsPageWithHasMore() {
        // join(seq1) + 5건(seq2..6) → afterSeq=1 이후 5건 존재, limit=2
        UUID sessionId = seededSession(5);

        ResponseEntity<Map> resp = rest.getForEntity(
                "/sessions/" + sessionId + "/events?afterSeq=1&limit=2", Map.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(events(resp.getBody())).hasSize(2);
        assertThat((Boolean) resp.getBody().get("hasMore")).isTrue();
    }

    @Test
    @DisplayName("AC-9: afterSeq 파라미터 없이 조회 → 400")
    void missingAfterSeqReturns400() {
        UUID sessionId = seededSession(2);

        ResponseEntity<Map> resp = rest.getForEntity(
                "/sessions/" + sessionId + "/events?limit=10", Map.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("AC-10: limit=0 / limit=501 → 400 (limit 범위 밖)")
    void limitOutOfRangeReturns400() {
        UUID sessionId = seededSession(2);

        ResponseEntity<Map> tooLow = rest.getForEntity(
                "/sessions/" + sessionId + "/events?afterSeq=0&limit=0", Map.class);
        assertThat(tooLow.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<Map> tooHigh = rest.getForEntity(
                "/sessions/" + sessionId + "/events?afterSeq=0&limit=501", Map.class);
        assertThat(tooHigh.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("AC-11: 존재하지 않는 세션으로 조회 → 404")
    void unknownSessionReturns404() {
        UUID unknown = UUID.randomUUID();

        ResponseEntity<Map> resp = rest.getForEntity(
                "/sessions/" + unknown + "/events?afterSeq=0", Map.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("limit 비정수(abc) → 400 (MethodArgumentTypeMismatch 핸들러)")
    void nonIntegerLimitReturns400() {
        UUID sessionId = seededSession(2);

        ResponseEntity<Map> resp = rest.getForEntity(
                "/sessions/" + sessionId + "/events?afterSeq=0&limit=abc", Map.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("afterSeq=0 조회 → seq 1부터 오름차순으로 반환")
    void afterSeqZeroReturnsFromSeqOne() {
        UUID sessionId = seededSession(3); // seq1..4

        ResponseEntity<Map> resp = rest.getForEntity(
                "/sessions/" + sessionId + "/events?afterSeq=0&limit=100", Map.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        List<Map<String, Object>> evs = events(resp.getBody());
        assertThat(evs).isNotEmpty();
        List<Long> seqs = evs.stream().map(ev -> ((Number) ev.get("seq")).longValue()).toList();
        assertThat(seqs).isSorted();
        assertThat(seqs.get(0)).isEqualTo(1L);
        // 첫 이벤트는 join → PARTICIPANT_JOINED
        assertThat(evs.get(0).get("type")).isEqualTo(EventType.PARTICIPANT_JOINED.name());
        // payload/occurredAt 필드 노출 확인(ResumeEvent.from 매핑)
        assertThat(evs.get(0)).containsKeys("seq", "type", "payload", "occurredAt");
    }
}
