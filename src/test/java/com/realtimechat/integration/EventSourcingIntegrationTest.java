package com.realtimechat.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.command.CommandHandler;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventStore;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
import com.realtimechat.common.error.InvalidEventException;
import com.realtimechat.event.payload.MessageDeletedPayload;
import com.realtimechat.event.payload.MessageEditedPayload;
import com.realtimechat.event.payload.MessageSentPayload;
import com.realtimechat.event.payload.ParticipantLeftPayload;
import com.realtimechat.event.payload.PresenceChangedPayload;
import com.realtimechat.projection.MessageViewDao;
import com.realtimechat.restore.RestoreService;
import com.realtimechat.restore.SessionState;
import com.realtimechat.restore.SnapshotDao;
import com.realtimechat.restore.SnapshotService;
import com.realtimechat.session.SessionService;
import com.realtimechat.support.AbstractIntegrationTest;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 이벤트 소싱 end-to-end 통합 테스트(Testcontainers PostgreSQL, 설계서 §12).
 *
 * <p>서비스/핸들러/저장소/복원 계층을 실제 빈으로 주입받아 PRD 수용 기준(AC1~AC6)을 실 DB로
 * 검증한다. append→동기 projection→복원의 전 경로가 실제 PostgreSQL(JSONB·UNIQUE·RETURNING)에서
 * 동작함을 확인하는 것이 이 테스트의 목적이다.
 */
class EventSourcingIntegrationTest extends AbstractIntegrationTest {

    @Autowired private SessionService sessionService;
    @Autowired private CommandHandler commandHandler;
    @Autowired private EventStore eventStore;
    @Autowired private RestoreService restoreService;
    @Autowired private SnapshotService snapshotService;
    @Autowired private SnapshotDao snapshotDao;
    @Autowired private MessageViewDao messageViewDao;
    @Autowired private JdbcTemplate jdbcTemplate;

    // ---- payload → JsonNode 헬퍼 ----

    private static JsonNode node(Object record) {
        return JsonUtil.toJsonNode(record);
    }

    // =====================================================================
    // AC1: 세션 생성→참여→메시지 송신→조회→종료 E2E
    // =====================================================================
    @Test
    @DisplayName("AC1: createSession→join→MESSAGE_SENT→findRecent→end E2E")
    void ac1_endToEndLifecycle() {
        UUID sessionId = sessionService.createSession();
        UUID alice = UUID.randomUUID();

        StoredEvent joined = sessionService.join(sessionId, alice, "join-" + alice);
        assertThat(joined.type()).isEqualTo(EventType.PARTICIPANT_JOINED);
        assertThat(joined.seq()).isEqualTo(1L);

        UUID messageId = UUID.randomUUID();
        StoredEvent sent = commandHandler.handle(
                sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(messageId, alice, "hello world")),
                "msg-" + messageId, alice);
        assertThat(sent.seq()).isEqualTo(2L);

        // 동기 projection으로 message_view가 즉시 갱신됨
        List<MessageViewDao.MessageRow> recent = messageViewDao.findRecent(sessionId, 10);
        assertThat(recent).hasSize(1);
        assertThat(recent.get(0).content()).isEqualTo("hello world");
        assertThat(recent.get(0).state()).isEqualTo("SENT");

        // session_view 반영 확인
        assertThat(sessionService.get(sessionId)).isPresent();
        assertThat(sessionService.get(sessionId).get().participantCount()).isEqualTo(1);
        assertThat(sessionService.get(sessionId).get().messageCount()).isEqualTo(1);

        // 종료
        sessionService.end(sessionId);
        assertThat(sessionService.get(sessionId).get().status()).isEqualTo("ENDED");
    }

    // =====================================================================
    // AC2: 동일 Idempotency-Key 2회 전송 → 이벤트 1건만, 동일 seq/결과 (계층1)
    // =====================================================================
    @Test
    @DisplayName("AC2: 같은 Idempotency-Key 재전송 시 이벤트 1건, 동일 seq 반환")
    void ac2_idempotentCollect() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();
        String key = "dup-key-" + messageId;

        StoredEvent first = commandHandler.handle(
                sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(messageId, sender, "once")), key, sender);
        StoredEvent second = commandHandler.handle(
                sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(messageId, sender, "once")), key, sender);

        // 동일 seq, 동일 eventId — 두 번째는 기존 이벤트를 그대로 반환
        assertThat(second.seq()).isEqualTo(first.seq());
        assertThat(second.eventId()).isEqualTo(first.eventId());

        // event 테이블에 1건만 존재
        List<StoredEvent> all = eventStore.findBySeqRange(sessionId, 0, Long.MAX_VALUE);
        assertThat(all).hasSize(1);

        // message_view도 1건, message_count 중복 증가 없음
        assertThat(messageViewDao.findRecent(sessionId, 10)).hasSize(1);
        assertThat(sessionService.get(sessionId).get().messageCount()).isEqualTo(1);
    }

    // =====================================================================
    // AC3: seq 단조증가 + (session_id, idempotency_key) 유니크 동작
    // =====================================================================
    @Test
    @DisplayName("AC3: 서버 채번 seq 단조증가, 서로 다른 키는 새 seq 발급")
    void ac3_monotonicSeqAndUniqueConstraints() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();

        StoredEvent e1 = commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(UUID.randomUUID(), sender, "a")), "k1", sender);
        StoredEvent e2 = commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(UUID.randomUUID(), sender, "b")), "k2", sender);
        StoredEvent e3 = commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(UUID.randomUUID(), sender, "c")), "k3", sender);

        assertThat(List.of(e1.seq(), e2.seq(), e3.seq())).containsExactly(1L, 2L, 3L);

        // idempotency_key 재사용(k1) → 기존(e1) 반환, 새 seq 미발급
        StoredEvent dup = commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(UUID.randomUUID(), sender, "dup")), "k1", sender);
        assertThat(dup.seq()).isEqualTo(e1.seq());

        // 총 3건만 저장됨 (seq 소비 없음)
        assertThat(eventStore.findBySeqRange(sessionId, 0, Long.MAX_VALUE)).hasSize(3);
    }

    // =====================================================================
    // AC4: MESSAGE_SENT→EDITED→DELETED 상태 전이 + findRecent seq DESC·DELETED 제외
    // =====================================================================
    @Test
    @DisplayName("AC4: message_view 상태 전이(SENT→EDITED→DELETED), findRecent seq DESC·DELETED 제외")
    void ac4_messageViewStateTransitions() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();
        UUID m1 = UUID.randomUUID();
        UUID m2 = UUID.randomUUID();
        UUID m3 = UUID.randomUUID();

        // m1: SENT→EDITED
        commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(m1, sender, "first")), "s1", sender);
        commandHandler.handle(sessionId, EventType.MESSAGE_EDITED,
                node(new MessageEditedPayload(m1, "first-edited")), "e1", sender);
        // m2: SENT→DELETED
        commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(m2, sender, "second")), "s2", sender);
        commandHandler.handle(sessionId, EventType.MESSAGE_DELETED,
                node(new MessageDeletedPayload(m2)), "d2", sender);
        // m3: SENT
        commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(m3, sender, "third")), "s3", sender);

        List<MessageViewDao.MessageRow> recent = messageViewDao.findRecent(sessionId, 10);
        // DELETED(m2) 제외 → m1, m3만 / seq DESC → m3(seq5) 먼저, m1(seq1) 나중
        assertThat(recent).hasSize(2);
        assertThat(recent.get(0).messageId()).isEqualTo(m3);
        assertThat(recent.get(1).messageId()).isEqualTo(m1);
        // m1은 EDITED 상태로 content 갱신
        assertThat(recent.get(1).state()).isEqualTo("EDITED");
        assertThat(recent.get(1).content()).isEqualTo("first-edited");
    }

    // =====================================================================
    // AC5: 중간 시점 복원 (restoreTo / restoreAt) + snapshot+replay 경로 일치
    // =====================================================================
    @Test
    @DisplayName("AC5a: restoreTo(중간 seq)가 그 시점의 참여자/메시지/상태를 정확히 복원")
    void ac5a_restoreToMidpoint() {
        UUID sessionId = sessionService.createSession();
        UUID alice = UUID.randomUUID();
        UUID m1 = UUID.randomUUID();

        sessionService.join(sessionId, alice, "j-" + alice);                     // seq 1
        commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(m1, alice, "v1")), "s1", alice);      // seq 2
        commandHandler.handle(sessionId, EventType.MESSAGE_EDITED,
                node(new MessageEditedPayload(m1, "v2")), "e1", alice);           // seq 3
        commandHandler.handle(sessionId, EventType.MESSAGE_DELETED,
                node(new MessageDeletedPayload(m1)), "d1", alice);               // seq 4

        // seq=2 시점: 메시지는 SENT/"v1", 아직 편집·삭제 전
        SessionState atSeq2 = restoreService.restoreTo(sessionId, 2);
        assertThat(atSeq2.message(m1).state()).isEqualTo("SENT");
        assertThat(atSeq2.message(m1).content()).isEqualTo("v1");
        assertThat(atSeq2.recentMessages(10)).hasSize(1);

        // seq=3 시점: EDITED/"v2"
        SessionState atSeq3 = restoreService.restoreTo(sessionId, 3);
        assertThat(atSeq3.message(m1).state()).isEqualTo("EDITED");
        assertThat(atSeq3.message(m1).content()).isEqualTo("v2");

        // seq=4 시점: DELETED → recentMessages에서 제외
        SessionState atSeq4 = restoreService.restoreTo(sessionId, 4);
        assertThat(atSeq4.message(m1).state()).isEqualTo("DELETED");
        assertThat(atSeq4.recentMessages(10)).isEmpty();
        assertThat(atSeq4.participant(alice).status()).isEqualTo("JOINED");
    }

    @Test
    @DisplayName("AC5b: restoreAt(중간 timestamp)가 그 시점 상태를 복원")
    void ac5b_restoreAtTimestamp() {
        UUID sessionId = sessionService.createSession();
        UUID alice = UUID.randomUUID();
        UUID m1 = UUID.randomUUID();

        sessionService.join(sessionId, alice, "j-" + alice);
        StoredEvent sent = commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(m1, alice, "v1")), "s1", alice);
        // sent.occurredAt 시점 이후에 편집 — 단조 시각 보장을 위해 미세 대기 없이 seq 권위로 복원
        commandHandler.handle(sessionId, EventType.MESSAGE_EDITED,
                node(new MessageEditedPayload(m1, "v2")), "e1", alice);

        // sent 발생 시점으로 복원 → 그 시점엔 메시지가 SENT/"v1"
        SessionState atSent = restoreService.restoreAt(sessionId, sent.occurredAt());
        assertThat(atSent.message(m1)).isNotNull();
        assertThat(atSent.message(m1).content()).isEqualTo("v1");
        assertThat(atSent.message(m1).state()).isEqualTo("SENT");
    }

    @Test
    @DisplayName("AC5c: snapshot 생성 후 이후 이벤트 추가 → snapshot+replay 경로가 전체 replay와 일치")
    void ac5c_snapshotPlusReplayMatchesFullReplay() {
        UUID sessionId = sessionService.createSession();
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        UUID m1 = UUID.randomUUID();
        UUID m2 = UUID.randomUUID();

        // 스냅샷 이전 이벤트들
        sessionService.join(sessionId, alice, "j-" + alice);                     // seq 1
        commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(m1, alice, "snap-before")), "s1", alice); // seq 2

        // 스냅샷 생성 (up_to_seq = 2)
        snapshotService.createSnapshot(sessionId);

        // 스냅샷 이후 이벤트들
        sessionService.join(sessionId, bob, "j-" + bob);                         // seq 3
        commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(m2, bob, "after-snap")), "s2", bob);  // seq 4
        commandHandler.handle(sessionId, EventType.MESSAGE_EDITED,
                node(new MessageEditedPayload(m1, "snap-edited")), "e1", alice);  // seq 5

        // 복원: snapshot(seq2) + replay(seq3..5). 결과 검증
        SessionState restored = restoreService.restoreTo(sessionId, 5);
        assertThat(restored.participantList()).hasSize(2);
        assertThat(restored.participant(alice).status()).isEqualTo("JOINED");
        assertThat(restored.participant(bob).status()).isEqualTo("JOINED");
        assertThat(restored.message(m1).content()).isEqualTo("snap-edited"); // 스냅샷 메시지가 replay로 편집됨
        assertThat(restored.message(m1).state()).isEqualTo("EDITED");
        assertThat(restored.message(m2).content()).isEqualTo("after-snap");

        // 동일성: 같은 atSeq 복원은 멱등(반복 호출 결과 동일)
        SessionState restoredAgain = restoreService.restoreTo(sessionId, 5);
        assertThat(restoredAgain.recentMessages(10))
                .hasSize(restored.recentMessages(10).size());
        assertThat(restoredAgain.participantList())
                .hasSameSizeAs(restored.participantList());
    }

    // =====================================================================
    // AC6: 멱등(중복)·복원 일관성 — 중복 주입 후에도 복원 결과가 일관됨
    // =====================================================================
    @Test
    @DisplayName("AC6: 중복 이벤트 주입 후에도 복원 결과 일관(정제는 append에서 1회 완료)")
    void ac6_duplicateInjectionConsistentRestore() {
        UUID sessionId = sessionService.createSession();
        UUID alice = UUID.randomUUID();
        UUID m1 = UUID.randomUUID();

        sessionService.join(sessionId, alice, "j-" + alice);
        commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(m1, alice, "v1")), "s1", alice);
        // 동일 키로 중복 주입 (계층1 멱등 → 무시)
        commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(m1, alice, "v1")), "s1", alice);
        commandHandler.handle(sessionId, EventType.MESSAGE_EDITED,
                node(new MessageEditedPayload(m1, "v2")), "e1", alice);
        // 편집도 중복 주입
        commandHandler.handle(sessionId, EventType.MESSAGE_EDITED,
                node(new MessageEditedPayload(m1, "v2")), "e1", alice);

        long maxSeq = eventStore.maxSeqAtOrBefore(sessionId, Instant.now()).orElse(0L);
        // 중복은 append에서 차단되어 seq는 3까지만(join1, sent2, edited3)
        assertThat(maxSeq).isEqualTo(3L);

        SessionState restored = restoreService.restoreTo(sessionId, maxSeq);
        assertThat(restored.message(m1).content()).isEqualTo("v2");
        assertThat(restored.message(m1).state()).isEqualTo("EDITED");
        assertThat(restored.recentMessages(10)).hasSize(1);
    }

    // =====================================================================
    // 회귀: participant_count는 실제 상태 전이일 때만 증감(과차감/비대칭 방지)
    // =====================================================================
    @Test
    @DisplayName("회귀: participant_count 전이 — join/leave/중복 leave/rejoin에서 과차감·비대칭 없음")
    void regression_participantCountTransitions() {
        UUID sessionId = sessionService.createSession();
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();

        // join(A) → 1
        sessionService.join(sessionId, alice, "j-a");
        assertThat(participantCount(sessionId)).isEqualTo(1);

        // join(B) → 2
        sessionService.join(sessionId, bob, "j-b");
        assertThat(participantCount(sessionId)).isEqualTo(2);

        // leave(A) → 1
        leave(sessionId, alice, "l-a-1");
        assertThat(participantCount(sessionId)).isEqualTo(1);

        // 동일 참여자(A)에 대해 다른 멱등키로 LEFT 한번 더 → 이미 LEFT라 과차감 없음, 여전히 1
        leave(sessionId, alice, "l-a-2");
        assertThat(participantCount(sessionId)).isEqualTo(1);

        // rejoin(A) → 2 (LEFT→JOINED 전이로 다시 증가, 비대칭 없음)
        sessionService.join(sessionId, alice, "j-a-2");
        assertThat(participantCount(sessionId)).isEqualTo(2);

        // 중복 JOINED(이미 JOINED, 다른 멱등키) → 증가 안 함, 여전히 2
        sessionService.join(sessionId, alice, "j-a-3");
        assertThat(participantCount(sessionId)).isEqualTo(2);
    }

    // =====================================================================
    // 회귀: 스냅샷 up_to_seq는 시각 무관 maxSeq까지 포함(occurred_at 역전 영향 없음)
    // =====================================================================
    @Test
    @DisplayName("회귀: createSnapshot이 시각 무관 maxSeq까지 up_to_seq로 저장")
    void regression_snapshotCoversMaxSeqRegardlessOfClock() {
        UUID sessionId = sessionService.createSession();
        UUID alice = UUID.randomUUID();

        sessionService.join(sessionId, alice, "j-a");                               // seq 1
        commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(UUID.randomUUID(), alice, "m1")), "s1", alice); // seq 2
        commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(UUID.randomUUID(), alice, "m2")), "s2", alice); // seq 3

        long maxSeq = eventStore.maxSeq(sessionId);
        assertThat(maxSeq).isEqualTo(3L);

        snapshotService.createSnapshot(sessionId);

        // 생성된 스냅샷은 현재 maxSeq(3)까지 포함해야 한다(시각 무관).
        assertThat(snapshotDao.findLatestAtOrBefore(sessionId, maxSeq))
                .isPresent()
                .get()
                .extracting(SnapshotDao.Snapshot::upToSeq)
                .isEqualTo(maxSeq);
    }

    // =====================================================================
    // 회귀: 같은 Idempotency-Key 재전송 시 projection 중복 적용 없음(isNew=false 경로)
    // =====================================================================
    @Test
    @DisplayName("회귀: 같은 Idempotency-Key 재전송 → message_count 중복 증가 없음(isNew=false)")
    void regression_duplicateKeyNoDoubleProjection() {
        UUID sessionId = sessionService.createSession();
        UUID sender = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();
        String key = "no-double-" + messageId;

        commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(messageId, sender, "once")), key, sender);
        // 동일 키 재전송 — append는 기존 이벤트 반환(isNew=false), projection 재적용 금지
        commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(messageId, sender, "once")), key, sender);

        // 이벤트 1건, message_view 1건, message_count 1 (projection 단일 적용)
        assertThat(eventStore.findBySeqRange(sessionId, 0, Long.MAX_VALUE)).hasSize(1);
        assertThat(messageViewDao.findRecent(sessionId, 10)).hasSize(1);
        assertThat(sessionService.get(sessionId).orElseThrow().messageCount()).isEqualTo(1);
    }

    // =====================================================================
    // 회귀: PRESENCE_CHANGED presence 값 검증(ONLINE/OFFLINE만 허용, 그 외 400)
    // =====================================================================
    @Test
    @DisplayName("회귀: PRESENCE_CHANGED 유효 값(ONLINE) 수집, 잘못된 값은 InvalidEventException(400)")
    void regression_presenceChangedValueValidation() {
        UUID sessionId = sessionService.createSession();
        UUID participant = UUID.randomUUID();
        sessionService.join(sessionId, participant, "pj-" + participant);

        // 유효 값: 정상 수집
        StoredEvent online = commandHandler.handle(sessionId, EventType.PRESENCE_CHANGED,
                node(new PresenceChangedPayload(participant, "ONLINE")), "pres-on", participant);
        assertThat(online.type()).isEqualTo(EventType.PRESENCE_CHANGED);

        // 잘못된 값: 400으로 거부
        assertThatThrownBy(() -> commandHandler.handle(sessionId, EventType.PRESENCE_CHANGED,
                node(new PresenceChangedPayload(participant, "BOGUS")), "pres-bad", participant))
                .isInstanceOf(InvalidEventException.class);
    }

    // =====================================================================
    // 회귀: 재가입 시 participant_view.joined_seq가 새 seq로 갱신되고 복원 모델과 일치
    // =====================================================================
    @Test
    @DisplayName("회귀: join→leave→rejoin 후 participant_view.joined_seq=재참여 seq, 복원 joinedSeq와 일치")
    void regression_rejoinUpdatesJoinedSeq() {
        UUID sessionId = sessionService.createSession();
        UUID alice = UUID.randomUUID();

        StoredEvent firstJoin = sessionService.join(sessionId, alice, "j-a-1");    // seq 1
        leave(sessionId, alice, "l-a-1");                                          // seq 2
        StoredEvent rejoin = sessionService.join(sessionId, alice, "j-a-2");       // seq 3

        // 동기 projection: participant_view.joined_seq가 재참여 seq(3)로 갱신됨(이전 join seq 1 아님)
        Long projectionJoinedSeq = jdbcTemplate.queryForObject(
                "SELECT joined_seq FROM participant_view WHERE session_id = ? AND participant_id = ?",
                Long.class, sessionId, alice);
        assertThat(projectionJoinedSeq).isEqualTo(rejoin.seq());
        assertThat(projectionJoinedSeq).isNotEqualTo(firstJoin.seq());

        // 복원 모델(event replay)의 joinedSeq도 재참여 seq → 동기 projection과 일치
        SessionState restored = restoreService.restoreTo(sessionId, rejoin.seq());
        assertThat(restored.participant(alice).status()).isEqualTo("JOINED");
        assertThat(restored.participant(alice).joinedSeq()).isEqualTo(rejoin.seq());
        assertThat(restored.participant(alice).joinedSeq()).isEqualTo(projectionJoinedSeq);
    }

    // =====================================================================
    // 회귀: ENDED 세션에는 이벤트 수집 불가(InvalidEventException → 400)
    // =====================================================================
    @Test
    @DisplayName("회귀: 세션 end 후 같은 세션에 이벤트 수집 시 거부(InvalidEventException)")
    void regression_endedSessionRejectsEvents() {
        UUID sessionId = sessionService.createSession();
        UUID alice = UUID.randomUUID();
        sessionService.join(sessionId, alice, "j-a");

        sessionService.end(sessionId);
        assertThat(sessionService.get(sessionId).orElseThrow().status()).isEqualTo("ENDED");

        // ENDED 세션에 신규 이벤트 수집 → CommandHandler status() 가드 + EventStore 채번 가드 모두 차단
        assertThatThrownBy(() -> commandHandler.handle(sessionId, EventType.MESSAGE_SENT,
                node(new MessageSentPayload(UUID.randomUUID(), alice, "after-end")), "after-end-1", alice))
                .isInstanceOf(InvalidEventException.class);

        // 거부되어 이벤트가 추가되지 않음(join seq 1만 존재)
        assertThat(eventStore.maxSeq(sessionId)).isEqualTo(1L);
    }

    // ---- 회귀 테스트 헬퍼 ----

    private int participantCount(UUID sessionId) {
        return sessionService.get(sessionId).orElseThrow().participantCount();
    }

    private void leave(UUID sessionId, UUID participantId, String idempotencyKey) {
        commandHandler.handle(
                sessionId, EventType.PARTICIPANT_LEFT,
                node(new ParticipantLeftPayload(participantId)), idempotencyKey, participantId);
    }
}
