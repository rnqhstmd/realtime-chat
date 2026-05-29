package com.realtimechat.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.realtimechat.common.error.InvalidEventException;
import com.realtimechat.common.error.SessionNotFoundException;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.common.web.IdempotencyKeys;
import com.realtimechat.event.AppendCommand;
import com.realtimechat.event.AppendResult;
import com.realtimechat.event.EventStore;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
import com.realtimechat.projection.ProjectionUpdater;
import com.realtimechat.realtime.SessionBroadcaster;
import com.realtimechat.session.SessionDao;
import jakarta.annotation.PostConstruct;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 모든 이벤트 수집의 <b>단일 수렴 진입점</b>(설계서 §6, §11 "REST/WS 단일 command handler 수렴").
 *
 * <p>REST({@code POST /sessions/{id}/events})와 WebSocket(STOMP inbound SEND)은 모두 이 핸들러의
 * {@link #handle} / {@link #collect}로 들어온다(B5에서 연결). 수집 경로가 둘이어도 멱등·순서·검증
 * 로직이 한 곳에 있어 일관성이 보장된다.
 *
 * <p>한 번의 {@link #handle}은 한 트랜잭션에서:
 * <ol>
 *   <li>멱등 키·세션 상태 검증(§4.1, §11),</li>
 *   <li>타입별 payload 검증·보강(§3.1: 검증은 append 전 커맨드 경계에서 완료),</li>
 *   <li>{@link EventStore#append}로 seq 채번 + 수집 멱등(§4.1 계층1),</li>
 *   <li>{@link ProjectionUpdater#apply}로 동기 projection(§13 Phase 1),</li>
 *   <li>커밋 직후 {@link SessionBroadcaster}로 실시간 팬아웃(§6)</li>
 * </ol>
 * 을 수행한다. 전송(broadcast)은 진실의 원천을 오염시키지 않도록 <b>afterCommit</b>에서만 일어난다.
 */
@Service
public class CommandHandler {

    private static final Logger log = LoggerFactory.getLogger(CommandHandler.class);

    private static final String STATUS_ENDED = "ENDED";

    /** PRESENCE_CHANGED에서 허용하는 presence 값(대문자 정규화 후 검증·저장). */
    private static final String PRESENCE_ONLINE = "ONLINE";
    private static final String PRESENCE_OFFLINE = "OFFLINE";

    private final EventStore eventStore;
    private final ProjectionUpdater projectionUpdater;
    private final SessionDao sessionDao;
    private final SessionBroadcaster broadcaster;
    private final boolean syncProjectionEnabled;

    public CommandHandler(EventStore eventStore,
                          ProjectionUpdater projectionUpdater,
                          SessionDao sessionDao,
                          SessionBroadcaster broadcaster,
                          @Value("${chat.projection.sync-enabled:false}") boolean syncProjectionEnabled) {
        this.eventStore = eventStore;
        this.projectionUpdater = projectionUpdater;
        this.sessionDao = sessionDao;
        this.broadcaster = broadcaster;
        this.syncProjectionEnabled = syncProjectionEnabled;
    }

    @PostConstruct
    void warnIfSyncProjectionEnabled() {
        if (syncProjectionEnabled) {
            log.warn("chat.projection.sync-enabled=true: 운영 사용 금지, read model 동기+비동기 이중 적용 위험. 테스트/디버깅 전용.");
        }
    }

    /**
     * 단일 이벤트를 수집한다. 모든 수집 경로(REST/WS)가 수렴하는 핵심 메서드.
     *
     * @param sessionId      대상 세션
     * @param type           이벤트 유형
     * @param payload        타입별 payload(검증·보강 전, nullable)
     * @param idempotencyKey 수집 멱등 키(§4.1 계층1)
     * @param actorId        행위 주체(nullable일 수 있으나 일부 타입의 기본값 보강에 사용)
     * @return 저장(또는 멱등 충돌 시 기존)된 이벤트
     */
    @Transactional
    public StoredEvent handle(UUID sessionId, EventType type, JsonNode payload,
                              String idempotencyKey, UUID actorId) {
        // (a) 멱등 키 검증: 공백 금지 + 최대 길이 제한(설계 가정 보강, 단일 소스 IdempotencyKeys)
        IdempotencyKeys.require(idempotencyKey);

        // (b) 세션 상태 검증: 없으면 404, 종료된 세션은 수집 불가(400)
        String status = sessionDao.status(sessionId)
                .orElseThrow(() -> new SessionNotFoundException(sessionId));
        if (STATUS_ENDED.equals(status)) {
            throw new InvalidEventException("Cannot collect events for ended session: " + sessionId);
        }

        // (c) 타입별 payload 검증·보강 → append에 전달할 최종 payload 확정
        JsonNode finalPayload = validateAndEnrich(type, payload, actorId);

        // (d) seq 채번 + 수집 멱등(§4.1 계층1)
        AppendResult result =
                eventStore.append(new AppendCommand(sessionId, type, finalPayload, idempotencyKey, actorId));
        StoredEvent stored = result.event();

        // 멱등 재유입(isNew=false)이면 projection·broadcast를 생략하고 기존 이벤트만 반환한다.
        // 첫 처리 시 이미 projection·broadcast가 수행되었으므로 중복 적용/전달을 막는다(§4.1 계층2, §6).
        if (result.isNew()) {
            // (e) 동기 projection — 기본 비활성. syncProjectionEnabled=true일 때만 실행(테스트/디버깅 전용).
            // Phase 2 비동기 파이프라인에서 projection은 EventListener가 담당하므로 운영 시 false 유지.
            if (syncProjectionEnabled) {
                projectionUpdater.apply(stored);
            }

            // (f) 커밋 후 실시간 팬아웃(§6). 동기화 비활성 상황(테스트/배치)에서는 즉시 전송.
            publishAfterCommit(sessionId, stored);
        }

        return stored;
    }

    /**
     * 컨트롤러 바인딩 편의 오버로드. 본문({@link CollectEventRequest})과 헤더 멱등 키를
     * {@link #handle}로 위임한다(설계서 §11).
     *
     * <p>{@code @Transactional}을 두지 않는다 — 같은 빈 내부 호출(self-invocation)은 프록시를
     * 우회하므로 이 어노테이션이 무효이고, 실제 트랜잭션 경계는 위임 대상 {@link #handle}에 있다.
     */
    public StoredEvent collect(UUID sessionId, CollectEventRequest req, String idempotencyKey) {
        return handle(sessionId, req.type(), req.payload(), idempotencyKey, req.actorId());
    }

    /**
     * 타입별 payload 검증·보강(설계서 §3.1: 검증은 append 전 커맨드 경계에서 완료).
     *
     * <p>필수 필드가 없으면 {@link InvalidEventException}을 던지고, 기본값으로 보강 가능한
     * 필드(messageId 자동 생성, senderId/participantId의 actorId 폴백)는 채워 넣은 뒤
     * 보강된 최종 {@link JsonNode}를 돌려준다. 핫패스가 payload 내부를 다시 파싱하지 않도록
     * 정제를 여기서 1회 완료한다.
     */
    private JsonNode validateAndEnrich(EventType type, JsonNode payload, UUID actorId) {
        ObjectNode node = asObjectNode(payload);
        return switch (type) {
            case MESSAGE_SENT -> enrichMessageSent(node, actorId);
            case MESSAGE_EDITED -> requireMessageEdited(node);
            case MESSAGE_DELETED -> requireMessageDeleted(node);
            case PARTICIPANT_JOINED -> enrichParticipant(node, actorId, "PARTICIPANT_JOINED");
            case PARTICIPANT_LEFT -> enrichParticipant(node, actorId, "PARTICIPANT_LEFT");
            case PRESENCE_CHANGED -> requirePresenceChanged(node);
        };
    }

    private JsonNode enrichMessageSent(ObjectNode node, UUID actorId) {
        // content 필수
        if (isBlankText(node.get("content"))) {
            throw new InvalidEventException("MESSAGE_SENT requires non-blank 'content'");
        }
        // messageId 없으면 서버 생성
        if (isMissing(node.get("messageId"))) {
            node.put("messageId", UUID.randomUUID().toString());
        }
        // senderId 없으면 actorId 폴백
        if (isMissing(node.get("senderId"))) {
            if (actorId == null) {
                throw new InvalidEventException("MESSAGE_SENT requires 'senderId' (or actorId)");
            }
            node.put("senderId", actorId.toString());
        }
        return node;
    }

    private JsonNode requireMessageEdited(ObjectNode node) {
        if (isMissing(node.get("messageId"))) {
            throw new InvalidEventException("MESSAGE_EDITED requires 'messageId'");
        }
        if (isBlankText(node.get("content"))) {
            throw new InvalidEventException("MESSAGE_EDITED requires non-blank 'content'");
        }
        return node;
    }

    private JsonNode requireMessageDeleted(ObjectNode node) {
        if (isMissing(node.get("messageId"))) {
            throw new InvalidEventException("MESSAGE_DELETED requires 'messageId'");
        }
        return node;
    }

    private JsonNode enrichParticipant(ObjectNode node, UUID actorId, String typeLabel) {
        if (isMissing(node.get("participantId"))) {
            if (actorId == null) {
                throw new InvalidEventException(typeLabel + " requires 'participantId' (or actorId)");
            }
            node.put("participantId", actorId.toString());
        }
        return node;
    }

    private JsonNode requirePresenceChanged(ObjectNode node) {
        if (isMissing(node.get("participantId"))) {
            throw new InvalidEventException("PRESENCE_CHANGED requires 'participantId'");
        }
        if (isBlankText(node.get("presence"))) {
            throw new InvalidEventException("PRESENCE_CHANGED requires non-blank 'presence'");
        }
        // presence는 ONLINE/OFFLINE만 허용(대문자 정규화 후 검증·저장). 그 외 값은 400으로 거부한다.
        String presence = node.get("presence").asText().trim().toUpperCase();
        if (!PRESENCE_ONLINE.equals(presence) && !PRESENCE_OFFLINE.equals(presence)) {
            throw new InvalidEventException(
                    "PRESENCE_CHANGED 'presence' must be " + PRESENCE_ONLINE + " or " + PRESENCE_OFFLINE);
        }
        node.put("presence", presence);
        return node;
    }

    /** payload(JsonNode)를 수정 가능한 ObjectNode로 변환. null/비객체면 빈 ObjectNode에서 시작. */
    private ObjectNode asObjectNode(JsonNode payload) {
        if (payload != null && payload.isObject()) {
            return ((ObjectNode) payload).deepCopy();
        }
        return JsonUtil.mapper().createObjectNode();
    }

    private static boolean isMissing(JsonNode field) {
        return field == null || field.isNull();
    }

    private static boolean isBlankText(JsonNode field) {
        return field == null || field.isNull() || field.asText().isBlank();
    }

    /**
     * 커밋 직후 broadcast 등록(설계서 §6). 트랜잭션 동기화가 활성일 때만 afterCommit 콜백을
     * 등록하고, 비활성(동기화 없는 호출 경로)이면 즉시 전송한다.
     */
    private void publishAfterCommit(UUID sessionId, StoredEvent stored) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    broadcaster.broadcast(sessionId, stored);
                }
            });
        } else {
            broadcaster.broadcast(sessionId, stored);
        }
    }
}
