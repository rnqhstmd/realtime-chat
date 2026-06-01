package com.realtimechat.presence;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.realtimechat.command.CommandHandler;
import com.realtimechat.common.error.InvalidEventException;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventType;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * presence TTL 만료 감지 + OFFLINE 자동 수집(설계서 §5-B, FR-P3-2, BR-2, BR-3, AC-4/5).
 *
 * <p>{@code expiry-poll-ms}(기본 5s) 주기로 sweep 대상 Set의 모든 멤버를 훑어 liveness 키가
 * 만료된 멤버를 찾는다. 만료 멤버는 {@link PresenceTracker#claimExpired}(SREM 원자)로 1회만
 * "선점"하고, 선점에 성공한 sweep만 PRESENCE_CHANGED(OFFLINE)를 발행한다. OFFLINE 중복 방지의
 * 권위는 전적으로 SREM 반환값이며 read model lag와 무관하다(BR-3, critic #1).
 *
 * <p>OFFLINE 발행은 일반 이벤트와 동일하게 기존 {@link CommandHandler#handle}로 수집되어
 * projection·팬아웃을 거친다(CommandHandler 수정 0, 설계서 §5-B). heartbeat ping은 이벤트가
 * 아니므로 이 경로와 무관하다.
 *
 * <p>{@code @Transactional} 없음: OFFLINE 발행의 TX 경계는 {@link CommandHandler#handle} 내부에
 * 있다. 백그라운드 스케줄이므로 멤버 처리 중 예외는 WARN으로 흡수하고 다음 멤버/다음 주기로
 * 진행한다(sweep 중단 방지).
 */
@Component
public class PresenceSweeper {

    private static final Logger log = LoggerFactory.getLogger(PresenceSweeper.class);

    private static final String PRESENCE_OFFLINE = "OFFLINE";

    private final PresenceTracker tracker;
    private final CommandHandler commandHandler;

    public PresenceSweeper(PresenceTracker tracker, CommandHandler commandHandler) {
        this.tracker = tracker;
        this.commandHandler = commandHandler;
    }

    /**
     * 만료 멤버를 훑어 OFFLINE을 1회 발행한다(설계서 §5-B). liveness 키가 만료된
     * 멤버만 {@link PresenceTracker#claimExpired}로 선점하고, 선점에 성공한 호출만 발행한다.
     */
    @Scheduled(fixedDelayString = "${chat.presence.expiry-poll-ms:5000}")
    public void sweep() {
        for (String member : tracker.trackedMembers()) {
            try {
                UUID sid = PresenceTracker.sessionIdOf(member);
                UUID pid = PresenceTracker.participantIdOf(member);
                if (!tracker.isAlive(sid, pid)) {            // liveness 키 만료
                    if (tracker.claimExpired(member)) {      // SREM==1: 자기만 발행(BR-3)
                        publishOffline(sid, pid);
                    }
                }
            } catch (RuntimeException e) {
                log.warn("presence sweep 처리 실패: member={}", member, e);  // sweep 중단 방지
            }
        }
    }

    /**
     * PRESENCE_CHANGED(OFFLINE)를 기존 수집 경로로 발행한다(설계서 §5-B). 멱등 키는 SREM이 1회
     * 보장하므로 단조요소(epoch-milli)로 충분하다. ENDED 세션 등으로 수집이 거부되면
     * ({@link InvalidEventException}) skip하고 WARN만 남긴다.
     */
    private void publishOffline(UUID sid, UUID pid) {
        try {
            ObjectNode payload = JsonUtil.mapper().createObjectNode()
                    .put("participantId", pid.toString())
                    .put("presence", PRESENCE_OFFLINE);
            String idem = "presence-offline-" + sid + "-" + pid + "-" + Instant.now().toEpochMilli();
            commandHandler.handle(sid, EventType.PRESENCE_CHANGED, payload, idem, pid);  // 정상 수집 경로
        } catch (InvalidEventException e) {                  // ENDED 세션 등
            log.warn("OFFLINE 발행 skip(세션 비활성): sid={}, pid={}", sid, pid, e);
        } catch (RuntimeException e) {                       // sweep 중단 방지
            log.warn("OFFLINE 발행 실패: sid={}, pid={}", sid, pid, e);
        }
    }
}
