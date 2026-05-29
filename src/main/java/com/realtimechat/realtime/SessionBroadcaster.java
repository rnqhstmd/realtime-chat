package com.realtimechat.realtime;

import com.realtimechat.event.StoredEvent;
import java.util.UUID;

/**
 * 커밋된 이벤트를 세션 구독자에게 팬아웃하는 전송 경계(설계서 §2.1, §6).
 *
 * <p>CommandHandler는 append + projection이 <b>커밋된 뒤</b>(afterCommit) 이 인터페이스로만
 * 실시간 전송을 위임한다. 쓰기 경로(멱등·순서·검증)는 전송 구현과 분리되어, 전송 실패가
 * 진실의 원천(Event Store)을 오염시키지 않는다.
 *
 * <p>Phase 1 구현체는 STOMP {@code /topic/session.{id}}로 직접 전달하며(B5에서 제공),
 * Phase 3에서 Redis Pub/Sub backplane으로 확장된다(설계서 §6, §7).
 */
public interface SessionBroadcaster {

    /**
     * 한 세션의 단일 이벤트를 해당 세션 구독자에게 전달한다.
     *
     * @param sessionId 대상 세션
     * @param event     커밋 완료된 저장 이벤트
     */
    void broadcast(UUID sessionId, StoredEvent event);
}
