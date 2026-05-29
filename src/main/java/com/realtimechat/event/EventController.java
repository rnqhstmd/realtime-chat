package com.realtimechat.event;

import com.realtimechat.command.CollectEventRequest;
import com.realtimechat.command.CommandHandler;
import com.realtimechat.common.web.IdempotencyKeys;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * 이벤트/메시지 수집 REST 컨트롤러(설계서 §11 {@code POST /sessions/{id}/events}, PRD R2).
 *
 * <p>REST와 WebSocket이 동일 {@link CommandHandler}로 수렴하는 단일 진입 원칙(§6, §11)에 따라,
 * 이 컨트롤러는 본문·헤더를 바인딩해 위임만 한다. 멱등 키 검증·세션 상태 검증·payload 검증·
 * seq 채번·동기 projection·broadcast는 모두 하위 계층({@link CommandHandler})에 있다.
 */
@RestController
public class EventController {

    private final CommandHandler commandHandler;

    public EventController(CommandHandler commandHandler) {
        this.commandHandler = commandHandler;
    }

    /**
     * 이벤트/메시지 수집(설계서 §11). {@code Idempotency-Key} 헤더 필수(§4.1 계층1).
     *
     * @param id             대상 세션
     * @param request        타입·payload·actorId(payload 검증·보강은 CommandHandler 경계에서 수행)
     * @param idempotencyKey 멱등 키 헤더
     * @return 저장(또는 멱등 충돌 시 기존)된 이벤트의 외부 표현
     */
    @PostMapping("/sessions/{id}/events")
    public EventResponse collect(
            @PathVariable UUID id,
            @Valid @RequestBody CollectEventRequest request,
            @RequestHeader(name = IdempotencyKeys.HEADER) String idempotencyKey) {
        StoredEvent stored = commandHandler.collect(id, request, idempotencyKey);
        return EventResponse.from(stored);
    }
}
