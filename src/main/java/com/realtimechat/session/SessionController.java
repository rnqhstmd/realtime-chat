package com.realtimechat.session;

import com.realtimechat.common.web.IdempotencyKeys;
import com.realtimechat.event.EventResponse;
import com.realtimechat.event.StoredEvent;
import jakarta.validation.Valid;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 세션 라이프사이클 REST 컨트롤러(설계서 §11, PRD R1).
 *
 * <p>생성/참여/종료/목록을 {@link SessionService}에 위임만 한다. 참여(join)는 PARTICIPANT_JOINED
 * 이벤트로서 동일 command handler 단일 수렴점을 거치므로(§6, §11), 그 결과를
 * {@link EventResponse}로 재사용해 응답한다. 멱등·검증·순서 로직은 모두 하위 계층에 있다.
 */
@RestController
public class SessionController {

    private final SessionService sessionService;

    public SessionController(SessionService sessionService) {
        this.sessionService = sessionService;
    }

    /** 세션 생성(설계서 §11 {@code POST /sessions}). 생성된 식별자를 201로 반환한다. */
    @PostMapping("/sessions")
    public ResponseEntity<CreateSessionResponse> create() {
        UUID sessionId = sessionService.createSession();
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new CreateSessionResponse(sessionId));
    }

    /**
     * 참여(설계서 §11 {@code POST /sessions/{id}/join}). {@code Idempotency-Key} 헤더 필수.
     * PARTICIPANT_JOINED 이벤트 수집 결과를 {@link EventResponse}로 반환한다.
     */
    @PostMapping("/sessions/{id}/join")
    public EventResponse join(
            @PathVariable UUID id,
            @Valid @RequestBody JoinRequest request,
            @RequestHeader(name = IdempotencyKeys.HEADER) String idempotencyKey) {
        StoredEvent stored = sessionService.join(id, request.participantId(), idempotencyKey);
        return EventResponse.from(stored);
    }

    /** 세션 종료(설계서 §11 {@code POST /sessions/{id}/end}). 미존재면 404. */
    @PostMapping("/sessions/{id}/end")
    public ResponseEntity<Void> end(@PathVariable UUID id) {
        sessionService.end(id);
        return ResponseEntity.ok().build();
    }

    /**
     * 세션 목록(설계서 §11 {@code GET /sessions}, ix_session_list). status 미지정 시 전체.
     *
     * @param status ACTIVE / ENDED 필터(optional)
     */
    @GetMapping("/sessions")
    public List<SessionSummary> list(@RequestParam(name = "status", required = false) String status) {
        return sessionService.list(status).stream()
                .map(SessionSummary::from)
                .toList();
    }

    /**
     * 세션 생성 응답 본문(설계서 §11). 생성된 세션 식별자만 반환한다.
     *
     * @param sessionId 생성된 세션 식별자
     */
    public record CreateSessionResponse(UUID sessionId) {
    }
}
