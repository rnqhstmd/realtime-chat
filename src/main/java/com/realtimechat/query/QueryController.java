package com.realtimechat.query;

import com.realtimechat.common.error.InvalidEventException;
import com.realtimechat.projection.MessageViewDao;
import com.realtimechat.restore.RestoreService;
import com.realtimechat.restore.SessionState;
import com.realtimechat.restore.SnapshotService;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 조회/복원 REST 컨트롤러(설계서 §4.3, §10, §11, PRD R6).
 *
 * <p>두 가지 조회 경로를 노출한다(설계서 §2.1):
 * <ul>
 *   <li>라이브 조회: 읽기 모델({@link MessageViewDao})을 직접 읽어 빠르게 응답(§10 Q2),</li>
 *   <li>시점 복원: snapshot + replay({@link RestoreService})로 무거운 복원 수행(§4.3).</li>
 * </ul>
 * 그리고 스냅샷 생성({@link SnapshotService})을 트리거한다(§11 {@code POST /snapshots}). 비즈니스
 * 로직(복원 알고리즘·fold·멱등 스냅샷)은 모두 하위 계층에 있고, 이 컨트롤러는 위임·매핑만 한다.
 */
@RestController
public class QueryController {

    private static final int DEFAULT_LIVE_MESSAGES = 50;

    /** limit 파라미터 상한(RISK 보강). 과도한 N으로 인한 메모리/응답 비대화를 막는다. */
    private static final int MAX_LIMIT = 500;

    private final RestoreService restoreService;
    private final SnapshotService snapshotService;
    private final MessageViewDao messageViewDao;

    /** 복원 타임라인에 담을 최근 메시지 수 기본값(설계서 §3.3, §4.3: POC 기본 N). */
    private final int timelineRecentMessages;

    public QueryController(
            RestoreService restoreService,
            SnapshotService snapshotService,
            MessageViewDao messageViewDao,
            @Value("${chat.timeline.recent-messages:200}") int timelineRecentMessages) {
        this.restoreService = restoreService;
        this.snapshotService = snapshotService;
        this.messageViewDao = messageViewDao;
        this.timelineRecentMessages = timelineRecentMessages;
    }

    /**
     * 시점 복원(설계서 §4.3, §11 {@code GET /sessions/{id}/timeline?at=}).
     *
     * <p>{@code at}이 숫자면 seq로 해석해 {@link RestoreService#restoreTo}, ISO-8601 timestamp면
     * {@link RestoreService#restoreAt}, 미지정이면 현재 시각으로 복원한다(§11: ts 또는 seq 둘 다 허용).
     * 파싱 불가한 {@code at}은 {@link InvalidEventException}(400)으로 매핑된다.
     *
     * @param id    대상 세션
     * @param at    복원 시점(ISO-8601 timestamp 또는 seq, optional)
     * @param limit 응답에 담을 최근 메시지 수(optional, 기본 {@code chat.timeline.recent-messages})
     */
    @GetMapping("/sessions/{id}/timeline")
    public TimelineResponse timeline(
            @PathVariable UUID id,
            @RequestParam(name = "at", required = false) String at,
            @RequestParam(name = "limit", required = false) Integer limit) {
        SessionState state = restore(id, at);
        int recent = limit != null ? validateLimit(limit) : timelineRecentMessages;
        return TimelineResponse.from(state, recent);
    }

    /**
     * 라이브 최근 메시지 조회(설계서 §10 Q2). 읽기 모델을 직접 조회하며 삭제 메시지는 제외한다.
     *
     * @param id    대상 세션
     * @param limit 최근 메시지 수(optional, 기본 {@value #DEFAULT_LIVE_MESSAGES})
     */
    @GetMapping("/sessions/{id}/messages")
    public List<MessageDto> messages(
            @PathVariable UUID id,
            @RequestParam(name = "limit", required = false, defaultValue = "" + DEFAULT_LIVE_MESSAGES) int limit) {
        return messageViewDao.findRecent(id, validateLimit(limit)).stream()
                .map(MessageDto::from)
                .toList();
    }

    /**
     * 스냅샷 생성/갱신(설계서 §11 {@code POST /sessions/{id}/snapshots}, §5 스냅샷 자동화).
     * 현재 시점까지의 상태를 멱등 저장하며, 처리 수락을 의미하는 202를 반환한다.
     */
    @PostMapping("/sessions/{id}/snapshots")
    public ResponseEntity<Void> snapshot(@PathVariable UUID id) {
        snapshotService.createSnapshot(id);
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }

    /**
     * {@code at} 파라미터를 해석해 적절한 복원 메서드로 위임한다(설계서 §11: ts 또는 seq).
     *
     * <ul>
     *   <li>null/공백 → 현재 시점(maxSeq) 복원(시계 무관, {@link RestoreService#restoreCurrent}),</li>
     *   <li>정수(seq) → {@link RestoreService#restoreTo},</li>
     *   <li>그 외(ISO-8601 timestamp) → {@link RestoreService#restoreAt}.</li>
     * </ul>
     */
    private SessionState restore(UUID id, String at) {
        if (at == null || at.isBlank()) {
            return restoreService.restoreCurrent(id);
        }
        String trimmed = at.trim();
        if (isSeq(trimmed)) {
            return restoreService.restoreTo(id, Long.parseLong(trimmed));
        }
        try {
            return restoreService.restoreAt(id, Instant.parse(trimmed));
        } catch (DateTimeParseException e) {
            throw new InvalidEventException(
                    "Invalid 'at': expected seq (integer) or ISO-8601 timestamp, got: " + at, e);
        }
    }

    /**
     * limit 범위 검증(1~{@value #MAX_LIMIT}). 범위 밖이면 {@link InvalidEventException}(400).
     * 미지정 기본값은 호출부에서 결정하므로 여기서는 명시된 값만 검증한다.
     */
    private static int validateLimit(int limit) {
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new InvalidEventException(
                    "Invalid 'limit': must be between 1 and " + MAX_LIMIT + ", got: " + limit);
        }
        return limit;
    }

    /** {@code at}이 순수 정수(seq)인지 판별한다. 음수·소수·문자 혼합은 timestamp 경로로 보낸다. */
    private static boolean isSeq(String value) {
        if (value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
