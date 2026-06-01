package com.realtimechat.query;

import com.realtimechat.common.error.InvalidEventException;
import com.realtimechat.common.error.SessionNotFoundException;
import com.realtimechat.common.web.LimitSupport;
import com.realtimechat.event.EventStore;
import com.realtimechat.event.StoredEvent;
import com.realtimechat.query.ResumeResponse.ResumeEvent;
import com.realtimechat.session.SessionDao;
import java.util.List;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * resume-by-seq 조회 REST 컨트롤러(설계서 §5-C, §11 {@code GET /sessions/{id}/events?afterSeq=}, PRD R6).
 *
 * <p>{@code afterSeq} 이후의 raw 이벤트 delta를 반환한다(결정 E). fold된 상태를 주는
 * {@code GET /sessions/{id}/timeline}({@link QueryController})과 달리, 실제 이벤트 시퀀스를
 * 그대로 노출하여 재연결 클라이언트가 누락 구간만 따라잡게 한다. 비즈니스 로직은
 * {@link EventStore}에 있고, 이 컨트롤러는 검증·세션 존재 확인·매핑만 한다.
 */
@RestController
public class ResumeEventController {

    private final EventStore eventStore;
    private final SessionDao sessionDao;

    public ResumeEventController(EventStore eventStore, SessionDao sessionDao) {
        this.eventStore = eventStore;
        this.sessionDao = sessionDao;
    }

    /**
     * resume delta 조회(설계서 §5-C, §11 {@code GET /sessions/{id}/events}).
     *
     * <p>검증 순서(§5-C):
     * <ol>
     *   <li>{@code afterSeq}는 필수 — 누락 시 {@link InvalidEventException}(400, AC-9).
     *       {@code required=false}로 받아 직접 검증하여 {@code MissingServletRequestParameterException}을 회피한다.</li>
     *   <li>{@code limit}은 {@link LimitSupport#validate(int)}로 1~{@value LimitSupport#MAX_LIMIT} 검증(400, AC-10).</li>
     *   <li>세션이 없으면 {@link SessionNotFoundException}(404, AC-11).</li>
     * </ol>
     *
     * <p>조회/hasMore: {@code findBySeqRange}는 반개구간 {@code (from, to]}이므로
     * {@code findBySeqRange(id, afterSeq, afterSeq + limit + 1)}로 afterSeq 초과 이벤트를 최대
     * {@code limit+1}건 가져온다. {@code limit+1}건이면 다음 페이지가 있으므로 {@code hasMore=true}로
     * 두고 앞 {@code limit}개만 반환한다. {@code afterSeq >= maxSeq}면 빈 결과 + {@code hasMore=false}로
     * 자연 충족된다(AC-6/7/8).
     *
     * @param id       대상 세션
     * @param afterSeq 이 seq를 초과하는 이벤트만 조회(필수)
     * @param limit    최대 이벤트 수(optional, 기본 100, 1~{@value LimitSupport#MAX_LIMIT})
     */
    @GetMapping("/sessions/{id}/events")
    public ResumeResponse events(
            @PathVariable UUID id,
            @RequestParam(name = "afterSeq", required = false) Long afterSeq,
            @RequestParam(name = "limit", required = false, defaultValue = "100") int limit) {
        if (afterSeq == null) {
            throw new InvalidEventException("Missing required parameter 'afterSeq'");
        }
        if (afterSeq < 0) {
            throw new InvalidEventException("afterSeq must be >= 0");
        }
        int validated = LimitSupport.validate(limit);
        if (sessionDao.status(id).isEmpty()) {
            throw new SessionNotFoundException(id);
        }

        // 반개구간 (from, to] — afterSeq 초과 이벤트를 hasMore 판정용으로 validated+1건까지 조회
        List<StoredEvent> rows = eventStore.findBySeqRange(id, afterSeq, afterSeq + validated + 1);

        boolean hasMore = rows.size() > validated;
        List<StoredEvent> page = hasMore ? rows.subList(0, validated) : rows;
        List<ResumeEvent> events = page.stream()
                .map(ResumeEvent::from)
                .toList();
        return new ResumeResponse(events, hasMore);
    }
}
