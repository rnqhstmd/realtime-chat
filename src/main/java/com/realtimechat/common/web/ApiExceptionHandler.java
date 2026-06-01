package com.realtimechat.common.web;

import com.realtimechat.common.error.InvalidEventException;
import com.realtimechat.common.error.SessionNotFoundException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/**
 * 전역 예외 → HTTP 응답 매핑.
 *
 * <p>본문 형식은 {@code {timestamp, status, error, message}}로 통일한다.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException e) {
        String message = e.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .orElse("Validation failed");
        return build(HttpStatus.BAD_REQUEST, message);
    }

    @ExceptionHandler(InvalidEventException.class)
    public ResponseEntity<Map<String, Object>> handleInvalidEvent(InvalidEventException e) {
        return build(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    /**
     * 필수 요청 헤더 누락(예: {@code Idempotency-Key}) → 400. 클라이언트 요청 결함이므로 400으로
     * 매핑한다(PRD R2 "Idempotency-Key 헤더 필수", 설계서 §11). 미처리 시 catch-all로 500이 되어
     * 클라이언트 오류가 서버 오류로 잘못 표기되는 것을 방지한다.
     */
    @ExceptionHandler(MissingRequestHeaderException.class)
    public ResponseEntity<Map<String, Object>> handleMissingHeader(MissingRequestHeaderException e) {
        return build(HttpStatus.BAD_REQUEST, "Missing required header: " + e.getHeaderName());
    }

    /**
     * 요청 파라미터 타입 불일치(예: {@code limit}에 숫자가 아닌 값) → 400. 클라이언트 요청 결함이므로
     * 400으로 매핑한다. 미처리 시 catch-all로 500이 되어 클라이언트 오류가 서버 오류로 잘못 표기되는 것을 방지한다.
     */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<Map<String, Object>> handleTypeMismatch(MethodArgumentTypeMismatchException e) {
        return build(HttpStatus.BAD_REQUEST, "Invalid parameter '" + e.getName() + "': " + e.getValue());
    }

    @ExceptionHandler(SessionNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleSessionNotFound(SessionNotFoundException e) {
        return build(HttpStatus.NOT_FOUND, e.getMessage());
    }

    /**
     * 제약 위반(중복 키 등) → 409. 원문/스택은 로그로만 남기고 본문에는 고정 메시지만 노출해
     * DB 스키마·SQL 세부가 외부로 새지 않도록 한다.
     */
    @ExceptionHandler(DataIntegrityViolationException.class)
    public ResponseEntity<Map<String, Object>> handleDataIntegrity(DataIntegrityViolationException e) {
        log.warn("Data integrity violation", e);
        return build(HttpStatus.CONFLICT, "duplicate or constraint violation");
    }

    /**
     * 내부 상태 불변식 위반 → 500. 내부 상세는 로그로만 남기고 본문에는 고정 메시지만 노출한다.
     */
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalState(IllegalStateException e) {
        log.error("Illegal state", e);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleUnexpected(Exception e) {
        log.error("Unhandled exception", e);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error");
    }

    private ResponseEntity<Map<String, Object>> build(HttpStatus status, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", Instant.now().toString());
        body.put("status", status.value());
        body.put("error", status.getReasonPhrase());
        body.put("message", message);
        return ResponseEntity.status(status).body(body);
    }
}
