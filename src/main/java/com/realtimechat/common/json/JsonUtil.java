package com.realtimechat.common.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * 공유 ObjectMapper 래퍼.
 *
 * <p>이벤트 payload(JSONB) 직렬화/역직렬화의 단일 진입점이다. JavaTimeModule을 등록하고
 * 타임스탬프를 숫자가 아닌 ISO-8601 문자열로 직렬화하여 {@link java.time.Instant} 등을
 * 안정적으로 다룬다.
 *
 * <p>Spring 빈으로 주입받아 사용하며, 정적 컨텍스트(RowMapper 등)에서도 쓸 수 있도록
 * 공유 인스턴스 접근자를 함께 제공한다.
 */
public final class JsonUtil {

    private static final ObjectMapper MAPPER = buildMapper();

    private JsonUtil() {
    }

    private static ObjectMapper buildMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /** 공유 ObjectMapper. 동일 설정을 외부에서 재사용할 때 노출한다. */
    public static ObjectMapper mapper() {
        return MAPPER;
    }

    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize object to JSON", e);
        }
    }

    public static JsonNode toJsonNode(Object value) {
        return MAPPER.valueToTree(value);
    }

    public static <T> T fromNode(JsonNode node, Class<T> type) {
        try {
            return MAPPER.treeToValue(node, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to deserialize JSON node to " + type.getName(), e);
        }
    }

    public static JsonNode readTree(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to parse JSON string", e);
        }
    }
}
