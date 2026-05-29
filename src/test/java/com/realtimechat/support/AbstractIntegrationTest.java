package com.realtimechat.support;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 통합 테스트 공통 베이스(설계서 §12 통합 레벨, PRD 수용 기준 8).
 *
 * <p>실제 PostgreSQL을 Testcontainers로 띄우고, Spring Boot 3.1+의 {@link ServiceConnection}으로
 * 컨테이너 JDBC 연결을 동적 주입한다(고정 datasource url 불필요).
 *
 * <p><b>싱글톤 컨테이너 패턴</b>: {@code @Testcontainers}/{@code @Container} 생명주기 관리를
 * 쓰지 않고, static 초기화에서 컨테이너를 1회 기동한 뒤 JVM 종료까지 살려 둔다(Ryuk가 정리).
 * 이유: 추상 베이스에서 {@code @Testcontainers}는 <b>각 구상 클래스가 끝날 때마다 static 컨테이너를
 * stop</b>한다. 그러면 첫 통합 클래스 이후 두 번째 클래스가 캐시된 Spring 컨텍스트(이미 죽은
 * 컨테이너 포트를 가리킴)로 붙어 connection refused가 난다. 여러 통합 클래스가 한 컨테이너를
 * 공유하려면 stop을 막아야 한다(이미지 pull·기동은 JVM당 1회).
 *
 * <p>스키마는 Flyway V1 마이그레이션이 컨테이너에 적용한다(설계서 §3, PRD R8). Hibernate가 없는
 * JdbcTemplate 스택이므로 ddl-auto는 무관하며 Flyway가 유일한 스키마 권위다.
 *
 * <p>테스트 격리: 각 테스트는 새로 생성한 sessionId로 데이터를 분리한다(공유 컨테이너이지만 세션
 * 단위로 데이터가 격리되어 교차 오염이 없다). 전역 카운트에 의존하는 단언은 두지 않는다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public abstract class AbstractIntegrationTest {

    /**
     * 모든 통합 테스트가 공유하는 PostgreSQL 16 컨테이너(싱글톤). static 초기화 블록에서 1회만
     * 기동하고 stop하지 않는다. {@link ServiceConnection}이 spring.datasource.* 프로퍼티를 자동
     * 연결한다.
     */
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16");

    static {
        POSTGRES.start();
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Flyway V1 마이그레이션이 컨테이너에 적용되어 모든 테이블이 존재하는지 확인한다(PRD R8).
     * 이 단언은 베이스 클래스 자체에서 실행되어, 통합 환경 부팅(컨테이너+Flyway+빈 배선)이
     * 정상임을 최소 1회 보증한다.
     */
    @Test
    void flywayCreatesSchemaOnContainer() {
        for (String table : new String[] {
                "session", "event", "outbox",
                "participant_view", "message_view", "session_view", "snapshot"
        }) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM information_schema.tables "
                            + "WHERE table_schema = 'public' AND table_name = ?",
                    Integer.class, table);
            assertThat(count)
                    .as("table '%s' should be created by Flyway V1", table)
                    .isEqualTo(1);
        }
    }
}
