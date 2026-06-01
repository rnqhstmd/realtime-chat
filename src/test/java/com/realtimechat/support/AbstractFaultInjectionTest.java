package com.realtimechat.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.realtimechat.command.CommandHandler;
import com.realtimechat.common.json.JsonUtil;
import com.realtimechat.event.EventStore;
import com.realtimechat.event.EventType;
import com.realtimechat.event.StoredEvent;
import com.realtimechat.event.payload.MessageSentPayload;
import com.realtimechat.projection.MessageViewDao;
import com.realtimechat.session.SessionService;
import com.redis.testcontainers.RedisContainer;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * 장애 주입(fault injection) 통합 테스트 공통 베이스(설계서 §9, 장애 주입 테스트 설계서 §4).
 *
 * <p><b>{@link AbstractIntegrationTest}와 분리한 이유</b>: 기존 베이스는 PostgreSQL·Redis를 static
 * 싱글톤으로 1회 기동하고 JVM 종료까지 stop하지 않으며 여러 통합 클래스가 공유한다. 장애 주입
 * 테스트가 그 공유 컨테이너를 끊으면 같은 컨테이너를 쓰는 다른 모든 통합 테스트가 깨진다(원칙 P-1).
 * 따라서 fault 테스트는 <b>자체 컨테이너 토폴로지를 직접 관리</b>한다.
 *
 * <p><b>토폴로지</b>: 공유 도커 네트워크에 PostgreSQL·Redis·Toxiproxy를 띄우고, 애플리케이션은
 * 직결(@ServiceConnection) 대신 {@link DynamicPropertySource}로 주입한 <b>Toxiproxy 프록시 주소</b>를
 * 통해 DB/Redis에 연결한다. 테스트는 {@code cutDb()/healDb()/cutRedis()/healRedis()}로 프록시 경로를
 * 차단·복구하여 실제 네트워크 단절을 재현한다(원칙 P-2). 연결 거부(refused)가 즉시 발생하므로
 * 타임아웃을 기다리지 않아 결정적이다.
 *
 * <p><b>컨텍스트 격리</b>: 프록시 주소가 프로퍼티로 들어가 이 베이스는 기존 통합 테스트와 다른 Spring
 * 컨텍스트 캐시 키를 가진다. fault 테스트끼리는 같은 컨텍스트·컨테이너를 공유하므로, 각 테스트 종료 시
 * {@link #healAll()}로 프록시 상태를 복구하여 클래스 간 상태 누수를 막는다(worker stop 등 컴포넌트
 * 상태는 각 서브클래스의 {@code @AfterEach}가 복구).
 *
 * <p>타임아웃 단축은 {@code application-fault.yml}(fault 프로파일)이 적용한다. 비동기 파이프라인 튜닝은
 * {@code application-test.yml}(test 프로파일)을 상속한다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"test", "fault"})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
public abstract class AbstractFaultInjectionTest {

    /** PostgreSQL·Redis·Toxiproxy가 서로를 alias로 찾도록 공유하는 도커 네트워크. */
    private static final Network NETWORK = Network.newNetwork();

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("postgres");

    static final RedisContainer REDIS =
            new RedisContainer(DockerImageName.parse("redis:7").asCompatibleSubstituteFor("redis"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("redis");

    static final ToxiproxyContainer TOXIPROXY =
            new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
                    .withNetwork(NETWORK);

    /** PostgreSQL 앞단 프록시. setConnectionCut(true)로 앱↔DB 경로를 차단한다. */
    static final ToxiproxyContainer.ContainerProxy PG_PROXY;
    /** Redis 앞단 프록시. setConnectionCut(true)로 앱↔Redis 경로를 차단한다. */
    static final ToxiproxyContainer.ContainerProxy REDIS_PROXY;

    static {
        POSTGRES.start();
        REDIS.start();
        TOXIPROXY.start();
        PG_PROXY = TOXIPROXY.getProxy(POSTGRES, PostgreSQLContainer.POSTGRESQL_PORT);
        REDIS_PROXY = TOXIPROXY.getProxy(REDIS, 6379);
    }

    /**
     * 데이터소스·Redis 연결을 Toxiproxy 프록시 주소로 주입한다. {@code @ServiceConnection}을 쓰지 않는
     * 이유: 컨테이너 직결이 되면 프록시를 우회하여 장애 주입이 불가능하기 때문이다.
     */
    @DynamicPropertySource
    static void proxyProps(DynamicPropertyRegistry registry) {
        // ContainerProxy(testcontainers 1.19.8)에는 getHost()가 없다. 프록시 호스트는 Toxiproxy
        // 컨테이너의 host(보통 localhost)이고, 포트는 각 프록시의 getProxyPort()로 얻는다.
        //
        // socketTimeout/connectTimeout(초)은 DB 차단(setConnectionCut) 시 hang을 막는 핵심이다.
        // Hikari connection-timeout은 '풀에서 새 커넥션을 빌리는' 대기만 제한하므로, 이미 풀에 있던
        // 커넥션으로 쿼리하다 소켓이 끊기면 PostgreSQL JDBC 기본 socketTimeout=0(무한)으로 영원히
        // 블록된다. socketTimeout=2로 끊긴 커넥션의 쿼리를 2초 내 SQLException → DataAccessException으로
        // 전환한다(차단 감지 결정성 확보).
        registry.add("spring.datasource.url", () ->
                "jdbc:postgresql://" + TOXIPROXY.getHost() + ":" + PG_PROXY.getProxyPort()
                        + "/" + POSTGRES.getDatabaseName()
                        + "?socketTimeout=2&connectTimeout=2&loginTimeout=2");
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.redis.host", TOXIPROXY::getHost);
        registry.add("spring.data.redis.port", REDIS_PROXY::getProxyPort);
    }

    // ---- 공통 협력자(서브클래스 공유) ----

    @Autowired protected SessionService sessionService;
    @Autowired protected CommandHandler commandHandler;
    @Autowired protected EventStore eventStore;
    @Autowired protected MessageViewDao messageViewDao;
    @Autowired protected JdbcTemplate jdbcTemplate;

    /**
     * 각 테스트 종료 후 프록시 상태를 정상으로 되돌려 클래스 간 상태 누수를 막는다(공유 컨텍스트).
     * 컴포넌트 상태(worker stop 등)는 각 서브클래스의 {@code @AfterEach}가 복구한다.
     */
    @AfterEach
    void healAll() {
        PG_PROXY.setConnectionCut(false);
        REDIS_PROXY.setConnectionCut(false);
    }

    // ---- 장애 주입 헬퍼 ----

    protected void cutDb() {
        PG_PROXY.setConnectionCut(true);
    }

    protected void healDb() {
        PG_PROXY.setConnectionCut(false);
    }

    protected void cutRedis() {
        REDIS_PROXY.setConnectionCut(true);
    }

    protected void healRedis() {
        REDIS_PROXY.setConnectionCut(false);
    }

    // ---- 도메인 헬퍼 ----

    /** MESSAGE_SENT 이벤트를 CommandHandler 경로(REST/WS와 동일 수렴)로 수집한다. */
    protected StoredEvent sendMessage(UUID sessionId, UUID sender, String content, String key) {
        JsonNode payload = JsonUtil.toJsonNode(new MessageSentPayload(UUID.randomUUID(), sender, content));
        return commandHandler.handle(sessionId, EventType.MESSAGE_SENT, payload, key, sender);
    }

    /** 세션의 미발행(published=false) outbox 레코드 수. */
    protected int unpublishedOutboxCount(UUID sessionId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox WHERE session_id = ? AND published = FALSE",
                Integer.class, sessionId);
        return count == null ? 0 : count;
    }

    /** 세션의 projection_offset.last_applied_seq(행이 없으면 0). */
    protected long lastAppliedSeq(UUID sessionId) {
        Long seq = jdbcTemplate.queryForObject(
                "SELECT coalesce(max(last_applied_seq), 0) FROM projection_offset WHERE session_id = ?",
                Long.class, sessionId);
        return seq == null ? 0L : seq;
    }
}
