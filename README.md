# realtime-chat

이벤트 소싱 + CQRS 기반 실시간 대화 세션 서버. 현재 범위는 **Phase 1 — 핵심 골격**(도메인 모델, Event Store + seq 채번 + 멱등, REST 수집/세션 CRUD, 동기 projection, snapshot+replay 복원, WebSocket 기본 송수신)이다.

## 기술 스택

- Java 17, Spring Boot 3.3.5 (Gradle Kotlin DSL)
- Spring JDBC (JdbcTemplate) + Flyway
- PostgreSQL 16

## 실행

아래 "실행"은 **호스트 개발용**(인프라만 컨테이너 + 앱은 `bootRun`)이다. 앱까지 컨테이너로 전체 스택을 한 번에 기동하려면 [모니터링 스택](#모니터링-스택)을 참조한다.

```bash
# 1. 인프라 기동 (PostgreSQL)
docker-compose up -d

# 2. 애플리케이션 실행 (기동 시 Flyway가 V1 스키마 마이그레이션 적용)
./gradlew bootRun
```

- 애플리케이션 포트: `8080`
- DB 접속: `jdbc:postgresql://localhost:5432/realtimechat` (user/pw: `realtimechat`)

> 설계 근거는 `.dev/feat-realtime-chat/design.md` 참조.

## 모니터링 스택

앱·PostgreSQL·Redis·Prometheus·Grafana 전체를 컨테이너로 한 번에 기동한다(앱은 멀티스테이지 `Dockerfile`로 빌드된다).

```bash
# 전체 스택 기동 (앱 컨테이너 포함). 최초 1회는 앱 이미지 빌드로 수 분 소요될 수 있다.
docker compose up --build
```

기동 후:

- **Grafana**: `http://localhost:3000` — 로그인 불필요(익명 Viewer). 대시보드 **"Realtime Chat — Operations"**가 자동 로드된다(프로비저닝). 패널:
  - Projection Lag (last / max): 투영 지연(ms). 각각 5000ms / 10000ms 수평 임계선 표시.
  - Stream Processed / Outbox Relayed: 스트림 처리·아웃박스 릴레이 처리량(`rate(...[1m])`).
  - DLQ Failures: DLQ 적재 실패 누적(0=정상 green, 1 이상 red).
  - HTTP Request Rate: 엔드포인트별 요청률.
  - JVM Heap Used: 힙 메모리 사용량.
- **Prometheus**: `http://localhost:9090` — 타깃 상태는 `http://localhost:9090/targets`에서 `realtime-chat-app`(app:8080) 확인. 15초 간격으로 `/actuator/prometheus` 스크랩.
- **앱 헬스 확인**: `curl http://localhost:8080/actuator/health` → `{"status":"UP"}`.

> 앱 컨테이너는 헬스체크를 정의하지 않으므로(런타임 이미지에 wget/curl 미보장) Prometheus/Grafana는 앱 기동 여부와 무관하게 항상 뜬다. 앱이 늦게 뜨면 Prometheus 타깃이 잠시 DOWN으로 표시되다가 자동 복구된다.

> 앱 healthcheck가 없어 스택 기동 직후 Prometheus 첫 스크랩까지 시차가 있다. `docker compose up` 후 약 15~30초 대기한 뒤 Grafana에 접속하면 패널에 데이터가 채워진다.

### 포트 오버라이드

`9090`/`3000`이 점유되어 있으면 환경변수로 변경한다(앱 포트 `8080`은 고정).

```bash
PROMETHEUS_PORT=19090 GRAFANA_PORT=13000 docker compose up --build
```

## Phase 1 보안 가정 및 비범위

Phase 1은 핵심 골격 검증이 목적이며, 아래 항목은 의도적으로 비범위(Out of Scope)다.

- **인증 없음 (Phase 2+ 대체)**: `actorId`/`senderId`는 클라이언트가 자기 선언한 값이다(서버가 신원을 검증하지 않음). Phase 2+에서 인증 도입 시 인증 컨텍스트(Principal)로 대체한다.
- **WebSocket origin 와일드카드 + 구독 권한 미검증 (Phase 2+)**: STOMP 엔드포인트는 `setAllowedOriginPatterns("*")`로 모든 origin을 허용하며 토픽 구독 권한도 검증하지 않는다. 인증 도입 시 실제 도메인으로 제한(CSWSH 방지)하고 구독 권한을 검증한다.
- **로컬 DB 크리덴셜은 개발 전용**: `application.yml`의 DB user/password는 로컬 docker-compose 전용이다. 운영은 `SPRING_DATASOURCE_PASSWORD` 등 환경변수/Vault로 주입한다.
- **관측성·비동기 파이프라인 단계 구분**: 구조화 로그/메트릭/추적 등 관측성은 Phase 3, outbox 기반 비동기 projection 파이프라인은 Phase 2 항목이다(Phase 1은 outbox 테이블만 선행 생성, 동기 projection 사용).

### 입력 제약 (적용됨)

- **limit 상한**: `/timeline`·`/messages`의 `limit`은 1~500 범위만 허용한다(범위 밖 → 400). 기본값은 messages 50, timeline `chat.timeline.recent-messages`(기본 200)다.
- **idempotency_key 길이 제한**: `Idempotency-Key` 헤더는 최대 200자다(공백·초과 → 400).
- **at 미래 시각 처리**: `/timeline?at=`에 미래 시각을 주면 "현재 시점"의 상태로 정상 복원된다(에러 아님). `at` 미지정 시에도 현재 시점(maxSeq, 시계 무관)으로 복원한다.
