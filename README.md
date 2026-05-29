# realtime-chat

이벤트 소싱 + CQRS 기반 실시간 대화 세션 서버. 현재 범위는 **Phase 1 — 핵심 골격**(도메인 모델, Event Store + seq 채번 + 멱등, REST 수집/세션 CRUD, 동기 projection, snapshot+replay 복원, WebSocket 기본 송수신)이다.

## 기술 스택

- Java 17, Spring Boot 3.3.5 (Gradle Kotlin DSL)
- Spring JDBC (JdbcTemplate) + Flyway
- PostgreSQL 16

## 실행

```bash
# 1. 인프라 기동 (PostgreSQL)
docker-compose up -d

# 2. 애플리케이션 실행 (기동 시 Flyway가 V1 스키마 마이그레이션 적용)
./gradlew bootRun
```

- 애플리케이션 포트: `8080`
- DB 접속: `jdbc:postgresql://localhost:5432/realtimechat` (user/pw: `realtimechat`)

> 설계 근거는 `.dev/feat-realtime-chat/design.md` 참조.

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
