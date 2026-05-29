## 코드 맵: 실시간 대화 세션 (이벤트 소싱) — Phase 1

### 핵심 파일
- `src/main/resources/db/migration/V1__init_schema.sql` → Flyway V1 스키마 (session/event/outbox + participant/message/session_view + snapshot, 제약·인덱스)
- `event/EventStore.java` → append-only 이벤트 저장소. 멱등 seq 채번(선조회→UPDATE last_seq RETURNING→INSERT ON CONFLICT), JSONB는 `CAST(:payload AS jsonb)`, `findBySeqRange(sid,fromExcl,toIncl)`, `maxSeqAtOrBefore(sid,at)`, `findByIdempotencyKey`. 호출자 트랜잭션 가정.
- `common/json/JsonUtil.java` → 공유 ObjectMapper(JavaTime, ISO ts). 모든 직렬화 단일 진입점.
- `RealtimeChatApplication.java` → Spring Boot 진입점
- `build.gradle.kts` → 빌드(Spring Boot 3.3.5 / Java 17 / JdbcTemplate+Flyway)

### 참조 파일
- `event/EventType.java` + `event/payload/*.java` → 6종 이벤트(MESSAGE_SENT/EDITED/DELETED, PARTICIPANT_JOINED/LEFT, PRESENCE_CHANGED) + payload record
- `event/StoredEvent.java`, `event/AppendCommand.java` → store 입출력 DTO(record, payload=JsonNode)
- `common/web/ApiExceptionHandler.java` → 전역 예외 매핑(@RestControllerAdvice)
- `common/web/IdempotencyKeys.java` → `Idempotency-Key` 헤더 상수/검증(require/isValid)
- `common/error/SessionNotFoundException.java`(404), `InvalidEventException.java`(400)
- `projection/ProjectionUpdater.java` → 동기 projection 오케스트레이터(@Component). `apply(StoredEvent)` 6종 분기. CommandHandler가 append 직후 동일 TX에서 호출.
- `projection/{ParticipantViewDao,MessageViewDao,SessionViewDao}.java` → 뷰 DAO. seq-guard UPDATE, applyJoined/applySent는 신규 INSERT 여부(boolean) 반환→카운터 중복증가 방지(xmax=0/affected). findRecent, list(status), 카운터.
- `restore/Fold.java` → 순수함수 fold(§4.3 전이). I/O 없음. B6 단위테스트 핵심.
- `restore/SessionState.java`(+ParticipantState/MessageState record) → 복원 상태 누적기. recentMessages(n).
- `restore/{SnapshotDao,SnapshotState,RestoreService,SnapshotService}.java` → 스냅샷 DAO(멱등 save), state JSONB 포맷 record, 복원 오케스트(restoreTo/restoreAt), 스냅샷 생성(recent N=`chat.snapshot.recent-messages:200`).

### 설정
- `application.yml` → 앱/DB/Flyway. `docker-compose.yml` → postgres:16, redis:7(future). `settings.gradle.kts`, gradle wrapper 8.10.2

### 컨벤션 (B2 확립)
- 순수 Java(Lombok 미사용), 생성자 주입, record DTO, UUID/Instant, NamedParameterJdbcTemplate, JsonUtil 직렬화.
