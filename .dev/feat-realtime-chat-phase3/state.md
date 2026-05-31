phase: complete
status: completed
vcs-type: git
branch: feat/realtime-chat-phase3
base: main
dev-dir: .dev/feat-realtime-chat-phase3
project-type: java-spring
project-root: ./
args: "phase3구현시작"
flags: (none)
mode: normal
intent-source: user-selection
tech-stack: "Java 17, Spring Boot 3.3.5, Gradle KTS, PostgreSQL(JdbcTemplate+Flyway), Redis(spring-data-redis/Lettuce; Stream+예정 Pub/Sub), WebSocket/STOMP, Awaitility/testcontainers"
package-base: com.realtimechat
started: 2026-05-30
auto-stashed: false
last-known-head: b25e8b70e5c8656eac694b5926b973636506346e
current-step: "완료 — PR #4 생성(https://github.com/rnqhstmd/realtime-chat/pull/4). 머지는 리뷰어 수행."
acceptance: "product-owner ACCEPT — [Must] AC-1~11,14,15 + [Should] AC-12,13 충족. SessionDisconnectEvent 즉시 OFFLINE은 Phase 4 권장."
pr: "https://github.com/rnqhstmd/realtime-chat/pull/4"
status-md-updated: "FR-12, FR-13, FR-14 → ✅ (#4)"
context-updated: "glossary(heartbeat/presence TTL/관측성 메트릭) + architecture(Realtime Gateway·REST API Phase 3 반영)"
review-result:
  - "Mechanical Gate: build+test green(77/0)."
  - "QA: CERTAIN 0 / Warning 2 / QUESTION 3. AC-1~15 전부 테스트 커버."
  - "ZT: CRITICAL 1(Actuator 무인증=PRD 인증 범위외 기록) / HIGH 5 / MEDIUM 5 / LOW 2."
  - "사용자 결정: in-scope 하드닝 4건 수정(afterSeq<0 400 / STOMP heartbeat 세션검증·null가드 / fanout sessionId UUID검증)."
  - "수정 후 재검증 77/0 green, qa-manager 확인 리뷰 통과(잔여 Critical 0)."
  - "범위외/기록 항목은 trust-ledger.md."
implement-steps:
  - "구현 계획 승인: completed"
  - "배치 구성(B1~B4): completed"
  - "coder 구현 B1(골격): completed (빌드 green)"
  - "coder 구현 B2(팬아웃/resume/presence인프라/메트릭 4병렬): completed (빌드 green)"
  - "coder 구현 B3(presence Controller·Sweeper / 카운터 호출부 2병렬): completed (빌드 green)"
  - "coder 구현 B4(MDC [Could]): completed (빌드 green)"
  - "자기점검(qa-manager): completed — Critical 0, Warning/Info 4, QUESTION 3 (phase-review 이월)"
  - "테스트 작성: completed — 신규 5파일(Resume 8/Actuator 2/Presence 4/Fanout 2/Broadcaster단위 1)"
  - "테스트 실행: completed — 전체 77 tests / 0 failures (AC-15 회귀 포함 green)"
fixes-during-test:
  - "PresenceController: heartbeat(UUID,HeartbeatRequest) REST/STOMP 중복 시그니처 → STOMP 메서드 heartbeatOverStomp로 개명(컴파일 에러 수정). ※ | tail 파이프가 앞선 컴파일 검증 exit code를 마스킹해 B1~B4 미검출."
  - "AC-12 prometheus 500: @SpringBootTest가 management.defaults.metrics.export.enabled=false 주입 → PrometheusScrapeEndpoint 미생성. application-test.yml에 management.prometheus.metrics.export.enabled=true 추가(테스트 한정, 운영은 기본 정상). debugger 규명·수정."
design-decisions:
  - "A presence = Redis TTL + heartbeat는 경량 ping(이벤트 아님). PRESENCE_CHANGED는 상태전이(ONLINE 진입/OFFLINE)에만. PRD FR-P3-2 deviation."
  - "B BR-3 중복방지 = Redis tracked Set의 SREM 원자 반환값(==1)으로 OFFLINE 1회 보장. read model 가드 폐기(findPresence 불요)."
  - "C 팬아웃 기본 mode=local(@ConditionalOnProperty matchIfMissing), redis는 명시 프로파일. 기존 테스트 회귀 회피."
  - "D disconnect 즉시 OFFLINE(StompPresenceListener) 후순위(헤더 규약 확정 후). 1차는 5s 폴링 sweep만."
  - "E resume=raw 이벤트 delta, timeline=fold 상태. hasMore=limit+1 조회."
  - "F limit 공용 LimitSupport, ApiExceptionHandler 타입미스매치 400, 메트릭 ChatMetrics 집약, presence 컴포넌트 2개(Tracker+Sweeper)로 통합."
  - "G AC-1 다중인스턴스 단일 JVM 검증 한계 → 백플레인 동작+수동 데모 문서화."
  - "DB V3 불요. CommandHandler 수정 0."
decisions:
  - "Q1 Phase 3 범위 = 4개 전부(팬아웃 FR-P3-1 + presence TTL FR-P3-2 + resume-by-seq FR-P3-3 + 관측성 FR-P3-4/5/6). 관측성은 [Should]."
  - "Q2 presence = 클라이언트 heartbeat(30s) + Redis 키 TTL(90s) + 만료감지 스케줄러(5s 폴링), SessionDisconnectEvent 즉시 트리거 보조."
  - "Q3 resume 응답 = events 배열 + hasMore 필드(페이징 신호). limit 기본 100/최대 500."
candidate-scope: "Phase 2 PRD 제외범위 = Redis Pub/Sub 팬아웃, presence TTL 자동 갱신, resume-by-seq 고도화, 관측성 대시보드 (정확 범위는 requirements에서 확정)"
domain-context: "context/realtime-chat (glossary, architecture) 로드됨"
references: "없음 (references/ 디렉토리 부재)"
notes:
  - "베이스 main 자동 선택(단일 후보). main은 b25e8b7 최신(Phase 1 PR#2 + Phase 2 PR#3 머지 완료)."
  - "setup 직전 .gitignore에 .research/ 추가 변경 1건이 워킹트리에 존재(이전 작업 stash 복원). Phase 3 커밋에 포함될 수 있음."
  - "Step 6 .gitignore: .gradle//build/ 이미 존재. .dev/ 패턴은 저장소가 .dev 산출물을 추적·커밋하는 기존 컨벤션(main에 .dev/feat-realtime-chat-phase2 커밋됨) 보존을 위해 의도적으로 미추가."
  - "프로젝트 루트 CLAUDE.md/AGENTS.md 없음. 컨벤션은 context/realtime-chat/architecture.md + phase2 codemap 참조."
phases:
  setup: completed
  requirements: completed
  design: completed
  implement: completed
  review: completed
  complete: completed
  implement: pending
  review: pending
  complete: pending
execution-log:
  - phase: setup
    result: "main 기반 feat/realtime-chat-phase3 생성, DEV_DIR 생성, 코드맵 작성, DOMAIN_CONTEXT(glossary+architecture) 로드, REFERENCES 없음 확인"
  - phase: requirements
    agent: product-owner
    result: "PRD 확정. Must 3(FR-P3-1/2/3) / Should 2(FR-P3-4/5) / Could 1(FR-P3-6), BR 7, QE 4, AC 15. Q&A 3건 사용자 확정(범위 4개 전부·heartbeat+TTL·hasMore). 사용자 승인."
