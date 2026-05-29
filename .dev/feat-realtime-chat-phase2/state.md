phase: implement
status: in_progress
vcs-type: git
branch: feat/realtime-chat-phase2
base: main
dev-dir: .dev/feat-realtime-chat-phase2
project-type: java-spring
project-root: ./
args: "phase2 구현시작"
flags: (none)
mode: normal
intent-source: user-selection
tech-stack: "Java 17, Spring Boot 3.3.5, Gradle KTS, JdbcTemplate+Flyway (Phase2: +spring-data-redis/Lettuce, Redis Stream, Awaitility/testcontainers-redis)"
package-base: com.realtimechat
started: 2026-05-29
current-step: "RESUME 지점 — 설계 승인·저장 완료, implement 미착수. design.md '구현 순서' 1번부터 시작할 것 (집에서 이어서 작업)."
decisions:
  - "Q1 전환전략 = 완전 비동기 (동기 projection 제거, CommandHandler는 append+outbox+broadcast만, 테스트 Awaitility 대기)"
  - "Q2 Outbox Relay = 폴링(500ms, published=false 부분인덱스)"
  - "Q3 Snapshot = Worker 처리 건수 기준(세션당 N건마다, 기본 200)"
  - "[design] 소비 메커니즘 = 수동 XREADGROUP 단일 루프(SmartLifecycle, 2-track race 차단)"
  - "[design] 백오프 = XAUTOCLAIM min-idle(30s) 근사, deliveryCount로 3회 판정"
  - "[design] 스냅샷 트리거 = afterCommit 별도 TX (리셋은 apply TX)"
  - "[design] Redis Testcontainer = RedisContainer + @ServiceConnection"
  - "[design] 다중 인스턴스 = POC 단일 인스턴스 전제 + consumer 이름 호스트명 유일"
critic-fixes-in-design:
  - "①카운터 멱등 권위 = per-row 전이 boolean 유지(projection_offset은 gap감지+스냅샷카운팅 전용, 카운터 가드 아님)"
  - "②touchActivity 시각 단조 가드 추가"
  - "③presence 늦은도착 drop 허용 + 근거 문서화"
  - "④outbox V2에 본문 컬럼 포함(Relay 재조회/race 제거)"
  - "⑤events 스트림 XADD MAXLEN trim"
phases:
  setup: completed
  requirements: completed
  design: completed
  implement: pending
  review: pending
  complete: pending
artifacts:
  - "prd.md (확정)"
  - "design.md (확정, 대형, 신규 13파일/수정 9파일, 구현순서 19단계)"
  - "codemap.md (Phase 2 주의사항 포함)"
domain-context: "context/realtime-chat (glossary, architecture) 로드됨"
references: "없음"
notes:
  - "Phase 1(핵심 골격)은 PR #2로 main 머지 완료. Phase 2는 main 기반 신규 브랜치."
  - "Phase 2 범위 = Outbox→Redis Stream→projection worker, 재시도/DLQ, snapshot 자동화."
  - "RESUME 방법: /gx-dev --resume (또는 'phase2 구현 이어서'). state.md 복원 후 implement 진입, design.md 구현순서대로 coder 배치 실행."
  - "구현 시작 전 코드 변경 없음(설계만 완료). 집 환경: gx-dev 플러그인 + .claude/config.json 필요, docker-compose(postgres+redis) 기동, Java 17, gradle wrapper 존재."
  - ".dev 산출물은 .gitignore 대상이나 이 브랜치에 git add -f로 강제 포함하여 origin 푸시함."
execution-log:
  - phase: setup
    result: "main 기반 feat/realtime-chat-phase2 생성, DEV_DIR 생성, 코드맵 작성, DOMAIN_CONTEXT 로드"
  - phase: requirements
    agent: product-owner
    result: "PRD 확정 (완전 비동기 전환). Must 9 / Should 2 / Could 1, BR 7, AC 11. 사용자 승인."
  - phase: design
    agent: architect + design-critic
    result: "설계 확정 (대형). design-critic MUST-ADDRESS 5건 정정 반영. 사용자 승인. design.md 저장."
