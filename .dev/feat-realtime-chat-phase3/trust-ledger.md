# Trust Ledger — Phase 3 통합 감사 (review)

> 2026-05-31. security-auditor 통합 감사 + qa-manager 리뷰 교차. 빌드+테스트 green(77/0).
> 분류: 조치 권장(in-scope 저비용) / 범위 외(인증 미구현, PRD 명시) / 기록(POC 인지, 후속).

## 조치 완료 (✅ 리뷰 반영 — 4건 수정·재검증 green, qa 확인 통과)
- ✅ **[GAP/HIGH] ResumeEventController afterSeq 음수 미검증** → `afterSeq < 0` 400 추가(0 허용).
- ✅ **[RISK/HIGH] STOMP heartbeat 세션 검증 누락** → `heartbeatOverStomp`에 `sessionDao.status` 미존재 시 silent ignore+로그.
- ✅ **[MEDIUM] STOMP heartbeat null participantId** → null 가드(touch 없이 return).
- ✅ **[RISK/HIGH] SessionFanoutListener 채널 sessionId 무검증** → 전달 전 `UUID.fromString` 검증, 비-UUID skip+WARN.

## (원본) 조치 권장 항목 — 상기 완료
- **[GAP/HIGH] ResumeEventController afterSeq 음수 미검증** — `afterSeq < 0`이면 `findBySeqRange`에 음수 전달 → seq 1부터 전체 반환 가능. seq는 1부터이므로 음수는 무효. → `afterSeq < 0` 400 추가. (afterSeq=0은 PRD 허용 엣지)
- **[RISK/HIGH] STOMP heartbeatOverStomp 세션 검증 누락** — REST heartbeat는 `sessionDao.status` 404 검증하나 STOMP는 미검증 → 유령 세션 liveness 키/tracked 멤버 생성(Redis 오염). QA도 Warning으로 중복 지적. → STOMP에도 세션 존재 검증 추가(또는 공통 로직 추출).
- **[MEDIUM] STOMP heartbeat @Valid 미적용 → null participantId NPE 위험** — `@MessageMapping`은 Bean Validation 미자동 적용. `participantId` null이면 touch에서 NPE/`presence:{sid}:null` 키. → null 가드 또는 `@Valid` 추가.
- **[RISK/HIGH] SessionFanoutListener 채널명 sessionId UUID 미검증** — redis 모드에서 `chat.fanout.*` 임의 채널 publish 시 sessionId가 STOMP 토픽으로 무검증 전달. → 파싱 후 `UUID.fromString` 검증, 실패 시 skip. (Redis ACL은 인프라 영역)

## 범위 외 (인증 미구현 — PRD "인증/인가 과제 범위 외" 명시)
- **[RISK/CRITICAL] Actuator 인증 없는 노출 + `show-details: always`** — `/actuator/health`(db/redis 연결정보), `/actuator/prometheus`(내부 메트릭) 무인증 노출. ※ `show-details: always`는 AC-13 검증 요건이며 테스트가 의존. 운영 배포 시 `when-authorized` + management port 분리 또는 Security 필요. → 가정 명시·기록(운영 전 필수).
- **[RISK/HIGH] PresenceSweeper OFFLINE actorId = 클라이언트 제공 participantId** — heartbeat 무인증이라 임의 participantId 주입 가능. 인증 도입 시 principal 대조 필요. → 범위 외 기록.

## 기록 (POC 인지, 후속 개선)
- [MEDIUM] resume `afterSeq + limit + 1` Long 오버플로우(afterSeq≈MAX_VALUE) → 빈 결과(무해). afterSeq 상한 cap 검토.
- [MEDIUM] `presence:tracked` 전역 단일 Set — 참여자 다수 시 SMEMBERS/순회 선형 부하(설계서 §10 트레이드오프 기인지). 운영화 시 세션별 Set/SSCAN.
- [MEDIUM] OFFLINE 멱등키 epoch-milli — SREM 직후~handle 전 크래시 시 재기동 후 중복 발행 가능(단일 인스턴스 정상 경로는 1회 보장). 후속: 고정 복합키+touch 무효화 또는 사이클 번호.
- [MEDIUM] `${HOSTNAME:worker-1}` consumer 이름 유일성 가정 — 다중 인스턴스 시 충돌 가능. 운영화 시 UUID 부여.
- [MEDIUM] SessionFanoutListener 역직렬화 실패 무시(BR-7 의도) — 반복 실패(스키마 불일치) 시 조용한 유실. → `chat.fanout.deserialize.failures` 메트릭 알람화 검토.
- [LOW] FanoutProps.channelPrefix는 설정 가능하나 SessionFanoutListener TOPIC_PREFIX는 코드 상수 — 의존 관계 문서화.
- [LOW] PresenceTracker member 파싱 UUID 전제 — 서버 생성 멤버이므로 indexOf(':') 정상. ID 형식 변경 시 깨짐 → Javadoc 전제 명시 권장.

## 검증 불요 (확인 완료)
- **prometheus 활성화 키**: `management.prometheus.metrics.export.enabled=true`는 Spring Boot 3.x 표준 키이며, `@SpringBootTest`가 주입하는 `management.defaults.metrics.export.enabled=false`를 prometheus 한정 오버라이드. debugger가 제거→500/추가→200으로 효과 검증. 테스트 한정 스코프 적절(운영은 기본 활성).

## PRD/설계 텍스트 괴리 (기능 정합, 문서 갱신 필요)
- PRD FR-P3-2 "STOMP SessionDisconnectEvent 즉시 OFFLINE" → 설계 결정 D로 1차 제외(폴링 sweep만). 구현은 설계 준수.
- PRD BR-3 "participant_view.presence 확인" → 설계에서 read model lag 문제로 SREM 원자 방식 대체(critic #1). 구현은 설계 준수, PRD 텍스트는 구 정책.
