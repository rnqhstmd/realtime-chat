# 부하 테스트 (k6) — 실행 가이드

BONUS-3 부하 테스트/성능 측정 산출물. 핵심 핫패스(이벤트 append, 시점 복원, 조회, 재연결 delta)의
TPS·latency를 **재현 가능한 방식(k6)**으로 측정한다.

- 스크립트: `load-test/k6/*.js`
- 공통 헬퍼: `load-test/k6/lib/common.js`
- 결과 문서: `docs/load-test/2026-06-01-load-test-results.md`

## 시나리오 개요

| 스크립트 | 대상 엔드포인트 | 부하(VU / ramp / 지속) | 측정 |
|----------|-----------------|------------------------|------|
| `append.js`  | `POST /sessions/{id}/events` (MESSAGE_SENT) | 10 / 5s / 30s | append TPS·latency |
| `restore.js` | `GET /sessions/{id}/timeline` | 5 / 5s / 30s | 복원 latency (스냅샷 無/有 2케이스) |
| `query.js`   | `GET /sessions/{id}/messages?limit=50` | 10 / 5s / 30s | 라이브 조회 latency |
| `resume.js`  | `GET /sessions/{id}/events?afterSeq=0&limit=100` | 10 / 5s / 30s | delta 페이징 latency |

각 스크립트는 `setup()`에서 자체 세션·데이터를 준비하므로 독립 실행 가능하다.

## 사전 조건

1. **인프라 기동** — PostgreSQL 16 + Redis 7 (프로젝트 루트):
   ```bash
   docker-compose up -d
   ```
2. **애플리케이션 기동 (8081 포트)** — 본 측정 환경에서 8080이 점유되어 8081을 사용했다. 8080이 비어 있으면 그대로 8080을 써도 되며, 그 경우 `BASE_URL`의 포트만 맞추면 된다:
   ```bash
   ./gradlew bootRun --args='--server.port=8081'
   ```
   또는:
   ```bash
   SERVER_PORT=8081 ./gradlew bootRun
   ```
   헬스 체크: `curl http://localhost:8081/actuator/health` → `{"status":"UP"}` 확인.

## 실행 — docker (k6 로컬 미설치 환경, 권장)

k6를 로컬에 설치하지 않고 `grafana/k6` 이미지로 실행한다. Docker Desktop(Windows/macOS)은
`host.docker.internal`을 기본 제공한다. **Linux Docker Engine**에서는 기본 제공되지 않으므로
docker run에 `--add-host=host.docker.internal:host-gateway`를 추가한다. 컨테이너에서 host 앱은
`host.docker.internal:8081`로 접근한다(`-e BASE_URL`로 주입).

프로젝트 루트에서 실행한다.

```bash
# append (FR-1)
docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 \
  -v "$(pwd)/load-test/k6:/scripts" grafana/k6 run /scripts/append.js

# restore — 스냅샷 無/有 두 케이스 (FR-2)
docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 \
  -v "$(pwd)/load-test/k6:/scripts" grafana/k6 run /scripts/restore.js

# query (FR-3)
docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 \
  -v "$(pwd)/load-test/k6:/scripts" grafana/k6 run /scripts/query.js

# resume delta (FR-8)
docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 \
  -v "$(pwd)/load-test/k6:/scripts" grafana/k6 run /scripts/resume.js
```

### Windows 경로 주의

- **Git Bash / WSL**: 위 `$(pwd)` 그대로 사용 가능.
- **PowerShell**: `$(pwd)` 대신 `${PWD}` 또는 절대경로를 쓴다:
  ```powershell
  docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 `
    -v "${PWD}/load-test/k6:/scripts" grafana/k6 run /scripts/append.js
  ```
- **cmd.exe**: 절대경로를 직접 지정한다:
  ```cmd
  docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 ^
    -v "D:/SQ/homework/load-test/k6:/scripts" grafana/k6 run /scripts/append.js
  ```

## 실행 — 로컬 k6 (설치된 경우)

```bash
cd load-test/k6
k6 run append.js
k6 run restore.js
k6 run query.js
k6 run resume.js
```

로컬 실행 시 `BASE_URL`은 기본값 `http://localhost:8081`이 적용된다. 포트가 다르면:

```bash
k6 run -e BASE_URL=http://localhost:9090 append.js
```

## 실행 순서 (권장)

1. `append.js` — 핫패스 append TPS 측정. 직후 `/actuator/prometheus`에서
   projection lag(`chat_projection_lag_millis{state="max"}`)을 스크랩하여 결과 문서에 기록.
2. `query.js` — 라이브 조회 latency.
3. `restore.js` — 복원 latency(스냅샷 無/有). setup 단계에서 스냅샷 有 세션에 1,200건 적재 +
   명시적 `POST /snapshots` 호출에 시간이 걸리므로 setup 타임아웃을 600s로 둔다.
4. `resume.js` — delta 페이징 latency.

## 튜닝 파라미터(환경변수, 선택)

스크립트 부하 규모는 PRD FR-4 고정값이지만, 사전 적재량 등은 `-e`로 조정 가능하다.

| 변수 | 스크립트 | 기본값 | 설명 |
|------|----------|--------|------|
| `BASE_URL` | 전체 | `http://localhost:8081` | 대상 앱 주소(BR-3) |
| `NO_SNAPSHOT_EVENTS` | restore | 150 | 스냅샷 無 케이스 사전 적재 수(trigger-interval 200 미만) |
| `SNAPSHOT_EVENTS` | restore | 1200 | 스냅샷 有 케이스 사전 적재 수(BR-2: 1,000건 이상) |
| `QUERY_SEED_EVENTS` | query | 300 | 조회 모집단 사전 적재 수 |
| `RESUME_SEED_EVENTS` | resume | 300 | delta 모집단 사전 적재 수 |

## 결과 확인 방법

- k6 기본 summary가 stdout에 출력된다. 다음 값을 결과 문서에 옮긴다:
  - **달성 TPS(req/s)**: 측정 구간(ramp 5s + plateau 30s = 35s) 기준 시나리오 처리율 = `iterations`(완료 수) / 35s.
    k6의 `http_reqs` rate는 setup 적재 요청·총 실행 시간을 포함해 시나리오 처리율을 과소 표기하므로 그대로 쓰지 않는다(결과 문서 §1 주석과 동일 기준).
  - **latency(ms)**: 스크립트별 커스텀 Trend(`append_latency` / `restore_no_snapshot_latency` /
    `restore_with_snapshot_latency` / `query_latency` / `resume_latency`)의 `med` / `p(95)` / `p(99)` / `max`.
  - **에러율(%)**: `http_req_failed` 의 `rate`.
- p99가 필요하면 JSON summary로 추출:
  ```bash
  docker run --rm -i -e BASE_URL=http://host.docker.internal:8081 \
    -v "$(pwd)/load-test/k6:/scripts" grafana/k6 run --summary-trend-stats="avg,min,med,p(50),p(90),p(95),p(99),max" \
    /scripts/append.js
  ```
- projection lag(append 기준, AC-5): append 측정 종료 직후 스크랩.
  ```bash
  curl -s http://localhost:8081/actuator/prometheus | grep chat_projection_lag_millis
  ```

## 멱등키 고유성 (BR-1 / AC-8)

append 시나리오의 각 요청은 `lib/common.js#uniqueIdempotencyKey()`가 생성하는
`idem-{vuId}-{iteration}-{counter}-{epochMs}` 형식의 고유 키를 `Idempotency-Key` 헤더로 전송한다.
동일 키 재사용에 의한 멱등 캐시 히트로 TPS가 부풀려지지 않는다(요청마다 고유).
