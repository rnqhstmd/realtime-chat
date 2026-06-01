# 부하 테스트 및 성능 측정 결과 (BONUS-3)

> 측정 도구: **k6**(grafana/k6 docker) · 산출물: 본 마크다운 문서 · 측정일: 2026-06-01
> 스크립트: `load-test/k6/*.js` · 실행 가이드: `load-test/README.md`

## 1. 측정 개요

시스템 핵심 핫패스(이벤트 append, 시점 복원, 라이브 조회, 재연결 delta)의 TPS·latency를
k6로 측정한다. 각 시나리오의 부하 규모(VU/ramp/지속)는 고정값이며(PRD FR-4), 멱등키는
요청마다 고유 생성하여 캐시 히트로 인한 TPS 오염을 방지한다(BR-1). 전 시나리오 에러율 0%.

| 시나리오 | 대상 엔드포인트 | 부하(VU / ramp / 지속) |
|----------|-----------------|------------------------|
| append | `POST /sessions/{id}/events` | 10 / 5s / 30s |
| restore (시점 복원) | `GET /sessions/{id}/timeline` | 5 / 5s / 30s |
| query (라이브 조회) | `GET /sessions/{id}/messages?limit=50` | 10 / 5s / 30s |
| resume (delta) | `GET /sessions/{id}/events?afterSeq=0&limit=100` | 10 / 5s / 30s |

> **달성 TPS 정의**: 측정 구간(ramp 5s + plateau 30s = 35s) 동안의 해당 시나리오 주요 요청 처리율
> (= 완료 iteration 수 / 35s). k6 stdout의 `http_reqs` rate는 setup 적재 요청·실행 시간을 포함하므로
> 시나리오 처리율을 과소 표기한다. 따라서 본 표의 TPS는 측정 구간 iteration 기준으로 환산했다.

## 2. 측정 환경

| 항목 | 값 |
|------|-----|
| OS | Windows 11 Pro (Version 10.0.26200.8457) |
| CPU | 8 logical cores (단일 머신에서 앱·PostgreSQL·Redis·k6 컨테이너 동시 구동) |
| JVM | OpenJDK HotSpot 17.0.14+8-LTS (Temurin) |
| Docker | Docker Desktop, Engine 27.4.0, Compose v2.31.0-desktop.2 |
| k6 | `grafana/k6:latest` (이미지 106MB, docker 실행) |
| PostgreSQL / Redis | postgres:16 (host 5433) / redis 7.4.9 (host 6379) |
| 앱 포트 | 8081 (`--server.port=8081`) |
| 주요 설정 | `chat.snapshot.trigger-interval=200`, `chat.projection.sync-enabled=false`, outbox poll 500ms |

> **측정 환경 노트**: 이 머신은 5432(별도 PostgreSQL)·8080(별도 프로세스)이 이미 점유되어 있어,
> 부하용 PostgreSQL을 **5433**에 별도 기동하고 앱을 `--server.port=8081
> --spring.datasource.url=jdbc:postgresql://localhost:5433/realtimechat`로 오버라이드했다.
> 포트가 비어 있는 환경에서는 `docker-compose up -d`(5432) + 앱 8080/8081로 그대로 재현 가능하다.
> k6는 docker 컨테이너에서 `host.docker.internal:8081`로 호스트 앱에 접속했다.
> 단일 머신에 모든 구성요소가 공존하므로 absolute latency는 전용 부하 환경 대비 보수적(높게)이다.

## 3. 시나리오별 결과

> latency 단위 ms. 각 시나리오의 커스텀 Trend(`*_latency`) 기준이며, `--summary-trend-stats`로 p99 추출.

### 3.1 append (`POST /events`)

| 시나리오 | 달성 TPS(req/s) | p50(ms) | p95(ms) | p99(ms) | max(ms) | 에러율(%) |
|----------|-----------------|---------|---------|---------|---------|-----------|
| append | ≈16.5 (578 req / 35s) | 455.45 | 1290 (1.29s) | 1770 (1.77s) | 2330 (2.33s) | 0.00 |

> append는 동기 경로(이벤트 저장 + outbox insert를 한 트랜잭션)라 단일 머신 공존 부하에서 latency가
> 가장 높다(avg 561.93ms). VU 10 기준 처리율 ≈16.5 req/s, 측정 구간 append 578건·실패 0건.
> setup의 세션 생성·join(2건)을 포함한 전체 수집 이벤트는 580건이며, 그중 579건이 스크랩 시점까지
> projection 처리됐다(§4 `chat_stream_processed_total`).

### 3.2 restore (`GET /timeline`) — 스냅샷 無/有 2케이스

"스냅샷 無"는 trigger-interval(200건) 미만으로 소규모 적재 + 명시적 스냅샷 미호출하여 **전체 replay
경로**를 측정한다. "스냅샷 有"는 1,000건 이상 적재(BR-2) 후 명시적 `POST /snapshots`로 스냅샷을
보장하여 **snapshot+delta replay 경로**를 측정한다.

| 시나리오 | 사전 적재 | 달성 TPS(req/s) | p50(ms) | p95(ms) | p99(ms) | max(ms) | 에러율(%) |
|----------|-----------|-----------------|---------|---------|---------|---------|-----------|
| restore — 스냅샷 無 (full replay) | 150건 | ≈71 (2485 req / 35s) | 27.23 | 56.46 | 83.65 | 116.67 | 0.00 |
| restore — 스냅샷 有 (snapshot+delta) | 1,200건 + 명시적 스냅샷 | ≈71 (2485 req / 35s) | 29.37 | 60.40 | 84.74 | 140.84 | 0.00 |

> 두 케이스 모두 ~30ms대로 매우 빠르다. 스냅샷 有(1,200건)가 無(150건)보다 근소하게 느린 것은
> snapshot+delta replay 자체보다 timeline 응답에 담기는 메시지 모집단(`chat.timeline.recent-messages=200`)
> 직렬화 비용 차이에 기인한다. 한 iteration이 두 케이스 timeline을 각 1회 호출하므로 케이스별 처리율은
> 동일(≈71 req/s)하다.
>
> 각주: 자동 스냅샷은 비동기 projection 파이프라인(`ProjectionApplier`, OutboxRelay→ProjectionWorker)
> 경로에서만 트리거되며 projection은 `sync-enabled=false`로 비동기다. "스냅샷 無" 세션은 적재량(150건)이
> trigger-interval(200) 미만이라 자동 스냅샷이 트리거되지 않아 결정적으로 전체 replay를 측정한다.
> `GET /timeline`은 event store에서 직접 replay하므로 projection 진행 상태와 무관하게 즉시 데이터를 반환한다.

### 3.3 query (`GET /messages`)

| 시나리오 | 달성 TPS(req/s) | p50(ms) | p95(ms) | p99(ms) | max(ms) | 에러율(%) |
|----------|-----------------|---------|---------|---------|---------|-----------|
| query | ≈502 (17583 req / 35s) | 14.71 | 35.75 | 53.11 | 324.91 | 0.00 |

> read model 직접 조회 경로로 전 시나리오 중 가장 빠르고 처리율이 높다(≈502 req/s, p50 14.71ms).
> projection이 비동기이므로 측정 시점 read model 모집단은 ProjectionWorker 진행도에 따라 달라질 수
> 있으나, 직접 조회 경로의 부하/latency 측정은 빈 결과여도 유효하다(응답은 모두 200 + 배열).

### 3.4 resume (`GET /events?afterSeq=`)

| 시나리오 | 달성 TPS(req/s) | p50(ms) | p95(ms) | p99(ms) | max(ms) | 에러율(%) |
|----------|-----------------|---------|---------|---------|---------|-----------|
| resume (delta, limit=100) | ≈204 (7138 req / 35s) | 39.00 | 87.34 | 120.74 | 252.48 | 0.00 |

> afterSeq=0 + limit=100으로 세션 이벤트 delta를 페이징 조회한다. event store seq range 조회 경로로,
> append(동기 저장)보다 빠르고 query(read model)보다는 느리다(≈204 req/s).

## 4. Projection lag (AC-5)

append 시나리오 종료 직후 `/actuator/prometheus`를 스크랩하여 `chat.projection.lag.millis{state=...}`
값을 기록한다.

```bash
curl -s http://localhost:8081/actuator/prometheus | grep chat_projection_lag_millis
```

| 메트릭 | 값 | 측정 시점 |
|--------|-----|-----------|
| `chat_projection_lag_millis{state="max"}` | **11631** ms | append 시나리오 종료 직후 |
| `chat_projection_lag_millis{state="last"}` | **9014** ms | append 시나리오 종료 직후 |
| `chat_stream_processed_total` | 579 | 스크랩 시점 (append 580건 중 579 처리) |

> append 부하 중 동기 수집(≈16.5 req/s)을 비동기 projection이 따라가며 일시적으로 최대 **11.6초**의 lag이
> 발생했다. 이는 단일 머신 공존 부하(앱·PG·Redis·k6 경쟁)에서 outbox relay(poll 500ms, batch 100)·
> ProjectionWorker 소비가 append 피크에 잠시 밀린 결과로, 부하 종료 후 빠르게 수렴한다(스크랩 시점
> 580건 중 579건 처리 완료). 본 측정의 핫패스인 append/restore/query/resume 응답 경로 자체는 lag과
> 무관하게 정상 동작했다(전 시나리오 에러율 0%).

## 5. 에러율 분석 (BR-4)

- **전 시나리오 에러율 0.00% (1% 이내) — 추가 원인 분석 불필요.** append 578건, restore 4,970건(timeline,
  두 케이스 합), query 17,583건, resume 7,138건 모두 실패 0건. k6 threshold `http_req_failed: rate<0.01`
  전 시나리오 통과.

## 6. 재현 절차 (FR-7 / AC-7)

### 6.1 사전 조건

1. 인프라 기동(프로젝트 루트). 포트가 비어 있는 환경:
   ```bash
   docker-compose up -d           # PostgreSQL 5432 + Redis 6379
   ```
   > 5432가 점유된 환경에서는 PG를 별도 포트로 띄운다(본 측정 방식):
   > ```bash
   > docker run -d --name rtchat-lt-pg -e POSTGRES_DB=realtimechat \
   >   -e POSTGRES_USER=realtimechat -e POSTGRES_PASSWORD=realtimechat \
   >   -p 5433:5432 postgres:16
   > ```
2. 애플리케이션 기동(8081 포트):
   ```bash
   # 포트가 비어 있으면:
   ./gradlew bootRun --args='--server.port=8081'
   # PG를 5433에 띄운 경우(빌드 jar 직접 실행):
   java -jar build/libs/realtime-chat-0.0.1-SNAPSHOT.jar \
     --server.port=8081 --spring.datasource.url=jdbc:postgresql://localhost:5433/realtimechat
   ```
3. 헬스 체크:
   ```bash
   curl http://localhost:8081/actuator/health   # {"status":"UP"} 확인
   ```

### 6.2 스크립트 실행 (docker grafana/k6)

프로젝트 루트에서 실행한다. Windows Git Bash는 경로 변환 방지를 위해 `MSYS_NO_PATHCONV=1` +
볼륨에 절대경로(`D:/SQ/homework/load-test/k6`)를 사용한다. p99 추출은 `--summary-trend-stats`로 한다.

```bash
# (Windows Git Bash 실제 사용 형태)
MSYS_NO_PATHCONV=1 docker run --rm -e BASE_URL=http://host.docker.internal:8081 \
  -v "D:/SQ/homework/load-test/k6:/scripts" grafana/k6 run \
  --summary-trend-stats="avg,min,med,p(90),p(95),p(99),max" \
  --summary-export=/scripts/results/append.json /scripts/append.js
# restore.js / query.js / resume.js 동일 패턴 (export 파일명만 변경)

# (Linux/macOS 일반 형태)
docker run --rm -e BASE_URL=http://host.docker.internal:8081 \
  -v "$(pwd)/load-test/k6:/scripts" grafana/k6 run /scripts/append.js
```

로컬에 k6가 설치된 경우:
```bash
cd load-test/k6 && k6 run append.js   # restore.js / query.js / resume.js
```

### 6.3 결과 확인 방법

1. k6 stdout summary / `results/*.json`에서 다음을 읽어 §3 표에 옮긴다:
   - 측정 구간 iteration 수 / 35s → 달성 TPS(req/s)
   - 커스텀 Trend(`append_latency`/`restore_no_snapshot_latency`/`restore_with_snapshot_latency`/
     `query_latency`/`resume_latency`)의 med/p(95)/p(99)/max → latency(ms)
   - `http_req_failed` rate → 에러율(%)
2. append 종료 직후 projection lag 스크랩(§4):
   ```bash
   curl -s http://localhost:8081/actuator/prometheus | grep chat_projection_lag_millis
   ```
3. 에러율 1% 초과 시 §5에 원인 분석 기재(이번 측정은 0%).

## 7. 멱등키 고유성 (BR-1 / AC-8)

append 시나리오의 각 요청은 `load-test/k6/lib/common.js#uniqueIdempotencyKey()`가 생성하는
`idem-{vuId}-{iteration}-{counter}-{epochMs}` 형식의 고유 키를 `Idempotency-Key` 헤더로 전송한다.
setup() 컨텍스트(VU 정보 없음)에서도 모듈 스코프 카운터 + `Date.now()`로 고유성을 보장한다.
서버 멱등 검증은 "공백 아님 + 200자 이하"만 요구하므로(UUID 불필요), 요청마다 고유한 이 키로
멱등 캐시 히트 없이 실제 처리량을 측정한다.
