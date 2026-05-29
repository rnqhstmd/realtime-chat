-- =====================================================================
-- V1__init_schema.sql
-- Realtime Chat — Event Sourcing + CQRS schema (Phase 1)
-- Source: design.md §3 (데이터 모델 · 인덱스 근거)
-- =====================================================================

-- ---------------------------------------------------------------------
-- §3.1 쓰기 측 (Write side): session / event / outbox
-- ---------------------------------------------------------------------

CREATE TABLE session (
    id          UUID PRIMARY KEY,
    status      TEXT NOT NULL DEFAULT 'ACTIVE',   -- ACTIVE / ENDED
    last_seq    BIGINT NOT NULL DEFAULT 0,         -- 세션별 seq 채번 카운터
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at    TIMESTAMPTZ
);

CREATE TABLE event (
    event_id        UUID PRIMARY KEY,
    session_id      UUID NOT NULL REFERENCES session(id),
    seq             BIGINT NOT NULL,
    event_type      TEXT NOT NULL,   -- MESSAGE_SENT/EDITED/DELETED, PARTICIPANT_JOINED/LEFT, PRESENCE_CHANGED
    payload         JSONB NOT NULL,
    idempotency_key TEXT NOT NULL,
    actor_id        UUID,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_event_seq  UNIQUE (session_id, seq),
    CONSTRAINT uq_event_idem UNIQUE (session_id, idempotency_key)
);
CREATE INDEX ix_event_time ON event (session_id, occurred_at);  -- at(timestamp) → seq 매핑

CREATE TABLE outbox (
    id         BIGSERIAL PRIMARY KEY,
    event_id   UUID NOT NULL,
    session_id UUID NOT NULL,
    seq        BIGINT NOT NULL,
    published  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_outbox_unpub ON outbox (id) WHERE published = FALSE;  -- 미발행분만 부분 인덱스

-- ---------------------------------------------------------------------
-- §3.2 읽기 측 (Projections): participant_view / message_view / session_view
-- ---------------------------------------------------------------------

CREATE TABLE participant_view (
    session_id       UUID NOT NULL,
    participant_id   UUID NOT NULL,
    status           TEXT NOT NULL,   -- JOINED / LEFT
    presence         TEXT,            -- ONLINE / OFFLINE
    joined_seq       BIGINT,
    left_seq         BIGINT,
    last_applied_seq BIGINT NOT NULL, -- projection 멱등 가드
    PRIMARY KEY (session_id, participant_id)
);
CREATE INDEX ix_part_active ON participant_view (session_id, status);

CREATE TABLE message_view (
    session_id       UUID NOT NULL,
    message_id       UUID NOT NULL,
    seq              BIGINT NOT NULL, -- 생성 seq (정렬 키)
    sender_id        UUID NOT NULL,
    content          TEXT,
    state            TEXT NOT NULL,   -- SENT / EDITED / DELETED
    created_at       TIMESTAMPTZ,
    last_updated_seq BIGINT NOT NULL,
    PRIMARY KEY (session_id, message_id)
);
CREATE INDEX ix_msg_recent ON message_view (session_id, seq DESC);  -- 최근 N개 조회

CREATE TABLE session_view (
    session_id        UUID PRIMARY KEY,
    status            TEXT NOT NULL,
    participant_count INT NOT NULL DEFAULT 0,
    message_count     BIGINT NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL,
    last_activity_at  TIMESTAMPTZ,
    ended_at          TIMESTAMPTZ
);
CREATE INDEX ix_session_list ON session_view (status, created_at DESC);  -- 목록/필터

-- ---------------------------------------------------------------------
-- §3.3 스냅샷 (Snapshot)
-- ---------------------------------------------------------------------

CREATE TABLE snapshot (
    session_id UUID NOT NULL,
    up_to_seq  BIGINT NOT NULL,
    state      JSONB NOT NULL,  -- { participants[], recent_messages[N], counts }
    taken_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, up_to_seq)
);
