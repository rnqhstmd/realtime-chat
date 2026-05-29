-- =====================================================================
-- V2__async_pipeline.sql
-- Realtime Chat — 비동기 파이프라인 스키마 확장 (Phase 2)
-- Source: design.md §3 (outbox 확장 · projection_offset 신규)
-- =====================================================================

-- ---------------------------------------------------------------------
-- §3.1 outbox 확장: 이벤트 본문 컬럼 추가
-- nullable 추가. Phase 2 이후 INSERT는 항상 본문 채움.
-- POC 신규 전개라 backfill 불요.
-- ---------------------------------------------------------------------

ALTER TABLE outbox
    ADD COLUMN event_type      TEXT,
    ADD COLUMN payload         JSONB,
    ADD COLUMN idempotency_key TEXT,
    ADD COLUMN actor_id        UUID,
    ADD COLUMN occurred_at     TIMESTAMPTZ;

-- ---------------------------------------------------------------------
-- §3.4 프로젝션 오프셋 (Projection offset)
-- ---------------------------------------------------------------------

CREATE TABLE projection_offset (
    session_id            UUID PRIMARY KEY,
    last_applied_seq      BIGINT NOT NULL DEFAULT 0,
    events_since_snapshot BIGINT NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
