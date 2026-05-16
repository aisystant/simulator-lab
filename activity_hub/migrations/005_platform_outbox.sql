-- Migration 005: Event Bus — platform_outbox + worker_cursors
-- WP-109 Ф5: Слой 2 Activity Hub (межсистемная реакция через pg_notify)

CREATE SCHEMA IF NOT EXISTS operations;

-- Outbox: буфер событий для downstream consumers
CREATE TABLE IF NOT EXISTS operations.platform_outbox (
    id          BIGSERIAL PRIMARY KEY,
    event_id    BIGINT,                    -- soft ref к development.user_events(id)
    event_type  TEXT NOT NULL,
    event_class TEXT NOT NULL,             -- LEARNING | ECONOMIC | IDENTITY | SOCIAL
    user_uuid   UUID NOT NULL,
    payload     JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ               -- NULL = unprocessed by dispatcher
);

-- Частичный индекс: только необработанные записи (для replay)
CREATE INDEX IF NOT EXISTS idx_platform_outbox_unprocessed
    ON operations.platform_outbox (id)
    WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_platform_outbox_event_class
    ON operations.platform_outbox (event_class, created_at DESC)
    WHERE processed_at IS NULL;

-- Cursors: позиция каждого subscriber для recovery после рестарта
CREATE TABLE IF NOT EXISTS operations.worker_cursors (
    worker_name    TEXT PRIMARY KEY,
    last_outbox_id BIGINT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
