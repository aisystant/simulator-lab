-- Migration 001: Activity Hub tables
-- Расширение user_events + новые таблицы для Hub

-- 1. Добавить external_id и ingested_at в user_events
ALTER TABLE development.user_events
    ADD COLUMN IF NOT EXISTS external_id TEXT,
    ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ DEFAULT NOW();

-- 2. Unique index для dedup: (source, external_id)
-- WHERE external_id IS NOT NULL — не трогает старые записи без external_id
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_source_external
    ON development.user_events (source, external_id)
    WHERE external_id IS NOT NULL;

-- 3. Identity map — маппинг внешних ID на user_uuid
CREATE TABLE IF NOT EXISTS development.identity_map (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,           -- 'lms', 'club', 'iwe'
    external_id TEXT NOT NULL,      -- ID в источнике
    user_uuid UUID NOT NULL,        -- Ory UUID
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (source, external_id)
);

-- 4. Карантин — невалидные события
CREATE TABLE IF NOT EXISTS development.quarantined_events (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    external_id TEXT,
    user_ref JSONB,
    event_type TEXT,
    payload JSONB,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 5. Лог синхронизации
CREATE TABLE IF NOT EXISTS development.sync_log (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    events_fetched INTEGER DEFAULT 0,
    events_written INTEGER DEFAULT 0,
    events_skipped INTEGER DEFAULT 0,
    events_quarantined INTEGER DEFAULT 0,
    status TEXT,        -- 'success', 'partial', 'failed'
    error_message TEXT,
    reconciliation JSONB
);
