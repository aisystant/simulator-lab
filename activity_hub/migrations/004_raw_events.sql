-- Migration 004: Landing zone (raw_events) — medallion Слой 1
-- WP-109 Ф8. АрхГейт: LIST(source) → RANGE(fetched_at) sub-partitioning.
-- Retention: выключен (ручное решение после 1-2 мес обкатки, см. WP-214).
-- Sources: lms, bot, club, iwe (exocortex — event_type внутри iwe, не отдельный source).

CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;

-- 1. Корневая партиционированная таблица
CREATE TABLE IF NOT EXISTS development.raw_events (
    id BIGSERIAL,
    source TEXT NOT NULL,                       -- 'lms', 'bot', 'club', 'iwe'
    external_id TEXT NOT NULL,                  -- ID в источнике (идемпотентность)
    payload JSONB NOT NULL,                     -- сырьё как пришло
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    transform_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (transform_status IN ('pending', 'done', 'failed', 'skipped')),
    transform_error TEXT,
    transformed_at TIMESTAMPTZ,
    PRIMARY KEY (id, source, fetched_at),
    UNIQUE (source, external_id, fetched_at)
) PARTITION BY LIST (source);

COMMENT ON TABLE development.raw_events IS
    'Landing zone (medallion Слой 1). Сырьё один-в-один из источников. '
    'Transform-worker читает pending → пишет в user_events. '
    'WP-109 Ф8.';

-- 2. LIST-child партиции по источникам + их RANGE sub-partitioning
--    Каждый source — отдельная таблица со своим sub-partitioning по fetched_at.

CREATE TABLE IF NOT EXISTS development.raw_events_lms
    PARTITION OF development.raw_events
    FOR VALUES IN ('lms')
    PARTITION BY RANGE (fetched_at);

CREATE TABLE IF NOT EXISTS development.raw_events_bot
    PARTITION OF development.raw_events
    FOR VALUES IN ('bot')
    PARTITION BY RANGE (fetched_at);

CREATE TABLE IF NOT EXISTS development.raw_events_club
    PARTITION OF development.raw_events
    FOR VALUES IN ('club')
    PARTITION BY RANGE (fetched_at);

CREATE TABLE IF NOT EXISTS development.raw_events_iwe
    PARTITION OF development.raw_events
    FOR VALUES IN ('iwe')
    PARTITION BY RANGE (fetched_at);

-- 3. Индексы для transform-worker и наблюдаемости.
--    Используется `WHERE transform_status='pending' ORDER BY fetched_at`.

CREATE INDEX IF NOT EXISTS idx_raw_events_pending
    ON development.raw_events (fetched_at)
    WHERE transform_status = 'pending';

CREATE INDEX IF NOT EXISTS idx_raw_events_failed
    ON development.raw_events (fetched_at)
    WHERE transform_status = 'failed';

-- 4. Регистрация в pg_partman — автоматическое создание будущих sub-партиций.
--    Каждый LIST-child регистрируется отдельно (pg_partman управляет только
--    RANGE-уровнем). premake=2 → всегда держать 2 партиции вперёд.
--    Retention NULL → не дропать (ручное решение после обкатки, WP-214).
--    Идемпотентность: runner.py прогоняет все миграции каждый раз,
--    create_parent падает при повторной регистрации → проверяем part_config.

DO $$
DECLARE
    parent_name TEXT;
    parents TEXT[] := ARRAY[
        'development.raw_events_lms',
        'development.raw_events_bot',
        'development.raw_events_club',
        'development.raw_events_iwe'
    ];
BEGIN
    FOREACH parent_name IN ARRAY parents LOOP
        IF NOT EXISTS (
            SELECT 1 FROM partman.part_config WHERE parent_table = parent_name
        ) THEN
            PERFORM partman.create_parent(
                p_parent_table => parent_name,
                p_control => 'fetched_at',
                p_interval => '1 month',
                p_premake => 2
            );
        END IF;
    END LOOP;
END $$;

-- 5. Расширение sync_log для Ф8.8 monitoring (raw/transformed/failed счётчики).
--    Старые поля events_fetched/events_written/events_skipped остаются —
--    заполняются landing-фазой. Новые — transform-фазой.

ALTER TABLE development.sync_log
    ADD COLUMN IF NOT EXISTS raw_rows INTEGER,
    ADD COLUMN IF NOT EXISTS transformed_rows INTEGER,
    ADD COLUMN IF NOT EXISTS failed_rows INTEGER;

COMMENT ON COLUMN development.sync_log.raw_rows IS
    'Количество записей, попавших в raw_events (landing).';
COMMENT ON COLUMN development.sync_log.transformed_rows IS
    'Количество записей, успешно переведённых из raw в user_events (transform-worker).';
COMMENT ON COLUMN development.sync_log.failed_rows IS
    'Количество записей, упавших на transform (transform_status=failed).';
