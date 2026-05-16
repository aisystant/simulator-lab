-- Migration 011: Points Engine cursor для learning.domain_event (WP-121 Ф2 Neon)
-- Applies to: Neon platform DB (SUBSCRIPTION_URL), points schema
--
-- После WP-268 cut-over события пилотов идут в learning.domain_event (Neon),
-- а не в development.user_events (Railway). Cursor для нового pipeline хранится
-- рядом с points.* — в той же Neon platform DB.

CREATE TABLE IF NOT EXISTS points.domain_event_cursor (
    worker_name    TEXT        PRIMARY KEY,
    last_event_id  BIGINT      NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE points.domain_event_cursor IS
    'Cursor per worker для инкрементальной обработки learning.domain_event';
