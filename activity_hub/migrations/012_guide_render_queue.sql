-- Migration 012: Guide render queue (WP-309 Ф7)
-- Applies to: Neon learning DB (LEARNING_URL / NEON_LEARNING_URL)
--
-- Очередь триггерного рендера персональных руководств.
-- Заполняется:
--   stage_transition_listener.py (trigger_type='stage_transition')
--   gateway-mcp create_repository handler (trigger_type='repo_created')
--   manual INSERT для force-render (trigger_type='manual')
-- Читается:
--   render-pilot-guides.py --queue-only (каждые 10 мин, systemd-timer)
--   render-pilot-guides.py routine run (начало, перед cron-рендером)

CREATE TABLE IF NOT EXISTS learning.guide_render_queue (
    queue_id        BIGSERIAL    PRIMARY KEY,
    account_id      UUID         NOT NULL,
    trigger_type    TEXT         NOT NULL CHECK (trigger_type IN (
                                    'repo_created',
                                    'stage_transition',
                                    'routine',
                                    'manual'
                                )),
    trigger_payload JSONB        NOT NULL DEFAULT '{}'::jsonb,
    status          TEXT         NOT NULL DEFAULT 'pending' CHECK (status IN (
                                    'pending',
                                    'in_progress',
                                    'done',
                                    'failed',
                                    'dead_letter'
                                )),
    attempts        INT          NOT NULL DEFAULT 0,
    max_attempts    INT          NOT NULL DEFAULT 3,
    last_error      TEXT,
    requested_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    worker_id       TEXT
);

CREATE INDEX IF NOT EXISTS idx_guide_render_queue_pending
    ON learning.guide_render_queue (status, requested_at)
    WHERE status IN ('pending', 'failed');

CREATE INDEX IF NOT EXISTS idx_guide_render_queue_pilot
    ON learning.guide_render_queue (account_id, requested_at DESC);

COMMENT ON TABLE learning.guide_render_queue IS
    'Очередь рендера персональных руководств. WP-309 Ф7.';
COMMENT ON COLUMN learning.guide_render_queue.trigger_type IS
    'repo_created: первый рендер после создания репо; stage_transition: смена ступени; routine: cron; manual: ручной force-render.';
COMMENT ON COLUMN learning.guide_render_queue.worker_id IS
    'ID воркера (hostname:pid) — для trace при нескольких инстанциях render-pilot-guides.';
