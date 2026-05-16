-- Migration 013: Cp-профиль ученика (WP-318 Ф3)
-- Applies to: Neon learning DB (LEARNING_URL / NEON_LEARNING_URL)
--
-- Хранит результаты диагностики R28 (DP.ROLE.042).
-- Источник алгоритма: PD.FORM.089 §6.1 v4.2.
-- Service Clause: DP.SC.132.
--
-- Privacy: хранится только UUID (account_id), числа и коды слотов.
-- Raw-текст ответов пилота НЕ хранится.

-- ============================================================
-- 1. Основная таблица cp-профилей
-- ============================================================

CREATE TABLE IF NOT EXISTS learning.cp_assessments (
    id                  BIGSERIAL    PRIMARY KEY,
    account_id          UUID         NOT NULL,

    -- Вычисленный cp-профиль (FORM.089 §6.1 формула cp_confirmed_stage)
    stage               SMALLINT     NOT NULL CHECK (stage BETWEEN 1 AND 5),
    bottleneck_slot     TEXT,        -- 'cp.rhy', 'cp.wld', 'cp.skl', 'cp.iwe', 'cp.int', 'cp.agt'
    recommended_stream  TEXT         CHECK (recommended_stream IN ('S1', 'S2', 'S3', 'S4')),
    skip_to_stage       SMALLINT     CHECK (skip_to_stage BETWEEN 1 AND 5),

    -- Детальные оценки по слотам
    cp_scores           JSONB        NOT NULL DEFAULT '{}',
    -- {"cp.rhy": 3, "cp.wld": 2, "cp.skl": 1, "cp.iwe": 2, "cp.int": 3, "cp.agt": 2}

    -- Метаданные сессии
    source              TEXT         NOT NULL CHECK (source IN ('dialogue', 'bh_proxy', 'import')),
    interface           TEXT         NOT NULL CHECK (interface IN ('tg', 'web', 'vscode', 'background')),
    questions_count     SMALLINT,    -- кол-во вопросов, фактически заданных (≤5)
    rcs_version         TEXT         DEFAULT 'v4.2',  -- версия рубрик FORM.089

    -- TTL
    assessed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    valid_until         TIMESTAMPTZ, -- assessed_at + 6 months для mandatory-срезов

    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cp_assessments_account_latest
    ON learning.cp_assessments (account_id, assessed_at DESC);

CREATE INDEX IF NOT EXISTS idx_cp_assessments_valid
    ON learning.cp_assessments (account_id, valid_until)
    WHERE valid_until IS NOT NULL;

COMMENT ON TABLE learning.cp_assessments IS
    'Cp-профили ученика от Диагноста R28 (DP.ROLE.042, WP-318). '
    'stage = min(cp_scores по 6 mandatory-слотам). TTL=6 мес. '
    'Privacy: только UUID + числа + коды слотов, без raw-текста ответов.';

COMMENT ON COLUMN learning.cp_assessments.cp_scores IS
    'JSONB: {cp.rhy, cp.wld, cp.skl, cp.iwe, cp.int, cp.agt} + информационные cp.*. '
    'Mandatory = [cp.rhy, cp.wld, cp.skl, cp.iwe, cp.int, cp.agt].';

COMMENT ON COLUMN learning.cp_assessments.skip_to_stage IS
    'Skip-level вход: с какой ступени начинать (= stage). '
    'Инвариант: skip_to_stage = stage (FORM.089 §6.1).';

COMMENT ON COLUMN learning.cp_assessments.valid_until IS
    'TTL для mandatory-срезов: assessed_at + 6 мес. '
    'NULL = бессрочный (informational-only срез).';

-- ============================================================
-- 2. FK в learning.stage_transitions — связь gate с cp-срезом
-- ============================================================

ALTER TABLE learning.stage_transitions
    ADD COLUMN IF NOT EXISTS cp_assessment_id BIGINT
        REFERENCES learning.cp_assessments(id) ON DELETE SET NULL;

COMMENT ON COLUMN learning.stage_transitions.cp_assessment_id IS
    'Ссылка на cp-срез, подтвердивший переход (двойной gate FORM.089 §5.1). '
    'NULL = переход по bh без cp-подтверждения (legacy или cp недоступен).';

-- ============================================================
-- 3. View: сигналы инвалидации для фонового Диагноста (DP.ROLE.042 §3.4)
-- ============================================================

CREATE OR REPLACE VIEW learning.cp_invalidation_signals AS
SELECT
    a.account_id,
    a.id            AS assessment_id,
    a.stage,
    a.bottleneck_slot,
    a.assessed_at,
    a.valid_until,
    NOW() - a.assessed_at AS age,
    CASE
        WHEN a.valid_until < NOW() THEN 'ttl_expired'
        WHEN NOW() - a.assessed_at > INTERVAL '90 days' THEN 'age_90d'
        ELSE NULL
    END AS invalidation_reason
FROM learning.cp_assessments a
WHERE
    -- Последний срез на account_id
    a.id = (
        SELECT id FROM learning.cp_assessments
        WHERE account_id = a.account_id
        ORDER BY assessed_at DESC
        LIMIT 1
    )
    -- Только истёкшие или старые 90+ дней
    AND (
        a.valid_until < NOW()
        OR NOW() - a.assessed_at > INTERVAL '90 days'
    );

COMMENT ON VIEW learning.cp_invalidation_signals IS
    'Сигналы инвалидации cp-срезов для фонового Диагноста (DP.ROLE.042 §3.4). '
    'Читается Hermes-агентом diagnostician-watcher (WP-316). '
    'Диагност пишет diagnostic.invalidation_proposed в learning.domain_event при срабатывании.';
