-- Migration 002: User Integrations (OAuth tokens)
-- Хранение OAuth-токенов для GitHub, WakaTime и других интеграций.
-- Пользователь подключается через бота (/connect github, /connect wakatime).
-- IWE-адаптер на сервере использует эти токены для сбора данных.

CREATE TABLE IF NOT EXISTS development.user_integrations (
    id SERIAL PRIMARY KEY,
    user_uuid UUID NOT NULL,
    service TEXT NOT NULL,            -- 'github', 'wakatime', 'google_calendar', ...
    access_token TEXT NOT NULL,       -- OAuth access token
    refresh_token TEXT,               -- OAuth refresh token (для обновления)
    token_expires_at TIMESTAMPTZ,    -- когда access_token истекает
    scope TEXT,                       -- 'repo:read', 'read_stats', ...
    metadata JSONB DEFAULT '{}',     -- github_username, wakatime_user_id, ...
    connected_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    active BOOLEAN DEFAULT TRUE,
    UNIQUE (user_uuid, service)
);

-- Индекс для быстрого поиска активных интеграций по сервису
CREATE INDEX IF NOT EXISTS idx_integrations_service_active
    ON development.user_integrations (service, active)
    WHERE active = TRUE;

-- RLS: каждый пользователь видит только свои интеграции
-- (применяется при доступе через бота/MCP, не при серверном sync)
ALTER TABLE development.user_integrations ENABLE ROW LEVEL SECURITY;
