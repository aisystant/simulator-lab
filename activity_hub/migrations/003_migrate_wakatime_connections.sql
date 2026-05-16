-- Migration 003: перенос данных из wakatime_connections → development.user_integrations
-- WP-109/WP-7: объединение таблиц WakaTime (удаление dual write)
--
-- Предусловия:
--   - wakatime_connections существует (public schema, ключ: chat_id)
--   - development.user_integrations существует (миграция 002)
--   - public.users содержит telegram_id → id (UUID) маппинг
--
-- Что делает:
--   1. Копирует записи из wakatime_connections в user_integrations
--      (только для пользователей с user_uuid в public.users)
--   2. ON CONFLICT DO NOTHING — не перезаписывает существующие OAuth-токены
--   3. НЕ удаляет wakatime_connections (rollback safety)
--
-- Запуск: вручную перед деплоем бота на pilot
--   psql $DATABASE_URL -f 003_migrate_wakatime_connections.sql

INSERT INTO development.user_integrations
    (user_uuid, service, access_token, scope, metadata, connected_at, updated_at, active)
SELECT
    u.id AS user_uuid,
    'wakatime' AS service,
    wc.api_key AS access_token,
    'read_stats' AS scope,
    CASE
        WHEN wc.wakatime_username IS NOT NULL
        THEN jsonb_build_object('wakatime_username', wc.wakatime_username)
        ELSE '{}'::jsonb
    END AS metadata,
    wc.connected_at AS connected_at,
    NOW() AS updated_at,
    TRUE AS active
FROM wakatime_connections wc
JOIN public.users u ON u.telegram_id = wc.chat_id
WHERE u.id IS NOT NULL
ON CONFLICT (user_uuid, service) DO NOTHING;

-- Отчёт: сколько записей мигрировано vs пропущено
DO $$
DECLARE
    total_wc INT;
    total_ui INT;
    no_uuid INT;
BEGIN
    SELECT COUNT(*) INTO total_wc FROM wakatime_connections;
    SELECT COUNT(*) INTO total_ui
    FROM development.user_integrations
    WHERE service = 'wakatime' AND active = TRUE;
    SELECT COUNT(*) INTO no_uuid
    FROM wakatime_connections wc
    LEFT JOIN public.users u ON u.telegram_id = wc.chat_id
    WHERE u.id IS NULL;

    RAISE NOTICE 'wakatime_connections: % записей', total_wc;
    RAISE NOTICE 'user_integrations (wakatime, active): % записей', total_ui;
    RAISE NOTICE 'wakatime_connections без user_uuid (пропущены): %', no_uuid;
END $$;
