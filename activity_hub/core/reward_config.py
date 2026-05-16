"""Прейскурант баллов — единственное место правки правил начисления.

# see DP.SC.133, DP.ROLE.043, WP-121 Ф2 v2, WP-319 Ф3.5

ВАЖНО: Не править reference.reward_rules в Neon напрямую.
Порядок изменения:
  1. Обновить константы в этом файле + поднять REWARD_CONFIG_VERSION
  2. Запустить: python scripts/generate_reward_rules_sql.py
  3. Применить сгенерированный SQL-патч к Neon reference DB
  4. Проверить: python scripts/validate_reward_config.py

Соответствует reference DB (ветка br-lingering-cake-aggtcdft, 8 мая 2026).
"""

# Версия конфига — менять при любой правке констант (аналог CONFIG_VERSION в stage_config.py)
REWARD_CONFIG_VERSION = "v1"
REWARD_CONFIG_DATE = "2026-05-16"

# ── Базовые суммы начисления (reference.reward_rules.amount) ─────────────────
# Ключ = event_type. Значение = базовая сумма в баллах ДО применения множителей.
# amount=0 = событие-условие (фиксируется для streak, но баллов не даёт).

BASE_AMOUNTS: dict[str, float] = {
    # LMS — освоение
    "lesson_completed":          10.0,
    "learning_completed":        12.0,
    "training_passed":           10.0,
    "training_attempt":           7.0,
    "test_passed":               12.0,
    "assessment_completed":      12.0,
    "task_submitted":            12.0,
    "text_submitted":             7.0,
    "table_submitted":            7.0,
    "feed_completed":             5.0,
    "pomodoro_completed":         5.0,
    "marathon_step":              5.0,
    "marathon_task":              7.0,
    "workbook_push":              5.0,
    "comment_created":            5.0,
    "topic_created":             12.0,
    # KE и Pack
    "knowledge_extracted":       35.0,
    "distinction_added":         46.0,
    "method_described":          46.0,
    "note_to_capture":            5.0,
    # ОРЗ ритуалы (amount=0 = условие систематичности, не баллы)
    "day_open":                   0.0,
    "day_close":                  0.0,
    "day_plan_opened":            0.0,
    "day_plan_closed":            0.0,
    "week_plan_created":          0.0,
    "week_plan_closed":           0.0,
    "month_plan_closed":          0.0,
    "slot_logged":                5.0,
    "strategy_session_completed": 15.0,
    # IWE
    "pack_updated":               7.0,
    "iwe_session":                5.0,
    "ai_chat":                    2.0,
    "ai_interaction":             2.0,
    # РП (в governance-репо → practice domain)
    "wp_created":                 5.0,
    "wp_closed":                 46.0,
    "wp_completed":              46.0,
    # Git и кодинг
    "commit_created":             7.0,
    "git_commit":                 7.0,
    "coding_time":                7.0,
    "content_published":         69.0,
    "fmt_commit_merged":         10.0,
    # Клуб (sandbox WP-296, числа согласовать с Ильшатом)
    "club_topic_created":        20.0,
    "club_post_created":         10.0,
    "club_like_created":          2.0,
    "club_user_created":          5.0,
}

# ── Домен по умолчанию для event_type ────────────────────────────────────────
# NULL в prod-системе = резолвится через repo_domain_map по payload.repo.
# В симуляторе используем дефолтный домен (governance-репо = practice).

EVENT_DOMAIN: dict[str, str] = {
    # learning — освоение нового
    "lesson_completed":          "learning",
    "learning_completed":        "learning",
    "training_passed":           "learning",
    "training_attempt":          "learning",
    "test_passed":               "learning",
    "assessment_completed":      "learning",
    "task_submitted":            "learning",
    "text_submitted":            "learning",
    "table_submitted":           "learning",
    "feed_completed":            "learning",
    "pomodoro_completed":        "learning",
    "marathon_step":             "learning",
    "marathon_task":             "learning",
    "workbook_push":             "learning",
    "comment_created":           "learning",
    "topic_created":             "learning",
    "knowledge_extracted":       "learning",
    "distinction_added":         "learning",
    "method_described":          "learning",
    "strategy_session_completed":"learning",
    "club_topic_created":        "learning",
    "club_post_created":         "learning",
    "club_like_created":         "learning",
    "club_user_created":         "learning",
    # practice — ОРЗ и IWE-инструмент
    "day_open":                  "practice",
    "day_close":                 "practice",
    "day_plan_opened":           "practice",
    "day_plan_closed":           "practice",
    "week_plan_created":         "practice",
    "week_plan_closed":          "practice",
    "month_plan_closed":         "practice",
    "slot_logged":               "practice",
    "pack_updated":              "practice",
    "iwe_session":               "practice",
    "note_to_capture":           "practice",
    "ai_chat":                   "practice",
    "ai_interaction":            "practice",
    "wp_created":                "practice",
    "wp_closed":                 "practice",
    "wp_completed":              "practice",
    # work — production-репо
    "commit_created":            "work",
    "git_commit":                "work",
    "coding_time":               "work",
    "content_published":         "work",
    "fmt_commit_merged":         "work",
}

# ── Множители домена (reference.activity_domain_multipliers) ─────────────────
# Seed: 203-seeds-reference-multipliers.sql

DOMAIN_MULT: dict[str, float] = {
    "learning": 3.0,   # освоение нового знания
    "practice": 5.0,   # тренировка изученного (ОРЗ, IWE, Pack)
    "work":     1.0,   # рабочая активность
}

DOMAIN_DAILY_CAP: dict[str, float] = {
    "learning": 100.0,
    "practice": 200.0,
    "work":      50.0,
}

# ── Множители ступеней Ученика (reference.student_stage_multipliers) ──────────
# Применяется при qualification_level=4 (Ученик).
# Seed: 203-seeds-reference-multipliers.sql

STAGE_MULT: dict[int, float] = {
    1: 1.0,   # Случайный
    2: 1.2,   # Практикующий
    3: 1.5,   # Систематический
    4: 2.0,   # Дисциплинированный
    5: 2.5,   # Проактивный
}

STAGE_DAILY_CAP: dict[int, float] = {
    1:  50.0,
    2:  80.0,
    3: 120.0,
    4: 200.0,
    5: 300.0,
}

# ── Streak-eligible event types (reference.reward_rules.streak_eligible) ─────
# Seed: 204-reward-rules-streak-eligible.sql

STREAK_ELIGIBLE: frozenset[str] = frozenset({
    "coding_time",
    "commit_created",
    "lesson_completed",
    "training_attempt",
    "pomodoro_completed",
    "knowledge_extracted",
    "note_to_capture",
    "distinction_added",
    "method_described",
    "ai_chat",
    "ai_interaction",
    "marathon_step",
    "marathon_task",
    "task_submitted",
    "text_submitted",
    "table_submitted",
    "feed_completed",
    "comment_created",
    "test_passed",
    "assessment_completed",
    "learning_completed",
})
