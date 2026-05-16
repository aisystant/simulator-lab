"""Stage Attestor configuration — WP-310 Ф7.

# see PD.FORM.089-learner-rcs.md §12.3, iwe-actions-catalog.md §7

Единое место для нормативов Аттестатора. Изменение порога = правка здесь + рестарт воркера.
Импортируется из stage_evaluator.py, stage_transition_listener.py, stage_simulator.py.
"""
from __future__ import annotations

# ── Версия нормативов (для симулятора WP-319 — NBR ветка 1) ─────────────────
CONFIG_VERSION = "v4"
CONFIG_DATE = "2026-05-15"   # дата последней правки нормативов

# ── Окна расчёта (i.accounting_period) ──────────────────────────────────────

# Количество недель для окна расчёта по текущей ступени пользователя
ACCOUNTING_WEEKS: dict[int, int] = {1: 1, 2: 4, 3: 8, 4: 12, 5: 24}

# ── Hard gate накопленных часов (all-time) ────────────────────────────────────
# К4 (WP-310): источники — slot_logged + lesson_completed.
# coding_time исключён: WakaTime считает все репо (включая рабочие), не только учёбу.

CUMULATIVE_HOURS_GATE: dict[int, float] = {2: 20.0, 3: 48.0, 4: 96.0, 5: 161.0}

# ── Multi-window пороги ───────────────────────────────────────────────────────

# Систематичность: (результирующий_idx, недели_окна, мин_дней_в_нед)
S_THRESHOLDS: list[tuple[int, int, float]] = [
    (5, 24, 6.7),
    (4, 12, 6.0),
    (3,  8, 5.0),
    (2,  4, 3.0),
    (1,  1, 1.0),
]

# Инвестированное время: (результирующий_idx, недели_окна, мин_часов_в_нед)
T_THRESHOLDS: list[tuple[int, int, float]] = [
    (5, 24, 10.0),
    (4, 12,  8.0),
    (3,  8,  6.0),
    (2,  4,  4.0),
    (1,  1,  0.5),
]

# ── Матрица минимальных требований ступеней (§12.3) ──────────────────────────
# Ключ = ступень, значение = (min_s, min_t, min_m, min_w, min_a)
STAGE_GATE_MATRIX: dict[int, tuple[int, int, int, int, int]] = {
    5: (5, 5, 4, 4, 4),   # bh.met ≥ 4 (исправлено WP-310: было 3)
    4: (4, 4, 2, 3, 2),
    3: (3, 3, 1, 2, 1),
    2: (2, 2, 0, 0, 0),
}

# ── bh.stb нормативы ─────────────────────────────────────────────────────────

# STB_GATE[stage] = мин. индекс bh.stb (устойчивость) для ступени.
# Индекс: ≤1д→5, 2-3д→4, 4-7д→3, 8-14д→2, 15-30д→1, >30д→0 (norm_stb).
STB_GATE: dict[int, int] = {2: 1, 3: 2, 4: 3, 5: 4}

# ── Нормализация характеристик → индекс 0–5 ─────────────────────────────────

def norm_m(count: int) -> int:
    """Методичность: событий за 30д → индекс."""
    if count == 0:   return 0
    if count <= 3:   return 1
    if count <= 10:  return 2
    if count <= 25:  return 3
    if count <= 50:  return 4
    return 5


def norm_w(score: float) -> int:
    """Системность мировоззрения: взвешенная сумма → индекс (§7.4)."""
    if score == 0:   return 0
    if score <= 4:   return 1
    if score <= 12:  return 2
    if score <= 28:  return 3
    if score <= 50:  return 4
    return 5


def norm_a(score: float) -> int:
    """Агентность: взвешенная сумма → индекс (§7.5)."""
    if score == 0:   return 0
    if score <= 3:   return 1
    if score <= 8:   return 2
    if score <= 17:  return 3
    if score <= 30:  return 4
    return 5


def norm_stb(max_gap_days: int) -> int:
    """bh.stb: макс. разрыв без SELF_DEV активности (дней) → индекс 0–5."""
    if max_gap_days <= 1:  return 5
    if max_gap_days <= 3:  return 4
    if max_gap_days <= 7:  return 3
    if max_gap_days <= 14: return 2
    if max_gap_days <= 30: return 1
    return 0


# ── Основной расчёт ступени ──────────────────────────────────────────────────

def compute_stage_mvp(
    s: int, t: int, m: int, w: int, a: int,
    total_hours: float = 0.0,
    stb: int = 5,
) -> int:
    """FORM.089 §12.3: ступень по 5 индексам + hard gates.

    Mandatory gate: s==0 или t==0 → ступень 1.
    Матрица: STAGE_GATE_MATRIX — все 5 характеристик должны быть >= порога.
    Hard gate: total_hours < CUMULATIVE_HOURS_GATE[stage] → понижаем.
    stb gate: stb < STB_GATE[stage] (bh.stb — макс. разрыв без SELF_DEV) → понижаем.
    """
    if s == 0 or t == 0:
        return 1
    stage = 1
    for lvl in (5, 4, 3, 2):
        min_s, min_t, min_m, min_w, min_a = STAGE_GATE_MATRIX[lvl]
        if s >= min_s and t >= min_t and m >= min_m and w >= min_w and a >= min_a:
            stage = lvl
            break
    while stage >= 2 and total_hours < CUMULATIVE_HOURS_GATE.get(stage, 0.0):
        stage -= 1
    while stage >= 2 and stb < STB_GATE.get(stage, 0):
        stage -= 1
    return stage


# ── Веса событий ─────────────────────────────────────────────────────────────

# Системность мировоззрения (w) — §7.4
W_WEIGHTS: dict[str, int] = {
    "week_plan_closed": 1,        # Gap-А: quality field не заполнен → fallback weight
    "month_plan_closed": 8,
    "strategy_session_completed": 6,
    "knowledge_extracted": 1,
    "pack_updated": 1,
}

# Агентность (a) — §7.5
A_WEIGHTS: dict[str, int] = {
    "wp_created": 3,
    "wp_closed": 2,
    "wp_completed": 2,
    "strategy_session_completed": 5,
}

# ── Типы событий саморазвития ─────────────────────────────────────────────────

SELF_DEV_EVENT_TYPES: list[str] = [
    "lesson_completed",
    "knowledge_extracted",
    "pack_updated",
    "qualification_granted",
    "day_plan_opened", "day_open",      # legacy aliases
    "day_plan_closed", "day_close",     # legacy aliases
    "week_plan_closed",
    "month_plan_closed",
    "strategy_session_completed",
    "iwe_session",
]

# З4 (WP-310): окно расчёта bh.awr (worldview score) — 30 дней.
AWR_WINDOW_DAYS: int = 30

# ── Вспомогательные таблицы ───────────────────────────────────────────────────

TRIGGERED_BY: dict[int, str] = {
    1: "manual_calibration",
    2: "SR.001",
    3: "SR.002",
    4: "SR.003",
    5: "SR.004",
}

TARGET_HOURS_PER_STAGE: dict[int, int] = {1: 2, 2: 4, 3: 6, 4: 8, 5: 10}

STAGE_NAMES_RU: dict[int, str] = {
    1: "Случайный",
    2: "Практикующий",
    3: "Систематический",
    4: "Дисциплинированный",
    5: "Проактивный",
}

STAGE_IDS: dict[int, str] = {
    1: "STG.Student.Random",
    2: "STG.Student.Practicing",
    3: "STG.Student.Systematic",
    4: "STG.Student.Disciplined",
    5: "STG.Student.Proactive",
}

# ── BH-измерения для UI (симул��тор WP-319 Ф5) ────────────────────────────────
# Каждый элемент: (код, метка, мин, макс, шаг, default)
BH_DIMENSIONS: list[tuple[str, str, float, float, float, float]] = [
    ("hours_per_week",  "Часов в неделю",      0.5, 20.0, 0.5, 4.0),
    ("days_per_week",   "Дней в неделю",        1.0,  7.0, 0.5, 3.0),
    ("max_gap_days",    "Макс. перерыв (дней)", 1.0, 30.0, 1.0, 7.0),
    ("m_delta_per_week","Рост уроков / нед",   -0.2,  0.3, 0.05, 0.0),
    ("w_delta_per_week","Рост закрытий / нед", -0.2,  0.3, 0.05, 0.0),
    ("a_delta_per_week","Рост РП / нед",       -0.2,  0.3, 0.05, 0.0),
]
