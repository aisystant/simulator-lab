"""S2 — Траектория баллов — WP-319 Ф3.

# see DP.SC.133, DP.ROLE.043

Сценарий: задаём частоту событий (уроков, коммитов, РП) и ступень пилота →
смотрим как накапливается баланс и какой тип активности даёт больше всего баллов.

Аналогия: endurance test — сколько «энергии» накапливает система при разных режимах нагрузки.

Логика расчёта = Python-аналог compute_effective_amount() (WP-121 Ф2 v2).
Константы: core/reward_config.py (SoT). Не дублировать здесь.
"""
from __future__ import annotations

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[4]))

from activity_hub.engines.simulator.base import (
    Scenario, ScenarioResult, SimulatorProfile,
)
from activity_hub.core.stage_config import STAGE_NAMES_RU, CONFIG_VERSION
from activity_hub.core.reward_config import (
    BASE_AMOUNTS, EVENT_DOMAIN, DOMAIN_MULT, DOMAIN_DAILY_CAP,
    STAGE_MULT, STAGE_DAILY_CAP, STREAK_ELIGIBLE,
    REWARD_CONFIG_VERSION,
)

# ── Человекочитаемые названия событий (Pilot-mode) ────────────────────────────

_EVENT_LABELS: dict[str, str] = {
    "lesson_completed":     "уроки LMS",
    "knowledge_extracted":  "извлечение знаний",
    "distinction_added":    "добавление различений",
    "wp_completed":         "закрытые РП",
    "commit_created":       "коммиты",
    "day_close":            "закрытия дня",
    "slot_logged":          "записи учебного времени",
    "strategy_session_completed": "стратегические сессии",
    "content_published":    "опубликованный контент",
}


def _streak_mult(day_closes_per_week: float) -> float:
    """Аналог _compute_streak_mult(): дней с закрытием за 7 дней → множитель 1.0–1.5."""
    days = min(7, round(day_closes_per_week))
    return min(1.5, 1.0 + (days / 7.0) * 0.5)


def _effective_per_event(event_type: str, stage: int, streak: float) -> float:
    """Эффективное начисление за одно событие (до применения daily cap)."""
    base = BASE_AMOUNTS.get(event_type, 0.0)
    if base == 0.0:
        return 0.0
    domain = EVENT_DOMAIN.get(event_type, "work")
    dom = DOMAIN_MULT.get(domain, 1.0)
    qual = STAGE_MULT.get(stage, 1.0)
    s = streak if event_type in STREAK_ELIGIBLE else 1.0
    return base * dom * qual * s


def _simulate_week(
    events_per_week: dict[str, float],
    stage: int,
    day_closes_per_week: float,
    active_days: float,
) -> tuple[float, str]:
    """Симулировать одну неделю. Возвращает (weekly_delta, top_event_type)."""
    streak = _streak_mult(day_closes_per_week)
    stage_cap = STAGE_DAILY_CAP.get(stage, 50.0)

    gross_per_type: dict[str, float] = {}
    for et, freq in events_per_week.items():
        if freq <= 0:
            continue
        per_event = _effective_per_event(et, stage, streak)
        gross_per_type[et] = per_event * freq

    if not gross_per_type:
        return 0.0, ""

    # Применяем cap по domain-группам (приближение: active_days × daily_cap)
    by_domain: dict[str, list[str]] = {}
    for et in gross_per_type:
        d = EVENT_DOMAIN.get(et, "work")
        by_domain.setdefault(d, []).append(et)

    for domain, ets in by_domain.items():
        dom_cap = DOMAIN_DAILY_CAP.get(domain, 50.0)
        effective_weekly_cap = min(dom_cap, stage_cap) * active_days
        domain_gross = sum(gross_per_type[et] for et in ets)
        if domain_gross > 0:
            ratio = min(1.0, effective_weekly_cap / domain_gross)
            for et in ets:
                gross_per_type[et] *= ratio

    weekly_delta = sum(gross_per_type.values())
    top = max(gross_per_type, key=lambda x: gross_per_type[x]) if gross_per_type else ""
    return weekly_delta, top


class S2RewardsTrajectory(Scenario):
    """S2: Траектория баллов при заданном паттерне активности.

    Вход: профиль пилота + частоты событий по типам в неделю
    Выход: week × {balance, weekly_delta, monthly_delta, top_event_type}
    """

    scenario_id = "s2"
    name = "Траектория баллов"
    description = "Как накапливается баланс при разных видах активности"

    def run(
        self,
        profile: SimulatorProfile,
        params: dict,
        horizon_weeks: int = 12,
    ) -> ScenarioResult:
        events_pw = {
            "lesson_completed":     float(params.get("lesson_completed_per_week", 5.0)),
            "knowledge_extracted":  float(params.get("knowledge_extracted_per_week", 0.0)),
            "wp_completed":         float(params.get("wp_completed_per_week", 0.0)),
            "commit_created":       float(params.get("commit_created_per_week", 10.0)),
            "day_close":            float(params.get("day_close_per_week", 0.0)),
            "slot_logged":          float(params.get("slot_logged_per_week", 0.0)),
            "note_to_capture":      float(params.get("note_to_capture_per_week", 0.0)),
            "strategy_session_completed": float(params.get("strategy_session_per_week", 0.0)),
            "content_published":    float(params.get("content_published_per_week", 0.0)),
        }
        # Convenience: per-month → per-week
        for month_key, wk_key, factor in [
            ("wp_completed_per_month",       "wp_completed",              1.0 / 4.0),
            ("strategy_session_per_month",   "strategy_session_completed", 1.0 / 4.0),
        ]:
            if month_key in params:
                events_pw[wk_key] = float(params[month_key]) * factor

        stage = max(1, min(5, profile.s or 1))
        day_closes_pw = float(params.get("day_close_per_week", profile.days_per_week or 1.0))
        active_days = float(params.get("active_days_per_week", profile.days_per_week or 5.0))

        balance = 0.0
        weekly_deltas: list[float] = [0.0]
        rows: list[dict] = []

        for week in range(0, horizon_weeks + 1):
            if week == 0:
                rows.append({
                    "week": 0,
                    "balance": balance,
                    "weekly_delta": 0.0,
                    "monthly_delta": 0.0,
                    "top_event_type": "",
                    "stage": stage,
                })
                continue

            weekly_delta, top = _simulate_week(events_pw, stage, day_closes_pw, active_days)
            balance += weekly_delta
            weekly_deltas.append(weekly_delta)

            # Скользящий месячный темп: сумма последних 4 недель
            window = weekly_deltas[max(0, week - 3): week + 1]
            monthly_rate = sum(window)

            rows.append({
                "week": week,
                "balance": round(balance, 1),
                "weekly_delta": round(weekly_delta, 1),
                "monthly_delta": round(monthly_rate, 1),
                "top_event_type": top,
                "stage": stage,
            })

        pilot_text, recommendation = self._make_pilot_text(profile, rows, events_pw, stage)

        return ScenarioResult(
            scenario_id=self.scenario_id,
            rows=[],
            rows_dicts=rows,
            pilot_text=pilot_text,
            recommendation=recommendation,
            config_version=f"{CONFIG_VERSION}/{REWARD_CONFIG_VERSION}",
        )

    def _make_pilot_text(
        self,
        profile: SimulatorProfile,
        rows: list[dict],
        events_pw: dict[str, float],
        stage: int,
    ) -> tuple[str, str]:
        if len(rows) < 2:
            return "", ""

        final_balance = rows[-1]["balance"]
        monthly_delta = sum(r["weekly_delta"] for r in rows[1:5])

        stage_name = STAGE_NAMES_RU.get(stage, f"Ступень {stage}")

        top_sources: dict[str, float] = {}
        for r in rows[1:]:
            et = r["top_event_type"]
            if et:
                top_sources[et] = top_sources.get(et, 0) + 1
        top_et = max(top_sources, key=lambda x: top_sources[x]) if top_sources else ""
        top_label = _EVENT_LABELS.get(top_et, top_et) if top_et else "разные действия"

        text = (
            f"При текущем ритме занятий вы будете зарабатывать около "
            f"{monthly_delta:.0f} баллов в месяц.\n\n"
            f"Через {len(rows) - 1} недель ваш баланс составит "
            f"примерно {final_balance:.0f} баллов.\n\n"
            f"Больше всего баллов будет приносить: {top_label}."
        )

        ke_ratio = BASE_AMOUNTS.get("knowledge_extracted", 35.0) / max(BASE_AMOUNTS.get("lesson_completed", 10.0), 1)
        rec = (
            f"Чтобы зарабатывать больше баллов, на ступени «{stage_name}» "
            f"выгоднее всего увеличить частоту извлечения знаний: "
            f"каждое извлечение даёт в {ke_ratio:.0f} раза больше, чем урок."
        )

        return text, rec
