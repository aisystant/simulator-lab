"""S3 — Когортная динамика — WP-319 Ф3.

# see DP.SC.133, DP.ROLE.043

Сценарий: N профилей (пресеты ст.1-5 или реальные) + нормы →
распределение ступеней через K недель.

Аналогия: fleet test — как разные «типы автомобилей» ведут себя под одной нагрузкой.
Полезно для команды: понять расслоение когорты wave-1 через 4/8/12 нед.
"""
from __future__ import annotations

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[4]))

from collections import Counter

from activity_hub.engines.simulator.base import (
    Scenario, ScenarioResult, SimulatorProfile,
)
from activity_hub.engines.simulator.data import make_preset_profile
from activity_hub.engines.simulator.scenarios.s1_stage_trajectory import S1StageTrajectory
from activity_hub.core.stage_config import STAGE_NAMES_RU, CONFIG_VERSION

# ── Типовые паттерны поведения для когортных сценариев ──────────────────────

# Базовые паттерны: hours_per_week × days_per_week × max_gap_days
COHORT_PATTERNS: dict[str, dict] = {
    "minimal":   {"hours_per_week": 2.0,  "days_per_week": 2.0,  "max_gap_days": 14},
    "moderate":  {"hours_per_week": 4.0,  "days_per_week": 3.0,  "max_gap_days": 7},
    "active":    {"hours_per_week": 6.0,  "days_per_week": 5.0,  "max_gap_days": 4},
    "intensive": {"hours_per_week": 10.0, "days_per_week": 6.0,  "max_gap_days": 2},
    "random":    {"hours_per_week": 1.0,  "days_per_week": 1.0,  "max_gap_days": 20},
}


def _make_wave1_cohort() -> tuple[list[SimulatorProfile], list[tuple[str, dict]]]:
    """7 типовых профилей для wave-1 (все начинают на ст.1 — К4 fix)."""
    base = make_preset_profile(1)
    base.account_id = "wave1_preset"
    cohort = []
    # Разные паттерны поведения волны
    patterns = [
        ("wave1_minimal_1",  COHORT_PATTERNS["minimal"]),
        ("wave1_minimal_2",  COHORT_PATTERNS["minimal"]),
        ("wave1_random_1",   COHORT_PATTERNS["random"]),
        ("wave1_moderate_1", COHORT_PATTERNS["moderate"]),
        ("wave1_moderate_2", COHORT_PATTERNS["moderate"]),
        ("wave1_active_1",   COHORT_PATTERNS["active"]),
        ("wave1_intensive_1", COHORT_PATTERNS["intensive"]),
    ]
    for pid, _ in patterns:
        p = SimulatorProfile(
            account_id=pid,
            s=1, t=1, m=1, w=0, a=0, stb=1,
            hours_per_week=1.0, days_per_week=1.0, total_hours=5.0,
            source="preset",
        )
        cohort.append(p)
    return cohort, patterns


class S3CohortDynamics(Scenario):
    """S3: Динамика распределения ступеней в когорте через K недель.

    Вход: список профилей + паттерны поведения (override на каждый профиль)
    Выход: week × {stage_N_count, mean_bh_inv, dropout_rate}
    """

    scenario_id = "s3"
    name = "Когортная динамика"
    description = "Как распределяется когорта по ступеням через несколько недель"

    def run(
        self,
        profile: SimulatorProfile,
        params: dict,
        horizon_weeks: int = 12,
    ) -> ScenarioResult:
        # Построить когорту
        cohort_type = params.get("cohort_type", "preset_all_stages")

        if cohort_type == "wave1":
            profiles, patterns = _make_wave1_cohort()
        elif cohort_type == "preset_all_stages":
            # 5 пресетных профилей (ст.1-5), каждый со своим паттерном
            profiles = [make_preset_profile(s) for s in range(1, 6)]
            patterns = [(f"preset_stage_{s}", COHORT_PATTERNS["active"]) for s in range(1, 6)]
        else:
            # Single profile from params (для тестирования)
            profiles = [profile]
            patterns = [(profile.account_id, params)]

        s1 = S1StageTrajectory()
        # Хранилище: week → Counter ступеней
        week_stages: dict[int, Counter] = {w: Counter() for w in range(horizon_weeks + 1)}
        week_bh_inv: dict[int, list[float]] = {w: [] for w in range(horizon_weeks + 1)}

        for p, (pid, pattern) in zip(profiles, patterns):
            run_params = {**pattern}  # копия чтобы не мутировать
            result = s1.run(p, run_params, horizon_weeks)
            for row in result.rows:
                week_stages[row.week][row.stage] += 1
                week_bh_inv[row.week].append(float(row.bh_inv))

        n = len(profiles)
        rows: list[dict] = []
        for week in range(horizon_weeks + 1):
            cnt = week_stages[week]
            mean_inv = sum(week_bh_inv[week]) / len(week_bh_inv[week]) if week_bh_inv[week] else 0.0
            rows.append({
                "week": week,
                "total": n,
                "stage_1_count": cnt.get(1, 0),
                "stage_2_count": cnt.get(2, 0),
                "stage_3_count": cnt.get(3, 0),
                "stage_4_count": cnt.get(4, 0),
                "stage_5_count": cnt.get(5, 0),
                "stage_1_pct": round(cnt.get(1, 0) / n * 100) if n > 0 else 0,
                "stage_2_pct": round(cnt.get(2, 0) / n * 100) if n > 0 else 0,
                "stage_3_pct": round(cnt.get(3, 0) / n * 100) if n > 0 else 0,
                "stage_4_pct": round(cnt.get(4, 0) / n * 100) if n > 0 else 0,
                "stage_5_pct": round(cnt.get(5, 0) / n * 100) if n > 0 else 0,
                "mean_bh_inv": round(mean_inv, 2),
            })

        pilot_text, recommendation = self._make_pilot_text(rows, n, cohort_type)

        return ScenarioResult(
            scenario_id=self.scenario_id,
            rows=[],
            rows_dicts=rows,
            pilot_text=pilot_text,
            recommendation=recommendation,
            config_version=CONFIG_VERSION,
        )

    def _make_pilot_text(
        self,
        rows: list[dict],
        total: int,
        cohort_type: str,
    ) -> tuple[str, str]:
        if not rows or total == 0:
            return "", ""

        last = rows[-1]
        weeks = last["week"]

        # Подсчёт тех, кто поднялся выше ст.1
        progressed = total - last.get("stage_1_count", total)
        pct_progressed = round(progressed / total * 100)

        cohort_label = {
            "wave1": "первая когорта (7 участников)",
            "preset_all_stages": "5 типовых профилей",
        }.get(cohort_type, f"{total} участников")

        text = (
            f"Через {weeks} недель из {cohort_label}: "
            f"{pct_progressed}% участников перешли выше ступени «Случайный».\n\n"
        )
        # Детализация по ступеням
        for stage in range(1, 6):
            cnt = last.get(f"stage_{stage}_count", 0)
            if cnt > 0:
                name = STAGE_NAMES_RU.get(stage, f"Ступень {stage}")
                text += f"• «{name}»: {cnt} чел. ({round(cnt/total*100)}%)\n"

        rec = (
            "Участники с минимальным ритмом (1-2 ч/нед) остаются на ступени «Случайный» "
            "дольше всего. Ключевой рычаг — довести хотя бы 1 учебный час в неделю "
            "до стабильных 4 часов."
        )

        return text.strip(), rec
