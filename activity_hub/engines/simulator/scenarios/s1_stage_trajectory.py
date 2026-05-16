"""S1 — Траектория ступени — WP-319 Ф3.

# see DP.SC.133, DP.ROLE.043

Сценарий краш-теста: задаём паттерн поведения (hours/week, days/week) →
смотрим как меняется ступень и bh-характеристики неделю за неделей.

Аналогия: frontal impact test — что сломается первым, когда
нагрузка меняется.
"""
from __future__ import annotations

import sys
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[4]))

from activity_hub.engines.simulator.base import (
    Scenario, ScenarioResult, ScenarioRow, SimulatorProfile,
)
from activity_hub.core.stage_config import (
    STAGE_GATE_MATRIX, STAGE_NAMES_RU, STB_GATE,
    CUMULATIVE_HOURS_GATE, CONFIG_VERSION,
    norm_m, norm_w, norm_a, norm_stb,
    compute_stage_mvp,
    S_THRESHOLDS, T_THRESHOLDS,
)

# ── Pilot-mode эталонные тексты (DP.SC.133 инвариант: без кодов) ─────────────

_STAGE_NAME = STAGE_NAMES_RU

_BOTTLENECK_TEXT: dict[str, str] = {
    "t0": (
        "Главная причина: бот не видит ваших учебных часов — "
        "за последние недели не записано ни одного учебного времени.\n\n"
        "Записать учебное время можно командой /slot 60 после каждой сессии "
        "(60 = минут). Или один раз в конце дня."
    ),
    "s": "Главная причина: нужно заниматься чаще — увеличить количество дней в неделю.",
    "t": "Главная причина: нужно больше учебного времени в неделю.",
    "m": "Главная причина: нужно проходить больше уроков и делать knowledge extraction.",
    "w": "Главная причина: нужно регулярно закрывать недели и проводить стратегические сессии.",
    "a": "Главная причина: нужно создавать и закрывать рабочие продукты (практические проекты).",
    "stb": "Главная причина: слишком большие перерывы — бот видит длинные паузы без активности.",
    "hours": "Главная причина: ещё не накоплено достаточно учебных часов для следующего уровня.",
}

_NEXT_STEP_TEXT: dict[str, str] = {
    "t0": "Начните записывать учебное время командой /slot N мин.",
    "s": "Занимайтесь на {target} дней в неделю вместо {current}.",
    "t": "Выделяйте {target} часов в неделю вместо {current:.1f}.",
    "m": "Проходите больше уроков или делайте извлечение знаний.",
    "w": "Закрывайте каждую неделю итоговым обзором.",
    "a": "Заведите и завершите хотя бы один практический проект.",
    "stb": "Не пропускайте больше {target} дней подряд — ставьте напоминание.",
    "hours": "Накопите ещё {gap:.0f} часов учебного времени.",
}


def _s_from_dpw(days_per_week: float) -> int:
    for idx, _wk, threshold in S_THRESHOLDS:
        if days_per_week >= threshold:
            return idx
    return 0


def _t_from_hpw(hours_per_week: float) -> int:
    for idx, _wk, threshold in T_THRESHOLDS:
        if hours_per_week >= threshold:
            return idx
    return 0


def _bottleneck(s, t, m, w, a, stb, total_hours, stage) -> str:
    """Найти главное ограничение для перехода на следующую ступень."""
    next_stage = stage + 1
    if next_stage > 5:
        return ""
    gate = STAGE_GATE_MATRIX.get(next_stage, (5, 5, 5, 5, 5))
    gate_dict = {"s": gate[0], "t": gate[1], "m": gate[2], "w": gate[3], "a": gate[4],
                 "stb": STB_GATE.get(next_stage, 0)}
    current = {"s": s, "t": t, "m": m, "w": w, "a": a, "stb": stb}

    if t == 0:
        return "t0"

    distances = [(max(0, gate_dict[k] - current[k]), k) for k in gate_dict]
    hours_gate = CUMULATIVE_HOURS_GATE.get(next_stage, 0)
    if total_hours < hours_gate:
        distances.append((2, "hours"))

    max_d = max(d for d, _ in distances) if distances else 0
    if max_d == 0:
        return ""
    return max(distances, key=lambda x: x[0])[1]


class S1StageTrajectory(Scenario):
    """S1: Траектория ступени при заданном паттерне поведения.

    Вход: профиль пилота + {hours_per_week?, days_per_week?, max_gap_days?}
    Выход: неделя × {stage, bh.*, total_hours, bottleneck}
    """

    scenario_id = "s1"
    name = "Траектория ступени"
    description = "Как изменится ваша ступень через 12 недель при заданном ритме занятий"

    def run(
        self,
        profile: SimulatorProfile,
        params: dict,
        horizon_weeks: int = 12,
    ) -> ScenarioResult:
        # Параметры сценария (override или берём из профиля)
        hpw = float(params.get("hours_per_week", profile.hours_per_week or 1.0))
        dpw = float(params.get("days_per_week",  profile.days_per_week  or 1.0))
        gap = int(params.get("max_gap_days",     profile.max_gap_days   or 30))

        # Начальные значения (из профиля, не пересчитываем по новым params для недели 0)
        s   = profile.s
        t   = profile.t
        m   = profile.m
        w   = profile.w
        a   = profile.a
        stb = profile.stb
        total_hours = profile.total_hours

        # Через N недель при новом паттерне bh-индексы станут другими
        new_s   = _s_from_dpw(dpw)
        new_t   = _t_from_hpw(hpw)
        new_stb = norm_stb(gap)

        rows: list[ScenarioRow] = []

        for week in range(0, horizon_weeks + 1):
            # Линейная интерполяция: индексы меняются постепенно
            # (имитируем multi-window: через 4 нед полный эффект нового ритма)
            ramp = min(1.0, week / 4.0)
            cur_s   = round(s   + (new_s   - s)   * ramp)
            cur_t   = round(t   + (new_t   - t)   * ramp)
            cur_stb = round(stb + (new_stb - stb) * ramp)

            # m, w, a растут медленнее (требуют событий: уроки, закрытия, РП)
            m_delta = params.get("m_delta_per_week", 0)
            w_delta = params.get("w_delta_per_week", 0)
            a_delta = params.get("a_delta_per_week", 0)
            cur_m = min(5, m + round(m_delta * week))
            cur_w = min(5, w + round(w_delta * week))
            cur_a = min(5, a + round(a_delta * week))

            total_hours += hpw if week > 0 else 0
            stage = compute_stage_mvp(cur_s, cur_t, cur_m, cur_w, cur_a,
                                       total_hours=total_hours, stb=cur_stb)
            bn = _bottleneck(cur_s, cur_t, cur_m, cur_w, cur_a, cur_stb, total_hours, stage)

            rows.append(ScenarioRow(
                week=week,
                stage=stage,
                bh_sys=cur_s, bh_inv=cur_t, bh_met=cur_m,
                bh_awr=cur_w, bh_agn=cur_a, bh_stb=cur_stb,
                total_hours=total_hours,
                bottleneck=bn,
            ))

        # Финальный bottleneck (на неделю 0 = текущий)
        final_bn = rows[0].bottleneck if rows else ""
        pilot_text, recommendation = self._make_pilot_text(
            profile, rows, hpw, dpw, final_bn,
        )

        return ScenarioResult(
            scenario_id=self.scenario_id,
            rows=rows,
            bottleneck_key=final_bn,
            bottleneck_label=_BOTTLENECK_TEXT.get(final_bn, ""),
            pilot_text=pilot_text,
            recommendation=recommendation,
            config_version=CONFIG_VERSION,
        )

    def _make_pilot_text(
        self,
        profile: SimulatorProfile,
        rows: list[ScenarioRow],
        hpw: float,
        dpw: float,
        bottleneck: str,
    ) -> tuple[str, str]:
        """Сформировать Pilot-mode текст по эталону SC.133 (без кодов)."""
        if not rows:
            return "", ""

        current = rows[0]
        final   = rows[-1]
        stage_name = _STAGE_NAME.get(current.stage, f"Ступень {current.stage}")

        # Когда произойдёт переход (первая неделя с stage > current)
        transition_week = None
        for r in rows[1:]:
            if r.stage > current.stage:
                transition_week = r.week
                break

        bn_text = _BOTTLENECK_TEXT.get(bottleneck, "")

        if bottleneck == "t0":
            text = (
                f"Сейчас вы на ступени «{stage_name}». {bn_text}"
            )
            rec = "Записать учебное время можно командой /slot 60 после каждой сессии (60 = минут)."
        elif transition_week:
            next_name = _STAGE_NAME.get(current.stage + 1, "следующую ступень")
            text = (
                f"Сейчас вы на ступени «{stage_name}». {bn_text}\n\n"
                f"Если в течение {transition_week} недель заниматься по {hpw:.0f} часа "
                f"в неделю — перейдёте на ступень «{next_name}»."
            )
            rec = _NEXT_STEP_TEXT.get(bottleneck, "Продолжайте в том же ритме.").format(
                target=round(hpw), current=profile.hours_per_week or hpw,
                gap=max(0, CUMULATIVE_HOURS_GATE.get(current.stage + 1, 0) - current.total_hours),
            )
        else:
            text = (
                f"Сейчас вы на ступени «{stage_name}». {bn_text}\n\n"
                f"При текущем ритме ({hpw:.0f} ч/нед) переход займёт больше "
                f"{len(rows) - 1} недель."
            )
            rec = _NEXT_STEP_TEXT.get(bottleneck, "Увеличьте учебное время.").format(
                target=round(hpw + 1), current=profile.hours_per_week or hpw,
                gap=max(0, CUMULATIVE_HOURS_GATE.get(current.stage + 1, 0) - current.total_hours),
            )

        return text, rec
