"""Лаборатория симуляций — Streamlit UI — WP-319 Ф5.

# see DP.SC.133, DP.ROLE.043

Два режима:
- Пилот: текущая ступень + bottleneck + рекомендация + 1 timeline-график + текстовый ввод
- Эксперт: N слайдеров + S1/S2/S3 выбор + multi-line график
"""
from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

import plotly.graph_objects as go
import streamlit as st

# ── Engine path ────────────────────────────────────────────────────────────────
# Ищем activity-hub рядом: ../activity-hub или через ACTIVITY_HUB_PATH
_HUB_CANDIDATES = [
    os.environ.get("ACTIVITY_HUB_PATH", ""),
    os.path.join(os.path.dirname(__file__), "..", "activity-hub"),
    os.path.join(os.path.dirname(__file__), "activity-hub"),
]
for _p in _HUB_CANDIDATES:
    if _p and os.path.isdir(os.path.join(_p, "activity_hub")):
        sys.path.insert(0, os.path.abspath(_p))
        break

from activity_hub.core.stage_config import (
    BH_DIMENSIONS, STAGE_NAMES_RU, CONFIG_VERSION, CONFIG_DATE,
    S_THRESHOLDS, T_THRESHOLDS, CUMULATIVE_HOURS_GATE, STAGE_GATE_MATRIX, STB_GATE,
)
from activity_hub.engines.simulator.base import SimulatorProfile
from activity_hub.engines.simulator.data import make_preset_profile, _s_from_dpw, _t_from_hpw
from activity_hub.engines.simulator.scenarios import ALL_SCENARIOS
from activity_hub.engines.simulator.scenarios.s1_stage_trajectory import _bottleneck

# ── Константы UI ──────────────────────────────────────────────────────────────

STAGE_COLORS = {1: "#6B7280", 2: "#3B82F6", 3: "#10B981", 4: "#8B5CF6", 5: "#F59E0B"}
HORIZON_OPTIONS = [4, 8, 12, 26, 52]
HORIZON_LABELS = ["4 нед", "8 нед", "12 нед", "полгода", "год"]

_BOTTLENECK_LABELS = {
    "t0": "Бот не видит ваших учебных часов — нет записей о времени занятий",
    "s": "Нужно заниматься чаще — увеличить количество дней в неделю",
    "t": "Нужно больше учебного времени в неделю",
    "m": "Нужно проходить больше уроков и делать извлечение знаний",
    "w": "Нужно регулярно закрывать недели и проводить стратегические сессии",
    "a": "Нужно создавать и закрывать рабочие продукты",
    "stb": "Слишком большие перерывы — нужно сократить паузы без занятий",
    "hours": "Ещё не накоплено достаточно учебных часов для следующего уровня",
    "": "Всё хорошо — продолжайте в том же ритме",
}

_NEXT_STEP_LABELS = {
    "t0": "Записывайте учебное время командой /slot 60 (60 = минут) после каждой сессии.",
    "s": "Занимайтесь как минимум {target} дней в неделю.",
    "t": "Выделяйте не менее {target} часов в неделю.",
    "m": "Проходите больше уроков или делайте извлечение знаний.",
    "w": "Закрывайте каждую неделю итоговым обзором.",
    "a": "Заведите и завершите хотя бы один практический проект.",
    "stb": "Не пропускайте более 7 дней подряд.",
    "hours": "Накопите ещё учебных часов — продолжайте в текущем ритме.",
    "": "Продолжайте в том же ритме.",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _run(coro):
    return asyncio.run(coro)


@st.cache_data(ttl=300, show_spinner=False)
def _load_profile_cached(account_id: str) -> dict:
    """Загрузить bh-профиль из Neon (кеш 5 мин)."""
    dsn = os.environ.get("NEON_SIM_RO_DSN", "")
    if not dsn:
        return {}

    async def _fetch():
        import asyncpg
        conn = await asyncpg.connect(dsn)
        try:
            from activity_hub.engines.simulator.data import load_profile
            profile = await load_profile(account_id, conn)
            return {
                "s": profile.s, "t": profile.t, "m": profile.m,
                "w": profile.w, "a": profile.a, "stb": profile.stb,
                "hours_per_week": profile.hours_per_week,
                "days_per_week": profile.days_per_week,
                "total_hours": profile.total_hours,
                "source": profile.source,
            }
        finally:
            await conn.close()

    try:
        return _run(_fetch())
    except Exception:
        return {}


def _profile_from_dict(d: dict, account_id: str = "") -> SimulatorProfile:
    return SimulatorProfile(
        account_id=account_id,
        s=d.get("s", 0), t=d.get("t", 0), m=d.get("m", 0),
        w=d.get("w", 0), a=d.get("a", 0), stb=d.get("stb", 0),
        hours_per_week=d.get("hours_per_week", 0.0),
        days_per_week=d.get("days_per_week", 0.0),
        total_hours=d.get("total_hours", 0.0),
        source=d.get("source", "preset"),
    )


def _parse_text_input(text: str) -> dict[str, Any] | None:
    """Вызвать LLM-парсер, вернуть результат или None при ошибке."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        st.warning("ANTHROPIC_API_KEY не задан — текстовый ввод недоступен.")
        return None
    try:
        from activity_hub.engines.simulator.llm_parser import parse_scenario_text
        return _run(parse_scenario_text(text))
    except Exception as e:
        st.error(f"Ошибка парсера: {e}")
        return None


# ── Plotly charts ──────────────────────────────────────────────────────────────

def stage_step_chart(rows: list[dict], title: str = "") -> go.Figure:
    """График ступени: step-function по неделям."""
    weeks = [r["week"] for r in rows]
    stages = [r["stage"] for r in rows]
    colors = [STAGE_COLORS.get(s, "#6B7280") for s in stages]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=weeks, y=stages,
        mode="lines+markers",
        line=dict(shape="hv", width=3, color="#3B82F6"),
        marker=dict(size=8, color=colors, line=dict(width=1, color="white")),
        name="Ступень",
        hovertemplate="Неделя %{x}: ступень %{y}<extra></extra>",
    ))
    fig.update_layout(
        title=title or "Траектория ступени",
        xaxis_title="Неделя",
        yaxis_title="Ступень",
        yaxis=dict(range=[0.5, 5.5], tickvals=[1, 2, 3, 4, 5],
                   ticktext=[STAGE_NAMES_RU[i] for i in range(1, 6)]),
        height=320,
        margin=dict(l=20, r=20, t=40, b=20),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def bh_multiline_chart(rows: list[dict]) -> go.Figure:
    """График всех bh-характеристик × время."""
    weeks = [r["week"] for r in rows]
    bh_keys = [
        ("bh.sys", "Систематичность", "#EF4444"),
        ("bh.inv", "Инв. время",      "#3B82F6"),
        ("bh.met", "Методичность",    "#10B981"),
        ("bh.awr", "Осведомлённость", "#8B5CF6"),
        ("bh.agn", "Агентность",      "#F59E0B"),
        ("bh.stb", "Устойчивость",    "#6B7280"),
    ]
    fig = go.Figure()
    for key, label, color in bh_keys:
        vals = [r.get(key, 0) for r in rows]
        fig.add_trace(go.Scatter(
            x=weeks, y=vals,
            mode="lines", name=label,
            line=dict(width=2, color=color),
            hovertemplate=f"{label} нед %{{x}}: %{{y}}<extra></extra>",
        ))
    fig.update_layout(
        title="Характеристики по неделям",
        xaxis_title="Неделя", yaxis_title="Уровень",
        yaxis=dict(range=[-0.2, 5.5]),
        height=360,
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", y=-0.25),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def cohort_bars_chart(rows: list[dict]) -> go.Figure:
    """Stacked bar по ступеням для S3 (когортная динамика)."""
    if not rows:
        return go.Figure()
    weeks = [r["week"] for r in rows]
    stage_keys = [k for k in rows[0].keys() if k.startswith("stage_") and k.endswith("_pct")]
    fig = go.Figure()
    for key in sorted(stage_keys):
        stage_num = key.replace("stage_", "").replace("_pct", "")
        try:
            n = int(stage_num)
        except ValueError:
            continue
        vals = [r.get(key, 0) for r in rows]
        fig.add_trace(go.Bar(
            x=weeks, y=vals, name=STAGE_NAMES_RU.get(n, f"Ст.{n}"),
            marker_color=STAGE_COLORS.get(n, "#6B7280"),
        ))
    fig.update_layout(
        barmode="stack", title="Распр��деление ступеней в когорте",
        xaxis_title="Неделя", yaxis_title="% участников",
        height=360, margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", y=-0.25),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


# ── Pilot Mode ─────────────────────────────────────────────────────────────────

def render_pilot_mode(profile: SimulatorProfile):
    stage = max(1, profile.s if profile.source == "real" else (profile.t or 1))
    from activity_hub.core.stage_config import compute_stage_mvp
    stage = compute_stage_mvp(
        profile.s, profile.t, profile.m, profile.w, profile.a,
        total_hours=profile.total_hours, stb=profile.stb,
    )
    stage_name = STAGE_NAMES_RU.get(stage, f"Ступень {stage}")
    bn = _bottleneck(profile.s, profile.t, profile.m, profile.w, profile.a,
                     profile.stb, profile.total_hours, stage)

    # Карточка текущего состояния
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Ст��пень", stage_name)
    with col2:
        st.metric("Часов накоплено", f"{profile.total_hours:.0f} ч")
    with col3:
        hpw = profile.hours_per_week or 0
        st.metric("Ритм", f"{hpw:.1f} ч/нед" if hpw else "нет данных")

    bn_label = _BOTTLENECK_LABELS.get(bn, "")
    if bn_label:
        st.warning(f"**Узкое место:** {bn_label}")

    # Рекомендация
    next_step = _NEXT_STEP_LABELS.get(bn, "")
    if next_step:
        hpw_target = 0
        for idx, _wk, threshold in T_THRESHOLDS:
            if idx == stage + 1:
                hpw_target = threshold
                break
        dpw_target = 0
        for idx, _wk, threshold in S_THRESHOLDS:
            if idx == stage + 1:
                dpw_target = threshold
                break
        try:
            next_step = next_step.format(
                target=hpw_target or dpw_target or "больше",
                current=profile.hours_per_week or 0,
            )
        except (KeyError, ValueError):
            pass
        st.info(f"**Что делать:** {next_step}")

    # Базовая симуляция S1 с текущим профилем
    st.subheader("Что будет через 12 недель при текущем ритме")
    scenario = ALL_SCENARIOS["s1"]
    result = scenario.run(profile, {}, horizon_weeks=12)
    st.plotly_chart(stage_step_chart(result.as_dicts()), use_container_width=True)

    # What-if текстовый ввод
    st.divider()
    st.subheader("Попробуйте «что если»")
    text_input = st.text_input(
        "Напишите свой сценарий",
        placeholder='Например: "начну учиться 6 часов в неделю" или "буду закрывать неделю"',
        key="pilot_text_input",
    )
    if text_input:
        with st.spinner("Анализирую..."):
            parsed = _parse_text_input(text_input)

        if parsed:
            if parsed["fallback_sliders"]:
                st.caption(f"Не смог точно распознать параметры (уверенность {parsed['confidence']:.0%}). "
                           "Попробуйте переключиться в режим Эксперт для ручной настройки.")
            else:
                st.caption(f"Распознано: {parsed['explanation']}")
                overrides = parsed["bh_overrides"]
                horizon = parsed["horizon_weeks"]
                what_if_result = scenario.run(profile, overrides, horizon_weeks=horizon)
                label = f"Что будет через {horizon} нед при новом ритме"
                st.plotly_chart(stage_step_chart(what_if_result.as_dicts(), title=label),
                                use_container_width=True)
                final_stage = what_if_result.as_dicts()[-1]["stage"]
                if final_stage > stage:
                    st.success(f"При таком ритме через {horizon} нед вы перейдёте на ступень "
                               f"«{STAGE_NAMES_RU.get(final_stage, final_stage)}»!")
                elif final_stage == stage:
                    st.info(f"Ступень останется «{stage_name}» — нужно ещё поработать над узким местом.")
                else:
                    st.error(f"При таком ритме ступень может упасть до «{STAGE_NAMES_RU.get(final_stage, final_stage)}».")

    st.caption(f"Нормативы {CONFIG_VERSION} от {CONFIG_DATE} · "
               "Открыть в боте: @aist_pilot_me")


# ── Expert Mode ────────────────────────────────────────────────────────────────

def render_expert_mode(profile: SimulatorProfile):
    scenario_choice = st.radio(
        "Сценарий",
        ["S1 — Траектория ступени", "S2 — Траектория баллов", "S3 — Когортная динамика"],
        horizontal=True,
        key="expert_scenario",
    )
    scenario_id = scenario_choice.split("—")[0].strip().lower()

    horizon = st.select_slider(
        "Горизонт",
        options=HORIZON_OPTIONS,
        value=12,
        format_func=lambda v: HORIZON_LABELS[HORIZON_OPTIONS.index(v)],
        key="expert_horizon",
    )

    params: dict[str, Any] = {}

    if scenario_id == "s1":
        st.subheader("Параметры поведения")
        cols = st.columns(3)
        for i, (code, label, mn, mx, step, default) in enumerate(BH_DIMENSIONS):
            current = getattr(profile, code, None) or default
            current = max(mn, min(mx, float(current)))
            val = cols[i % 3].slider(label, min_value=mn, max_value=mx, value=current,
                                     step=step, key=f"s1_{code}")
            params[code] = val

    elif scenario_id == "s2":
        st.subheader("Активность в неделю / месяц")
        c1, c2 = st.columns(2)
        params["lesson_completed_per_week"]   = c1.slider("Уроков в нед",   0.0, 14.0, 2.0, 0.5, key="s2_lc")
        params["wp_completed_per_month"]      = c2.slider("РП в месяц",     0.0, 10.0, 1.0, 0.5, key="s2_wp")
        params["day_close_per_week"]          = c1.slider("Закрытий дня/нед", 0.0, 7.0, 3.0, 0.5, key="s2_dc")
        params["commit_created_per_week"]     = c2.slider("Коммитов в нед",  0.0, 30.0, 3.0, 1.0, key="s2_cc")
        params["knowledge_extracted_per_week"]= c1.slider("Из��лечений знаний/нед", 0.0, 7.0, 1.0, 0.5, key="s2_ke")

    else:  # s3
        st.subheader("Тип когорты")
        cohort_label = st.radio(
            "Когорта",
            ["Первая волна (7 пилотов)", "Все ступени (5 типовых профилей)"],
            key="s3_cohort",
        )
        params["cohort_type"] = "wave1" if "7 пилотов" in cohort_label else "preset_all_stages"

    if st.button("Запустить симуляцию", type="primary", key="expert_run"):
        scenario = ALL_SCENARIOS[scenario_id]
        with st.spinner("Симуляция..."):
            result = scenario.run(profile, params, horizon_weeks=horizon)
        rows = result.as_dicts()
        if not rows:
            st.warning("Нет данных")
            return

        if scenario_id == "s1":
            st.plotly_chart(stage_step_chart(rows), use_container_width=True)
            st.plotly_chart(bh_multiline_chart(rows), use_container_width=True)
            with st.expander("Таблица данных"):
                st.dataframe(rows, use_container_width=True)
        elif scenario_id == "s2":
            weeks = [r["week"] for r in rows]
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=weeks, y=[r.get("balance", 0) for r in rows],
                                     mode="lines", name="Баланс баллов", line=dict(color="#3B82F6")))
            fig.add_trace(go.Bar(x=weeks, y=[r.get("weekly_delta", 0) for r in rows],
                                 name="Прирост за нед", marker_color="#10B981", opacity=0.5,
                                 yaxis="y2"))
            fig.update_layout(
                title="Траектория баллов",
                xaxis_title="Неделя",
                yaxis_title="Баланс",
                yaxis2=dict(overlaying="y", side="right", title="Прирост/нед"),
                height=380, margin=dict(l=20, r=60, t=40, b=20),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig, use_container_width=True)
            with st.expander("Таблица данных"):
                st.dataframe(rows, use_container_width=True)
        else:  # s3
            st.plotly_chart(cohort_bars_chart(rows), use_container_width=True)
            with st.expander("Таблица данных"):
                st.dataframe(rows, use_container_width=True)

    st.caption(f"Нормативы {CONFIG_VERSION} от {CONFIG_DATE}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="Лаборатория симуляций",
        page_icon="🔬",
        layout="wide",
    )
    st.title("🔬 Лаборат��рия симуляций")

    # Sidebar
    with st.sidebar:
        mode = st.radio("Режим", ["Пилот", "Эксперт"], key="mode")
        st.divider()

        account_id = st.text_input("UUID пилота (опционально)", key="account_id",
                                   placeholder="xxxxxxxx-xxxx-...")
        if account_id:
            profile_data = _load_profile_cached(account_id)
            if profile_data and profile_data.get("source") == "real":
                profile = _profile_from_dict(profile_data, account_id)
                st.caption("Профиль загружен из базы данных.")
            else:
                preset_stage = st.selectbox("Типовой профиль", [1, 2, 3, 4, 5],
                                            format_func=lambda s: STAGE_NAMES_RU[s],
                                            key="preset_stage")
                profile = make_preset_profile(preset_stage)
                st.caption("Нет данных — используется типовой профиль.")
        else:
            preset_stage = st.selectbox("Типовой профиль", [1, 2, 3, 4, 5],
                                        format_func=lambda s: STAGE_NAMES_RU[s],
                                        key="preset_stage_anon")
            profile = make_preset_profile(preset_stage)
            st.caption("Введите UUID пилота для загрузки реального профиля.")

    if mode == "Пилот":
        render_pilot_mode(profile)
    else:
        render_expert_mode(profile)


if __name__ == "__main__":
    main()
