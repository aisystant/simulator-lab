"""Simulator data layer — WP-319 Ф2.

# see DP.SC.133, DP.ROLE.043

Read-only загрузка профиля пилота из Neon через stage_simulator_ro.
Все функции — async, все запросы — SELECT только.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[3]))

from activity_hub.engines.simulator.base import SimulatorProfile
from activity_hub.core.stage_config import (
    norm_m, norm_w, norm_a, norm_stb,
    S_THRESHOLDS, T_THRESHOLDS,
    CONFIG_VERSION,
)


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


async def load_profile(account_id: str, conn) -> SimulatorProfile:
    """Загрузить bh-профиль пилота.

    confirmed_stage = max(cp_assessments.stage, stage_transitions.to_stage).
    Правило: машина может только поднять ступень выше подтверждённой диагностом,
    но не опустить ниже — пока накопленных данных недостаточно.
    """
    row = await conn.fetchrow(
        """
        SELECT to_stage, evidence, occurred_at
        FROM learning.stage_transitions
        WHERE account_id = $1
        ORDER BY occurred_at DESC
        LIMIT 1
        """,
        account_id,
    )

    # Подтверждённая ступень из диагностики (пол)
    cp_row = await conn.fetchrow(
        """
        SELECT stage FROM learning.cp_assessments
        WHERE account_id = $1
          AND (valid_until IS NULL OR valid_until > NOW())
        ORDER BY assessed_at DESC
        LIMIT 1
        """,
        account_id,
    )
    cp_stage = int(cp_row["stage"]) if cp_row else 0

    if row is None:
        return SimulatorProfile(
            account_id=account_id,
            confirmed_stage=cp_stage,
            source="no_data" if cp_stage == 0 else "cp_only",
        )

    evidence = row["evidence"]
    if isinstance(evidence, str):
        evidence = json.loads(evidence)
    if not isinstance(evidence, dict):
        evidence = {}

    # stage_evaluator пишет плоский формат: {"s": 1, "t": 2, "m": 1, "w": 0, "a": 0, ...}
    s   = int(evidence.get("s", 0))
    t   = int(evidence.get("t", 0))
    m   = int(evidence.get("m", 0))
    w   = int(evidence.get("w", 0))
    a   = int(evidence.get("a", 0))
    # stb default=5: evaluator не всегда пишет stb в evidence.
    stb = int(evidence.get("stb", 5))

    # Сырые метрики
    hours_per_week = float(evidence.get("hours_per_week", 0.0))
    days_per_week  = float(evidence.get("days_per_week", 0.0))
    total_hours    = float(evidence.get("total_hours", 0.0))

    evaluator_stage = int(row["to_stage"])
    confirmed_stage = max(cp_stage, evaluator_stage)

    return SimulatorProfile(
        account_id=account_id,
        s=s, t=t, m=m, w=w, a=a, stb=stb,
        hours_per_week=hours_per_week,
        days_per_week=days_per_week,
        total_hours=total_hours,
        confirmed_stage=confirmed_stage,
        source="real",
    )


async def load_real_events(account_id: str, conn, days: int = 90) -> list[dict]:
    """Загрузить сырые события пилота за последние N дней.

    Используется в сценариях S2 (баллы) и S3 (когортная динамика).
    Возвращает список dict {event_type, occurred_at, payload}.
    """
    rows = await conn.fetch(
        """
        SELECT event_type, occurred_at, payload
        FROM learning.events
        WHERE account_id = $1
          AND occurred_at >= NOW() - ($2 || ' days')::interval
        ORDER BY occurred_at ASC
        """,
        account_id,
        str(days),
    )
    return [dict(r) for r in rows]


def make_preset_profile(stage: int) -> SimulatorProfile:
    """Типовой профиль для заданной ступени (используется как fallback без Neon)."""
    from activity_hub.core.stage_config import STAGE_GATE_MATRIX, STB_GATE, CUMULATIVE_HOURS_GATE

    presets = {
        1: SimulatorProfile(s=1, t=1, m=1, w=0, a=0, stb=1,
                            hours_per_week=1.0, days_per_week=1.0, total_hours=5.0,
                            source="preset"),
        2: SimulatorProfile(s=2, t=2, m=2, w=1, a=1, stb=2,
                            hours_per_week=4.0, days_per_week=3.0, total_hours=25.0,
                            source="preset"),
        3: SimulatorProfile(s=3, t=3, m=3, w=3, a=3, stb=3,
                            hours_per_week=6.0, days_per_week=5.0, total_hours=55.0,
                            source="preset"),
        4: SimulatorProfile(s=4, t=4, m=3, w=4, a=4, stb=4,
                            hours_per_week=8.0, days_per_week=6.0, total_hours=100.0,
                            source="preset"),
        5: SimulatorProfile(s=5, t=5, m=5, w=5, a=5, stb=5,
                            hours_per_week=10.0, days_per_week=6.7, total_hours=165.0,
                            source="preset"),
    }
    return presets.get(stage, presets[1])
