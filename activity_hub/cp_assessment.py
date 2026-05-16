"""Cp-assessment utilities — WP-318 Ф3.

# see DP.SC.132, DP.ROLE.042
# see PD.FORM.089 §6.1 v4.2 — формула cp_confirmed_stage

Утилиты для работы с learning.cp_assessments (Neon learning DB):
  - get_latest_cp_assessment(conn, account_id) → dict | None
  - compute_cp_stage(cp_scores) → dict  (cp_confirmed_stage, bottleneck, stream, skip)
  - save_cp_assessment(conn, account_id, cp_scores, source, interface) → int (id)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Optional

import asyncpg

# ── Mandatory slots (FORM.089 §6.1) ──────────────────────────────────────────

MANDATORY_SLOTS = ["cp.rhy", "cp.wld", "cp.skl", "cp.iwe", "cp.int", "cp.agt"]
CP_ASSESSMENT_TTL_DAYS = 180  # 6 месяцев


# ── Computation ───────────────────────────────────────────────────────────────

def compute_cp_stage(cp_scores: dict) -> dict:
    """Вычислить cp-профиль из словаря оценок по слотам.

    FORM.089 §6.1:
      cp_confirmed_stage = min(mandatory)
      bottleneck_slot = argmin(mandatory)
      skip_to_stage = cp_confirmed_stage
      recommended_stream = S{cp_confirmed_stage}

    Args:
        cp_scores: {cp.rhy: 3, cp.wld: 2, ...}

    Returns:
        {stage, bottleneck_slot, recommended_stream, skip_to_stage}
    """
    mandatory_values = {
        slot: int(cp_scores.get(slot, 1))
        for slot in MANDATORY_SLOTS
    }

    stage = min(mandatory_values.values())
    bottleneck_slot = min(mandatory_values, key=mandatory_values.get)
    skip_to_stage = stage
    recommended_stream = f"S{max(1, min(4, stage))}"

    return {
        "stage": stage,
        "bottleneck_slot": bottleneck_slot,
        "recommended_stream": recommended_stream,
        "skip_to_stage": skip_to_stage,
    }


# ── DB helpers ────────────────────────────────────────────────────────────────

async def get_latest_cp_assessment(
    conn: asyncpg.Connection,
    account_id: str,
    only_valid: bool = True,
) -> Optional[dict]:
    """Вернуть последний cp-срез для account_id.

    Args:
        conn:         asyncpg connection к learning DB.
        account_id:   UUID пилота.
        only_valid:   если True — вернуть None при истёкшем TTL.

    Returns:
        dict с полями cp-профиля или None если среза нет / устарел.
    """
    query = """
        SELECT
            id,
            account_id,
            stage,
            bottleneck_slot,
            recommended_stream,
            skip_to_stage,
            cp_scores,
            source,
            interface,
            questions_count,
            rcs_version,
            assessed_at,
            valid_until
        FROM learning.cp_assessments
        WHERE account_id = $1::uuid
        ORDER BY assessed_at DESC
        LIMIT 1
    """
    row = await conn.fetchrow(query, account_id)
    if row is None:
        return None

    row_dict = dict(row)

    if only_valid and row_dict.get("valid_until"):
        if row_dict["valid_until"] < datetime.now(timezone.utc):
            return None

    if isinstance(row_dict.get("cp_scores"), str):
        row_dict["cp_scores"] = json.loads(row_dict["cp_scores"])

    row_dict["account_id"] = str(row_dict["account_id"])
    for field in ("assessed_at", "valid_until"):
        if row_dict.get(field):
            row_dict[field] = row_dict[field].isoformat()

    return row_dict


async def save_cp_assessment(
    conn: asyncpg.Connection,
    account_id: str,
    cp_scores: dict,
    source: str,
    interface: str,
    questions_count: Optional[int] = None,
    rcs_version: str = "v4.2",
) -> int:
    """Сохранить cp-срез в learning.cp_assessments.

    Вычисляет stage/bottleneck/stream/skip из cp_scores (FORM.089 §6.1).
    Ставит TTL = NOW() + 6 месяцев.

    Returns:
        id записи.
    """
    profile = compute_cp_stage(cp_scores)
    valid_until = datetime.now(timezone.utc) + timedelta(days=CP_ASSESSMENT_TTL_DAYS)

    row_id = await conn.fetchval(
        """
        INSERT INTO learning.cp_assessments (
            account_id, stage, bottleneck_slot, recommended_stream, skip_to_stage,
            cp_scores, source, interface, questions_count, rcs_version, valid_until
        ) VALUES (
            $1::uuid, $2, $3, $4, $5,
            $6::jsonb, $7, $8, $9, $10, $11
        )
        RETURNING id
        """,
        account_id,
        profile["stage"],
        profile["bottleneck_slot"],
        profile["recommended_stream"],
        profile["skip_to_stage"],
        json.dumps(cp_scores),
        source,
        interface,
        questions_count,
        rcs_version,
        valid_until,
    )
    return row_id


async def emit_invalidation_proposed(
    conn: asyncpg.Connection,
    account_id: str,
    reason: str,
) -> None:
    """Записать событие diagnostic.invalidation_proposed в learning.domain_event.

    Вызывается фоновым Диагностом (DP.ROLE.042 §3.4) при срабатывании
    триггера инвалидации. НЕ пишет пилоту напрямую — только событие.
    Активные роли (Навигатор/Портной/Аттестатор) читают при следующем
    взаимодействии с пилотом.

    Args:
        conn:       asyncpg connection к learning DB.
        account_id: UUID пилота.
        reason:     причина инвалидации ('ttl_expired', 'age_90d', 'bh_spike').
    """
    await conn.execute(
        """
        INSERT INTO learning.domain_event (account_id, event_type, payload)
        VALUES ($1::uuid, 'diagnostic.invalidation_proposed', $2::jsonb)
        """,
        account_id,
        json.dumps({"reason": reason, "source": "diagnostician_watcher"}),
    )
