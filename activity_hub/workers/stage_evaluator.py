"""Stage Evaluator — SR.001-SR.004 (WP-310 Ф4, FORM.089 §12.3).

# see DP.SC.020, B7.3.6 privacy spec, PD.FORM.089-learner-rcs.md §12
# see iwe-actions-catalog.md §7 (весовая конфигурация)

Запускается на cron tsekh-1 (после profiler, 04:35 МСК) или on-demand backfill.

Алгоритм (FORM.089 §12.3 compute_stage_mvp):
  1. Загрузить opt-in consent: SELECT account_id FROM learning.tracking_consent WHERE opt_in=TRUE.
  2. Для каждого account_id:
     a. cur = MAX(to_stage) FROM learning.stage_transitions WHERE account_id = $1 (default 1).
     b. Вычислить 5 индексов по multi-window схеме FORM.089 §12.2:
        s = Систематичность (self_dev_days_per_week)   — SELF_DEV_EVENT_TYPES
        t = Инвестированное время (avg_hours_per_week)  — slot_logged + lesson_completed ONLY
        m = Методичность мышления (events/30d)          — фиксированное окно 30д
        w = Системность мировоззрения (weighted score)  — каталог §7.4
        a = Агентность (weighted score)                 — каталог §7.5
        total_hours = накопленные часы all-time (hard gate, slot_logged + lesson_completed)
        stb = устойчивость (max gap без SELF_DEV, дни → idx)
     c. computed = compute_stage_mvp(s, t, m, w, a, total_hours, stb)  # FORM.089 §12.3
     d. if computed > cur:
          Двойной gate (FORM.089 §5.1, WP-318 Ф4):
            cp_data = get_latest_cp_assessment(conn, account_id)
            Нет валидного cp → proceed (не блокировать, CTA через бот), cp_assessment_id=NULL
            cp_confirmed < computed → BLOCK (cp_gate_blocked), не вставлять
            cp_confirmed >= computed → proceed + cp_assessment_id = cp_data.id
          INSERT stage_transitions (from=cur, to=computed, triggered_by=..., evidence=snapshot, cp_assessment_id)
  3. Метрики: stage_transitions_inserted, users_processed, users_skipped_consent, errors.

L2-PRIVACY (B7.3.6): каждый SELECT/INSERT — явный WHERE account_id = $1.
Evidence JSONB: только числа/индексы — никакого raw-text, email, имён.
CHECK to_stage > from_stage: worker всегда вставляет инкремент; для 0→1 triggered_by='manual_calibration'.

Multi-window (FORM.089 §12.2):
  Для s и t проверяем ВСЕ 5 уровней с их собственными окнами (1/4/8/12/24 нед).
  Берём МАКСИМАЛЬНЫЙ подтверждённый индекс. Один запрос с 5 FILTER-окнами.
  Это даёт более точный idx чем один окно текущей ступени.

К4 fix (WP-310 Ф10, FORM.089 v4 §12.2):
  coding_time (WakaTime) ИСКЛЮЧЁН из bh.inv (t_idx) и CUMULATIVE_HOURS_GATE.
  Причина: WakaTime считает все репо (practice + work domain) → overestimates.
  До реализации repo_domain_map (WP-214 Ф10.5) используются только slot_logged + lesson_completed.
  coding_time_hpw остаётся в evidence как информационное поле (прозрачность, отладка).

Ф13a (WP-310, Option A): новый event_type НЕ вводится. Self-report идёт через
  slot_logged с payload.source='self_report_*'. t_idx и total_hours считают
  slot_logged с любым source равноценно (Pack: FORM.089 §12.2, commit 1a54520).
  Evidence добавляет source-разбивку за 4w: sl_active_4w_hpw + sl_selfrep_4w_hpw.

bh.stb (WP-310 Ф10, FORM.089 v4 §12.2):
  Максимальный разрыв (дни) без SELF_DEV события за 168 дней (24 нед).
  norm_stb() → индекс 0-5. STB_GATE в compute_stage_mvp.
"""
from __future__ import annotations

import argparse
import asyncio
import json

from activity_hub.core.health_metrics import write_internal_metric
from activity_hub.cp_assessment import get_latest_cp_assessment
from activity_hub.core.stage_config import (
    ACCOUNTING_WEEKS,
    CUMULATIVE_HOURS_GATE,
    S_THRESHOLDS,
    STB_GATE,
    T_THRESHOLDS,
    SELF_DEV_EVENT_TYPES,
    TRIGGERED_BY,
    W_WEIGHTS,
    A_WEIGHTS,
    norm_m,
    norm_stb,
    norm_w,
    norm_a,
    compute_stage_mvp,
)
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import asyncpg

logger = logging.getLogger("stage_evaluator")


# ── Вычислители характеристик ────────────────────────────────────────────────

async def calc_s(conn: asyncpg.Connection, account_id: str) -> tuple[int, dict]:
    """Систематичность (s) — multi-window per FORM.089 §12.2.

    Один SQL-запрос с FILTER по 5 окнам. Берём максимальный idx,
    чей порог days_per_week выполнен в своём окне.
    """
    row = await conn.fetchrow(
        """
        SELECT
          COUNT(DISTINCT DATE(occurred_at)) FILTER (
            WHERE occurred_at >= NOW() - INTERVAL '7 days'
          ) AS days_1w,
          COUNT(DISTINCT DATE(occurred_at)) FILTER (
            WHERE occurred_at >= NOW() - INTERVAL '28 days'
          ) AS days_4w,
          COUNT(DISTINCT DATE(occurred_at)) FILTER (
            WHERE occurred_at >= NOW() - INTERVAL '56 days'
          ) AS days_8w,
          COUNT(DISTINCT DATE(occurred_at)) FILTER (
            WHERE occurred_at >= NOW() - INTERVAL '84 days'
          ) AS days_12w,
          COUNT(DISTINCT DATE(occurred_at)) FILTER (
            WHERE occurred_at >= NOW() - INTERVAL '168 days'
          ) AS days_24w
        FROM public.domain_event
        WHERE account_id = $1::uuid
          AND event_type = ANY($2::text[])
        """,
        account_id, SELF_DEV_EVENT_TYPES,
    )

    window_days = {1: row["days_1w"], 4: row["days_4w"], 8: row["days_8w"],
                   12: row["days_12w"], 24: row["days_24w"]}
    dpw = {wk: (window_days[wk] or 0) / wk for wk in window_days}

    s_idx = 0
    for idx, wk, threshold in S_THRESHOLDS:
        if dpw[wk] >= threshold:
            s_idx = idx
            break

    return s_idx, {"days_per_window": window_days, "days_per_week": dpw}


async def calc_t(conn: asyncpg.Connection, account_id: str) -> tuple[int, dict]:
    """Инвестированное время (t) — multi-window per FORM.089 §12.2.

    К4 fix (WP-310 Ф10): coding_time (WakaTime) ИСКЛЮЧЁН из t_idx.
    Используются только slot_logged + lesson_completed.
    coding_time_hpw остаётся в evidence как информационное поле
    (прозрачность; WakaTime считает все репо, включая рабочие).
    Полная интеграция с domain_filter — WP-214 Ф10.5.

    Ф13a (WP-310, Option A): нет event_type 'time_self_reported'. Self-report хранится
    в slot_logged с payload.source ∈ {active, self_report_backfill, self_report_daily,
    self_report_weekly}. slot_logged с любым source считается равноценно.
    Evidence добавляет source-разбивку за 4w: sl_active_4w_hpw + sl_selfrep_4w_hpw.
    """
    row = await conn.fetchrow(
        """
        SELECT
          -- coding_time по окнам (WakaTime — все репозитории, ИНФОРМАЦИОННО)
          COALESCE(SUM((payload->>'total_seconds')::float / 3600)
            FILTER (WHERE event_type = 'coding_time'
                      AND occurred_at >= NOW() - INTERVAL '7 days'), 0) AS ct_1w,
          COALESCE(SUM((payload->>'total_seconds')::float / 3600)
            FILTER (WHERE event_type = 'coding_time'
                      AND occurred_at >= NOW() - INTERVAL '28 days'), 0) AS ct_4w,
          COALESCE(SUM((payload->>'total_seconds')::float / 3600)
            FILTER (WHERE event_type = 'coding_time'
                      AND occurred_at >= NOW() - INTERVAL '56 days'), 0) AS ct_8w,
          COALESCE(SUM((payload->>'total_seconds')::float / 3600)
            FILTER (WHERE event_type = 'coding_time'
                      AND occurred_at >= NOW() - INTERVAL '84 days'), 0) AS ct_12w,
          COALESCE(SUM((payload->>'total_seconds')::float / 3600)
            FILTER (WHERE event_type = 'coding_time'
                      AND occurred_at >= NOW() - INTERVAL '168 days'), 0) AS ct_24w,
          -- slot_logged по окнам (все source: active + self_report_*)
          COALESCE(SUM((payload->>'hours')::float)
            FILTER (WHERE event_type = 'slot_logged'
                      AND payload->>'hours' IS NOT NULL
                      AND occurred_at >= NOW() - INTERVAL '7 days'), 0) AS sl_1w,
          COALESCE(SUM((payload->>'hours')::float)
            FILTER (WHERE event_type = 'slot_logged'
                      AND payload->>'hours' IS NOT NULL
                      AND occurred_at >= NOW() - INTERVAL '28 days'), 0) AS sl_4w,
          COALESCE(SUM((payload->>'hours')::float)
            FILTER (WHERE event_type = 'slot_logged'
                      AND payload->>'hours' IS NOT NULL
                      AND occurred_at >= NOW() - INTERVAL '56 days'), 0) AS sl_8w,
          COALESCE(SUM((payload->>'hours')::float)
            FILTER (WHERE event_type = 'slot_logged'
                      AND payload->>'hours' IS NOT NULL
                      AND occurred_at >= NOW() - INTERVAL '84 days'), 0) AS sl_12w,
          COALESCE(SUM((payload->>'hours')::float)
            FILTER (WHERE event_type = 'slot_logged'
                      AND payload->>'hours' IS NOT NULL
                      AND occurred_at >= NOW() - INTERVAL '168 days'), 0) AS sl_24w,
          -- lesson_completed по окнам
          COALESCE(SUM((payload->>'duration_minutes')::float / 60)
            FILTER (WHERE event_type = 'lesson_completed'
                      AND payload->>'duration_minutes' IS NOT NULL
                      AND occurred_at >= NOW() - INTERVAL '7 days'), 0) AS lc_1w,
          COALESCE(SUM((payload->>'duration_minutes')::float / 60)
            FILTER (WHERE event_type = 'lesson_completed'
                      AND payload->>'duration_minutes' IS NOT NULL
                      AND occurred_at >= NOW() - INTERVAL '28 days'), 0) AS lc_4w,
          COALESCE(SUM((payload->>'duration_minutes')::float / 60)
            FILTER (WHERE event_type = 'lesson_completed'
                      AND payload->>'duration_minutes' IS NOT NULL
                      AND occurred_at >= NOW() - INTERVAL '56 days'), 0) AS lc_8w,
          COALESCE(SUM((payload->>'duration_minutes')::float / 60)
            FILTER (WHERE event_type = 'lesson_completed'
                      AND payload->>'duration_minutes' IS NOT NULL
                      AND occurred_at >= NOW() - INTERVAL '84 days'), 0) AS lc_12w,
          COALESCE(SUM((payload->>'duration_minutes')::float / 60)
            FILTER (WHERE event_type = 'lesson_completed'
                      AND payload->>'duration_minutes' IS NOT NULL
                      AND occurred_at >= NOW() - INTERVAL '168 days'), 0) AS lc_24w,
          -- Option A: source-разбивка slot_logged за 4w (ИНФОРМАЦИОННО)
          COALESCE(SUM((payload->>'hours')::float)
            FILTER (WHERE event_type = 'slot_logged'
                      AND payload->>'hours' IS NOT NULL
                      AND COALESCE(payload->>'source', 'active') = 'active'
                      AND occurred_at >= NOW() - INTERVAL '28 days'), 0) AS sl_active_4w,
          COALESCE(SUM((payload->>'hours')::float)
            FILTER (WHERE event_type = 'slot_logged'
                      AND payload->>'hours' IS NOT NULL
                      AND payload->>'source' LIKE 'self_report%'
                      AND occurred_at >= NOW() - INTERVAL '28 days'), 0) AS sl_selfrep_4w
        FROM public.domain_event
        WHERE account_id = $1::uuid
          AND event_type = ANY(ARRAY['coding_time','slot_logged','lesson_completed'])
        """,
        account_id,
    )

    wk_map = {1: "1w", 4: "4w", 8: "8w", 12: "12w", 24: "24w"}
    # К4 + Ф13a: t_idx = slot_logged + lesson_completed (без coding_time).
    # Self-report ⊂ slot_logged.payload.source — уже включён в sl_* (Option A).
    total_hpw: dict[int, float] = {}
    coding_time_hpw: dict[int, float] = {}  # ИНФОРМАЦИОННО — для прозрачности/дебага
    for wk, suffix in wk_map.items():
        ct = row[f"ct_{suffix}"] or 0.0
        sl = row[f"sl_{suffix}"] or 0.0
        lc = row[f"lc_{suffix}"] or 0.0
        total_hpw[wk] = (sl + lc) / wk  # К4: БЕЗ coding_time. Self-report ⊂ slot_logged.
        coding_time_hpw[wk] = ct / wk   # ИНФОРМАЦИОННО

    t_idx = 0
    for idx, wk, threshold in T_THRESHOLDS:
        if total_hpw[wk] >= threshold:
            t_idx = idx
            break

    return t_idx, {
        "hours_per_week": total_hpw,
        "coding_time_hpw": coding_time_hpw,
        "t_slot_only_idx": t_idx,  # alias для совместимости evidence
        # Option A: source-разбивка slot_logged за 4w (активные vs self-report)
        "sl_active_4w_hpw": round((row["sl_active_4w"] or 0.0) / 4, 2),
        "sl_selfrep_4w_hpw": round((row["sl_selfrep_4w"] or 0.0) / 4, 2),
        # Флаг: WakaTime включает work-domain репо → исключён из bh.inv (К4)
        # Полная интеграция с domain_filter — WP-214 Ф10.5
        "wakatime_domain_filter_pending": True,
    }


async def calc_m(conn: asyncpg.Connection, account_id: str) -> tuple[int, dict]:
    """Методичность (m) — прямой счёт событий за 30д (FORM.089 §12.2)."""
    rows = await conn.fetch(
        """
        SELECT event_type, COUNT(*) AS cnt
        FROM public.domain_event
        WHERE account_id = $1::uuid
          AND event_type = ANY(ARRAY[
            'lesson_completed','knowledge_extracted','pack_updated','qualification_granted'
          ])
          AND occurred_at >= NOW() - INTERVAL '30 days'
        GROUP BY event_type
        """,
        account_id,
    )
    breakdown = {r["event_type"]: int(r["cnt"]) for r in rows}
    total = sum(breakdown.values())
    return norm_m(total), {"events_30d": total, "breakdown": breakdown}


async def calc_w(conn: asyncpg.Connection, account_id: str) -> tuple[int, dict]:
    """Системность мировоззрения (w) — взвешенная сумма за 30д (каталог §7.4).

    Веса: week_plan_closed×4 (quality≥3) или ×1 (fallback — Gap-А),
          month_plan_closed×8, strategy_session_completed×6,
          knowledge_extracted×1, pack_updated×1.
    """
    rows = await conn.fetch(
        """
        SELECT event_type, COUNT(*) AS cnt
        FROM public.domain_event
        WHERE account_id = $1::uuid
          AND event_type = ANY(ARRAY[
            'week_plan_closed','month_plan_closed','strategy_session_completed',
            'knowledge_extracted','pack_updated'
          ])
          AND occurred_at >= NOW() - INTERVAL '30 days'
        GROUP BY event_type
        """,
        account_id,
    )
    breakdown = {r["event_type"]: int(r["cnt"]) for r in rows}
    score = sum(breakdown.get(et, 0) * w for et, w in W_WEIGHTS.items())
    return norm_w(score), {"score": score, "breakdown": breakdown,
                            "note": "week_plan_closed weight=1 (Gap-А: quality field not filled)"}


async def calc_a(conn: asyncpg.Connection, account_id: str) -> tuple[int, dict]:
    """Агентность (a) — взвешенная сумма за 30д (каталог §7.5).

    Веса: wp_created×3, wp_closed×2, wp_completed×2, strategy_session_completed×5.
    MVP: все wp_created считаются самоинициированными (нет поля initiator, WP-214 Ф10.5).
    """
    rows = await conn.fetch(
        """
        SELECT event_type, COUNT(*) AS cnt
        FROM public.domain_event
        WHERE account_id = $1::uuid
          AND event_type = ANY(ARRAY[
            'wp_created','wp_closed','wp_completed','strategy_session_completed'
          ])
          AND occurred_at >= NOW() - INTERVAL '30 days'
        GROUP BY event_type
        """,
        account_id,
    )
    breakdown = {r["event_type"]: int(r["cnt"]) for r in rows}
    score = sum(breakdown.get(et, 0) * w for et, w in A_WEIGHTS.items())
    return norm_a(score), {"score": score, "breakdown": breakdown}


async def calc_total_hours(conn: asyncpg.Connection, account_id: str) -> float:
    """Накопленные часы за всё время (hard gate CUMULATIVE_HOURS_GATE, FORM.089 §12.2).

    К4 fix (WP-310 Ф10): только slot_logged + lesson_completed.
    coding_time (WakaTime) исключён — включает все репо, не только саморазвитие.
    Ф13a (Option A): slot_logged включает все source (active + self_report_*) —
    нет отдельного event_type 'time_self_reported' (Pack: FORM.089 §12.2).
    """
    row = await conn.fetchrow(
        """
        SELECT
          COALESCE(SUM((payload->>'hours')::float)
            FILTER (WHERE event_type = 'slot_logged'
                      AND payload->>'hours' IS NOT NULL), 0) AS sl,
          COALESCE(SUM((payload->>'duration_minutes')::float / 60)
            FILTER (WHERE event_type = 'lesson_completed'
                      AND payload->>'duration_minutes' IS NOT NULL), 0) AS lc
        FROM public.domain_event
        WHERE account_id = $1::uuid
          AND event_type = ANY(ARRAY['slot_logged','lesson_completed'])
        """,
        account_id,
    )
    return (row["sl"] or 0.0) + (row["lc"] or 0.0)


async def calc_stb(conn: asyncpg.Connection, account_id: str) -> tuple[int, dict]:
    """bh.stb — устойчивость: максимальный разрыв без SELF_DEV события (FORM.089 §12.2).

    SQL: window function LAG → MAX gap между последовательными днями SELF_DEV активности
    за 168 дней (24 нед, ACCOUNTING_WEEKS[5]).

    Edge cases:
      - <2 событий: нет разрыва → max_gap_days=0 → norm_stb(0)=5 (не блокирует).
      - 0 событий: нет разрыва → max_gap_days=0 → norm_stb(0)=5.
      - NULL из COALESCE: обрабатывается как 0.

    Returns:
        (stb_idx: int, detail: {"max_gap_days": int, "self_dev_event_count": int})
    """
    row = await conn.fetchrow(
        """
        WITH self_dev AS (
          SELECT DATE(occurred_at) AS day
          FROM public.domain_event
          WHERE account_id = $1::uuid
            AND event_type = ANY($2::text[])
            AND occurred_at >= NOW() - INTERVAL '168 days'
          GROUP BY 1
        ),
        gaps AS (
          -- date - date в PG возвращает integer (дни), не interval
          SELECT (day - LAG(day) OVER (ORDER BY day))::int AS gap_days
          FROM self_dev
        )
        SELECT
          COALESCE(MAX(gap_days), 0) AS max_gap_days,
          (SELECT COUNT(*) FROM self_dev) AS event_count
        FROM gaps;
        """,
        account_id, SELF_DEV_EVENT_TYPES,
    )

    max_gap_days = int(row["max_gap_days"] or 0)
    event_count = int(row["event_count"] or 0)
    stb_idx = norm_stb(max_gap_days)

    return stb_idx, {"max_gap_days": max_gap_days, "self_dev_event_count": event_count}


# compute_stage_mvp импортирован из stage_config — см. импорты вверху файла

# ── Загрузка всех метрик ─────────────────────────────────────────────────────

async def load_rcs_metrics(conn: asyncpg.Connection, account_id: str) -> dict:
    """Вычислить все 5 индексов + total_hours + stb. Возвращает полный snapshot для evidence.

    L2-PRIVACY: явный WHERE account_id = $1 в каждом вложенном запросе.
    """
    s_idx, s_detail = await calc_s(conn, account_id)
    t_idx, t_detail = await calc_t(conn, account_id)
    m_idx, m_detail = await calc_m(conn, account_id)
    w_idx, w_detail = await calc_w(conn, account_id)
    a_idx, a_detail = await calc_a(conn, account_id)
    total_hours = await calc_total_hours(conn, account_id)
    stb_idx, stb_detail = await calc_stb(conn, account_id)

    return {
        "s": s_idx, "t": t_idx, "m": m_idx, "w": w_idx, "a": a_idx,
        "total_hours": total_hours,
        "stb": stb_idx,
        "detail": {
            "s": s_detail, "t": t_detail, "m": m_detail,
            "w": w_detail, "a": a_detail, "stb": stb_detail,
        },
    }


# ── evaluate_one и run_stage_evaluator ──────────────────────────────────────

async def evaluate_one(
    conn: asyncpg.Connection,
    account_id: str,
    dry_run: bool = False,
) -> dict:
    """Обработать одного opt-in пользователя.

    Returns: {"action": "insert"|"noop"|"skip"|"error", "from": int, "to": int, ...}
    """
    cur_row = await conn.fetchrow(
        "SELECT COALESCE(MAX(to_stage), 0) AS cur "
        "FROM learning.stage_transitions WHERE account_id = $1::uuid",
        account_id,
    )
    cur = int(cur_row["cur"] or 0)

    metrics = await load_rcs_metrics(conn, account_id)
    s, t, m, w, a = metrics["s"], metrics["t"], metrics["m"], metrics["w"], metrics["a"]
    total_hours = metrics["total_hours"]
    stb = metrics["stb"]
    computed = compute_stage_mvp(s, t, m, w, a, total_hours, stb=stb)

    if computed <= cur:
        return {"action": "noop", "account_id": account_id, "cur": cur, "computed": computed}

    if computed < 1 or computed > 5:
        return {"action": "skip", "account_id": account_id,
                "reason": f"computed_out_of_range:{computed}"}

    # Двойной gate: cp-подтверждение (FORM.089 §5.1, WP-318 Ф4, DP.SC.132)
    # Нет валидного cp → не блокировать (DP.SC.132 §Режим отказа), cp_assessment_id=NULL
    # Есть cp и cp_confirmed < computed → заблокировать переход
    # Есть cp и cp_confirmed >= computed → разрешить, привязать cp_assessment_id
    cp_data = await get_latest_cp_assessment(conn, account_id, only_valid=True)
    cp_assessment_id: int | None = None
    if cp_data is not None:
        cp_confirmed = cp_data["stage"]
        if cp_confirmed < computed:
            return {
                "action": "cp_gate_blocked",
                "account_id": account_id,
                "bh_recommended": computed,
                "cp_confirmed": cp_confirmed,
                "bottleneck": cp_data.get("bottleneck_slot"),
            }
        cp_assessment_id = cp_data["id"]

    triggered_by = TRIGGERED_BY[computed]
    # coding_time_hpw за 4 нед — информационно (для дебага без блокирования)
    t_with_coding_time_hpw_4w = round(
        metrics["detail"]["t"].get("coding_time_hpw", {}).get(4, 0.0), 2
    )
    evidence = {
        # Индексы (К4: t — только slot_logged + lesson_completed)
        "s": s, "t": t, "m": m, "w": w, "a": a,
        "total_hours": round(total_hours, 1),
        # bh.stb — устойчивость (К4 + FORM.089 v4 §12.2)
        "stb": stb,
        "max_gap_days": metrics["detail"]["stb"]["max_gap_days"],
        # Информационные поля (не участвуют в расчёте ступени)
        "t_slot_only_idx": metrics["detail"]["t"].get("t_slot_only_idx", t),
        "t_with_coding_time_hpw_4w": t_with_coding_time_hpw_4w,
        "wakatime_domain_filter_pending": True,
        # Option A: прозрачность вклада self-report через slot_logged.source (Ф13a)
        "t_selfrep_4w_hpw": metrics["detail"]["t"].get("sl_selfrep_4w_hpw", 0.0),
        # Ключевые метрики
        "m_events_30d": metrics["detail"]["m"]["events_30d"],
        "w_score": metrics["detail"]["w"]["score"],
        "a_score": metrics["detail"]["a"]["score"],
        "s_days_8w": metrics["detail"]["s"]["days_per_window"].get(8, 0),
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }

    if dry_run:
        return {
            "action": "would_insert",
            "account_id": account_id,
            "from": cur,
            "to": computed,
            "triggered_by": triggered_by,
            "cp_assessment_id": cp_assessment_id,
            "evidence": evidence,
        }

    # ON CONFLICT DO NOTHING — migration 114 добавила UNIQUE(account_id, to_stage).
    await conn.execute(
        """
        INSERT INTO learning.stage_transitions
          (account_id, from_stage, to_stage, triggered_by, evidence, cp_assessment_id)
        VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6)
        ON CONFLICT (account_id, to_stage) DO NOTHING
        """,
        account_id, cur, computed, triggered_by, json.dumps(evidence), cp_assessment_id,
    )
    return {
        "action": "insert",
        "account_id": account_id,
        "from": cur,
        "to": computed,
        "triggered_by": triggered_by,
        "cp_assessment_id": cp_assessment_id,
    }


async def run_stage_evaluator(
    learning_dsn: str,
    user_filter: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Главный entrypoint worker'а.

    Args:
        learning_dsn: DSN к learning БД (env LEARNING_URL или DATABASE_URL_STAGE_EVALUATOR).
        user_filter: если задан — обработать только этого account_id.
        dry_run: показать что было бы вставлено, не писать.
    """
    stats = {"processed": 0, "inserted": 0, "noop": 0, "skipped_consent": 0, "errors": 0}

    conn = await asyncpg.connect(learning_dsn, statement_cache_size=0)
    try:
        if user_filter:
            consent = await conn.fetchval(
                "SELECT opt_in FROM learning.tracking_consent "
                "WHERE account_id = $1::uuid", user_filter,
            )
            if not consent:
                stats["skipped_consent"] += 1
                logger.warning("user %s: no opt-in consent — skip", user_filter[:8])
                return stats
            account_ids = [user_filter]
        else:
            rows = await conn.fetch(
                "SELECT account_id::text AS account_id "
                "FROM learning.tracking_consent WHERE opt_in = TRUE"
            )
            account_ids = [r["account_id"] for r in rows]

        logger.info("processing %d account(s), dry_run=%s", len(account_ids), dry_run)

        for account_id in account_ids:
            try:
                result = await evaluate_one(conn, account_id, dry_run=dry_run)
                stats["processed"] += 1
                if result["action"] == "insert":
                    stats["inserted"] += 1
                    logger.info(
                        "INSERT %s: %d → %d (%s)",
                        account_id[:8], result["from"], result["to"], result["triggered_by"],
                    )
                elif result["action"] == "would_insert":
                    stats["inserted"] += 1
                    logger.info(
                        "[dry-run] would INSERT %s: %d → %d (%s) evidence=%s",
                        account_id[:8], result["from"], result["to"],
                        result["triggered_by"], json.dumps(result["evidence"]),
                    )
                elif result["action"] == "noop":
                    stats["noop"] += 1
                    logger.debug(
                        "noop %s: cur=%d computed=%d",
                        account_id[:8], result["cur"], result["computed"],
                    )
                else:
                    stats["errors"] += 1
                    logger.warning("skip %s: %s", account_id[:8], result.get("reason"))
            except Exception as e:
                stats["errors"] += 1
                logger.exception("error processing %s: %s", account_id[:8], e)
    finally:
        # Heartbeat в health.internal_metrics — alerter ловит «evaluator не запускался N дней»
        # по freshness measured_at. Best-effort, не блокирует close.
        if not dry_run:
            try:
                hb_conn = await asyncpg.connect(learning_dsn, statement_cache_size=0)
                try:
                    await write_internal_metric(
                        hb_conn,
                        metric_name="stage_evaluator_completed",
                        worker="stage-evaluator-neon",
                        value_numeric=float(stats["inserted"]),
                        value_jsonb=stats,
                    )
                finally:
                    await hb_conn.close()
            except Exception as exc:
                logger.warning("heartbeat write failed: %s", exc)
        await conn.close()

    logger.info("stage_evaluator done: %s", stats)
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Stage Evaluator (SR.001-SR.004, FORM.089 §12.3)"
    )
    parser.add_argument("--user", help="Backfill для одного account_id (UUID)")
    parser.add_argument("--backfill", action="store_true", help="Backfill mode (alias)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show would-be inserts without writing")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    learning_dsn = (
        os.environ.get("DATABASE_URL_STAGE_EVALUATOR")
        or os.environ.get("LEARNING_URL")
        or os.environ.get("DATABASE_URL_LEARNING_DIRECT")
    )
    if not learning_dsn:
        logging.getLogger(__name__).critical(
            "LEARNING_URL / DATABASE_URL_STAGE_EVALUATOR not set"
        )
        sys.exit(1)

    asyncio.run(run_stage_evaluator(
        learning_dsn=learning_dsn,
        user_filter=args.user,
        dry_run=args.dry_run,
    ))


if __name__ == "__main__":
    main()
