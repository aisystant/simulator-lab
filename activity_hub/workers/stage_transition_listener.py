"""Stage Transition Listener — WP-310 Ф3 (DP.ROLE.041 Аттестатор).

# see DP.ROLE.041, DP.SC.020, PD.FORM.089 §5

Polling-воркер на learning.stage_transitions. Обнаруживает новые переходы ступеней
(записанные stage_evaluator.py) и запускает downstream-эффекты.

Downstream (Ф3, реализован):
  1. UPDATE indicators.calculated_profile (stage_id в 3_derived.3_4_qualification)
  2. Уведомление пилоту в бот (@aist_me_bot) — telegram_id из persona.ory_identity
  3. INSERT в learning.guide_render_queue → render-pilot-guides подхватит (WP-309 Ф7)
  4. Целевой ритм — включён в текст уведомления пилоту

Архитектура (паттерн profiler_subscriber_neon):
  - Polling cursor в public.stage_listener_cursor (self-bootstrapping, CREATE TABLE IF NOT EXISTS).
  - Cursor = (last_seen_at, last_seen_id) — keyset pagination по (occurred_at, id).
    Защищает от потери строк при одинаковом occurred_at в конце батча.
  - НЕ LISTEN/NOTIFY: Neon pooler (PgBouncer transaction-mode) несовместим.
    (feedback_neon_pooler_listen_notify.md)
  - pg_try_advisory_lock: enforcement single-replica (Railway redeploy, scaling).
  - L2-PRIVACY: каждый SELECT — явный WHERE account_id (BYPASSRLS + explicit filter).

Env vars:
  LEARNING_URL                 — DSN к learning DB (обязательно)
  INDICATORS_URL               — DSN к indicators Neon DB (для UPDATE digital twin)
  PERSONA_URL                  — DSN к persona Neon DB (для telegram_id lookup)
  AIST_BOT_TOKEN               — Telegram bot token (для уведомлений)
  STAGE_LISTENER_POLL_INTERVAL — интервал polling в секундах (default: 60)
  STAGE_LISTENER_BATCH         — размер батча (default: 100)

Запуск:
  python runner.py stage-transition-listener
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid as _uuid_module
from datetime import datetime, timezone

import aiohttp
import asyncpg

from activity_hub.core.health_metrics import write_internal_metric
from activity_hub.core.stage_config import (
    STAGE_GATE_MATRIX,
    TARGET_HOURS_PER_STAGE as _TARGET_HOURS,
    STAGE_NAMES_RU as _STAGE_NAMES_RU,
    STAGE_IDS as _STAGE_IDS,
)

log = logging.getLogger(__name__)

WORKER_NAME = "stage-transition-listener"
POLL_INTERVAL_SEC = float(os.environ.get("STAGE_LISTENER_POLL_INTERVAL", "60"))
BATCH_SIZE = int(os.environ.get("STAGE_LISTENER_BATCH", "100"))

# pg_try_advisory_lock key — уникальный per-worker (не пересекается с profiler 920109001).
_ADVISORY_LOCK_KEY = 920310001

# Порог лага (сек) — перечисленные в health.internal_metrics
_LAG_ALERT_SECONDS = 1800  # Alert 2: неотработанный переход > 30 мин

# Optional downstream env vars (при отсутствии — stub с предупреждением)
_INDICATORS_URL = os.environ.get("INDICATORS_URL")
_PERSONA_URL = os.environ.get("PERSONA_URL")
_BOT_TOKEN = os.environ.get("AIST_BOT_TOKEN")

# Sentinel UUID для инициализации курсора (нет "нулевого" id у stage_transitions)
_NIL_UUID = _uuid_module.UUID(int=0)


# ---------------------------------------------------------------------------
# Cursor management (self-bootstrapping + keyset pagination)
# ---------------------------------------------------------------------------

async def _init_cursor(conn: asyncpg.Connection) -> tuple[datetime, _uuid_module.UUID]:
    """Создать таблицу курсора и вернуть (last_seen_at, last_seen_id).

    Keyset cursor по (occurred_at, id) — защищает от потери строк при
    одинаковом occurred_at в конце батча (vs single-column high-water mark).
    ADD COLUMN IF NOT EXISTS мигрирует существующую таблицу из Ф2 (только last_seen_at).
    """
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS public.stage_listener_cursor (
            worker_name  TEXT        PRIMARY KEY,
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01'::timestamptz,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    # Ф2→Ф3 миграция: добавить last_seen_id если ещё нет
    await conn.execute(
        """
        ALTER TABLE public.stage_listener_cursor
        ADD COLUMN IF NOT EXISTS
            last_seen_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000'::uuid
        """
    )

    row = await conn.fetchrow(
        "SELECT last_seen_at, last_seen_id FROM public.stage_listener_cursor WHERE worker_name = $1",
        WORKER_NAME,
    )
    if row is None:
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        await conn.execute(
            """
            INSERT INTO public.stage_listener_cursor (worker_name, last_seen_at, last_seen_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (worker_name) DO NOTHING
            """,
            WORKER_NAME, epoch, _NIL_UUID,
        )
        return epoch, _NIL_UUID
    return row["last_seen_at"], row["last_seen_id"]


async def _advance_cursor(
    conn: asyncpg.Connection,
    new_seen_at: datetime,
    new_seen_id: _uuid_module.UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO public.stage_listener_cursor (worker_name, last_seen_at, last_seen_id, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (worker_name) DO UPDATE
            SET last_seen_at = EXCLUDED.last_seen_at,
                last_seen_id = EXCLUDED.last_seen_id,
                updated_at   = EXCLUDED.updated_at
        WHERE (stage_listener_cursor.last_seen_at, stage_listener_cursor.last_seen_id)
            < (EXCLUDED.last_seen_at, EXCLUDED.last_seen_id)
        """,
        WORKER_NAME, new_seen_at, new_seen_id,
    )


# ---------------------------------------------------------------------------
# Downstream dispatch (Ф3: реальные вызовы; Портной — stub до WP-309)
# ---------------------------------------------------------------------------

async def _update_digital_twin_stage(
    account_id: str, to_stage: int, conn: asyncpg.Connection
) -> None:
    """Ф3: обновить 3_derived.3_4_qualification в indicators.calculated_profile."""
    if not _INDICATORS_URL:
        log.info(
            "[STUB] digital_twin UPDATE skipped: account=%s stage_id=%s (INDICATORS_URL not set)",
            account_id[:8], _STAGE_IDS.get(to_stage),
        )
        return

    stage_payload = json.dumps({
        "stage": to_stage - 1,       # 0-indexed, совместимо с dt_calc.py STAGE_NAMES
        "stage_id": _STAGE_IDS.get(to_stage, "STG.Student.Unknown"),
        "stage_name_ru": _STAGE_NAMES_RU.get(to_stage, ""),
        "path": "student",
        "updated_by": "stage-transition-listener",
    })

    ind_conn = await asyncpg.connect(_INDICATORS_URL, statement_cache_size=0)
    try:
        await ind_conn.execute(
            """
            INSERT INTO public.calculated_profile
                (account_id, indicators, last_recalc_at, source_version)
            VALUES (
                $1::uuid,
                jsonb_set('{}'::jsonb, '{3_derived,3_4_qualification}', $2::jsonb, true),
                NOW(),
                'stage-transition-listener'
            )
            ON CONFLICT (account_id) DO UPDATE
            SET indicators = jsonb_set(
                    COALESCE(public.calculated_profile.indicators, '{}'::jsonb),
                    '{3_derived,3_4_qualification}',
                    $2::jsonb,
                    true
                ),
                last_recalc_at = NOW(),
                source_version  = EXCLUDED.source_version
            """,
            account_id, stage_payload,
        )
        log.info(
            "digital_twin updated: account=%s stage_id=%s",
            account_id[:8], _STAGE_IDS.get(to_stage),
        )
    finally:
        await ind_conn.close()


def _format_evidence_table(evidence: dict, next_stage: int) -> str:
    """Ф-Д (WP-310): таблица нормативов в формате В.

    evidence: {"s": 0-5, "t": 0-5, "m": 0-5, "w": 0-5, "a": 0-5, "total_hours": N, ...}
    next_stage: следующая ступень (to_stage + 1, или None если ст.5)
    Возвращает отформатированную строку для TG HTML (parse_mode=HTML).
    """
    _LABELS = {"s": "Систем.", "t": "Инвест.", "m": "Метод.", "w": "Системн.", "a": "Агентн."}
    _KEYS = ("s", "t", "m", "w", "a")

    s = evidence.get("s", 0)
    t = evidence.get("t", 0)
    m = evidence.get("m", 0)
    w = evidence.get("w", 0)
    a = evidence.get("a", 0)
    total_hours = evidence.get("total_hours", 0)
    current = {"s": s, "t": t, "m": m, "w": w, "a": a}

    if next_stage and next_stage <= 5:
        gate = STAGE_GATE_MATRIX.get(next_stage, (0, 0, 0, 0, 0))
        thresh = dict(zip(_KEYS, gate))
        header = f"{'Хар-ка':<10} {'Сейчас':>6}  {'→Ст.' + str(next_stage):>6}"
        rows = [header, "-" * len(header)]
        for key in _KEYS:
            val = current[key]
            thr = thresh.get(key, 0)
            ok = "✅" if (thr == 0 or val >= thr) else "❌"
            thr_str = f"≥{thr}" if thr else " —"
            rows.append(f"{_LABELS[key]:<10} {val:>6}  {thr_str:>6}  {ok}")
    else:
        header = f"{'Хар-ка':<10} {'Индекс':>6}"
        rows = [header, "-" * len(header)]
        for key in ("s", "t", "m", "w", "a"):
            rows.append(f"{_LABELS[key]:<10} {current[key]:>6}")

    if total_hours:
        rows.append(f"Часов всего: {int(total_hours)}")

    return "<code>" + "\n".join(rows) + "</code>"


async def _notify_pilot(
    account_id: str, from_stage: int, to_stage: int, evidence: dict | None = None
) -> None:
    """Ф3: уведомление пилоту через Telegram Bot API.

    Lookup telegram_id из persona.ory_identity (L2-PRIVACY: explicit WHERE).
    Сообщение включает новую ступень, таблицу нормативов (Ф-Д, WP-310) и целевой ритм.
    """
    if not _BOT_TOKEN:
        log.info(
            "[STUB] bot notify skipped: account=%s %d→%d (AIST_BOT_TOKEN not set)",
            account_id[:8], from_stage, to_stage,
        )
        return

    if not _PERSONA_URL:
        log.info(
            "[STUB] bot notify skipped: account=%s %d→%d (PERSONA_URL not set)",
            account_id[:8], from_stage, to_stage,
        )
        return

    # L2-PRIVACY: явный WHERE account_id; asyncpg принимает str для UUID-колонок
    persona_conn = await asyncpg.connect(_PERSONA_URL, statement_cache_size=0)
    try:
        row = await persona_conn.fetchrow(
            "SELECT telegram_id FROM ory_identity WHERE account_id = $1",
            account_id,
        )
    finally:
        await persona_conn.close()

    if row is None or row["telegram_id"] is None:
        log.warning("notify_pilot: no telegram_id for account=%s — skipping", account_id[:8])
        return

    telegram_id = row["telegram_id"]
    stage_name = _STAGE_NAMES_RU.get(to_stage, f"ступень {to_stage}")
    target_hours = _TARGET_HOURS.get(to_stage, 4)
    next_stage = to_stage + 1 if to_stage < 5 else None

    table_block = ""
    if evidence:
        table_block = "\n\n" + _format_evidence_table(evidence, next_stage)

    text = (
        f"🎓 Вы достигли <b>{to_stage} ступени</b> — {stage_name}!"
        f"{table_block}\n\n"
        f"Целевой ритм: <b>{target_hours} ч/нед</b>\n"
        f"Персональный план скоро обновится под вашу ступень."
    )

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage",
            json={"chat_id": telegram_id, "text": text, "parse_mode": "HTML"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status == 200:
                log.info(
                    "bot notify sent: account=%s stage=%d",
                    account_id[:8], to_stage,
                )
            else:
                body = await resp.text()
                log.error(
                    "bot notify failed: account=%s status=%d body=%s",
                    account_id[:8], resp.status, body[:200],
                )


async def _trigger_personal_guide_render(
    account_id: str, to_stage: int, conn: asyncpg.Connection
) -> None:
    """Ф7 (WP-309): INSERT в learning.guide_render_queue → render-pilot-guides подхватит.

    render-pilot-guides.py запускается каждые 10 мин (--queue-only systemd-timer).
    При stage_transition используется mode='weekly' — пилот получает все 6 файлов
    с нарративом под новую ступень.
    """
    await conn.execute(
        """
        INSERT INTO learning.guide_render_queue (account_id, trigger_type, trigger_payload)
        VALUES ($1::uuid, 'stage_transition', $2::jsonb)
        """,
        account_id,
        json.dumps({"to_stage": to_stage}),
    )
    log.info(
        "guide render enqueued: account=%s stage=%d",
        account_id[:8], to_stage,
    )


async def _update_weekly_rhythm(account_id: str, to_stage: int) -> None:
    """Stub: ритм включён в уведомление пилоту; файловое обновление — WP-310 Ф8."""
    target_hours = _TARGET_HOURS.get(to_stage, 4)
    log.info(
        "[STUB] rhythm update: account=%s stage=%d target=%dh/wk (folded into bot notify)",
        account_id[:8], to_stage, target_hours,
    )


async def _dispatch_downstream(
    row: asyncpg.Record,
    conn: asyncpg.Connection,
) -> None:
    """Запустить все downstream-эффекты для одного перехода."""
    account_id = str(row["account_id"])
    from_stage = int(row["from_stage"])
    to_stage = int(row["to_stage"])
    evidence: dict | None = dict(row["evidence"]) if row["evidence"] else None

    log.info(
        "dispatch: account=%s transition %d→%d triggered_by=%s occurred_at=%s",
        account_id[:8], from_stage, to_stage,
        row["triggered_by"], row["occurred_at"].isoformat(),
    )

    await _update_digital_twin_stage(account_id, to_stage, conn)
    await _notify_pilot(account_id, from_stage, to_stage, evidence)
    await _trigger_personal_guide_render(account_id, to_stage, conn)
    await _update_weekly_rhythm(account_id, to_stage)


# ---------------------------------------------------------------------------
# Polling batch (keyset pagination по (occurred_at, id))
# ---------------------------------------------------------------------------

async def _run_batch(
    conn: asyncpg.Connection,
    last_seen_at: datetime,
    last_seen_id: _uuid_module.UUID,
) -> int:
    """Один инкрементальный батч: прочитать новые stage_transitions → dispatch.

    Keyset pagination по (occurred_at, id) — корректен при одинаковом occurred_at.
    Возвращает количество обработанных переходов.
    Останавливается на первой ошибке dispatch и двигает cursor только до
    последней успешно обработанной строки — гарантирует retry при следующем poll.
    """
    rows = await conn.fetch(
        """
        SELECT id, account_id, from_stage, to_stage, triggered_by, occurred_at, evidence
        FROM learning.stage_transitions
        WHERE (occurred_at, id) > ($1, $2::uuid)
        ORDER BY occurred_at, id
        LIMIT $3
        """,
        last_seen_at,
        last_seen_id,
        BATCH_SIZE,
    )

    if not rows:
        return 0

    last_success_at: datetime | None = None
    last_success_id: _uuid_module.UUID | None = None
    processed = 0

    for row in rows:
        try:
            await _dispatch_downstream(row, conn)
            last_success_at = row["occurred_at"]
            last_success_id = row["id"]
            processed += 1
        except Exception as exc:
            log.error(
                "dispatch failed: account=%s transition %d→%d: %s — stopping batch, retry next poll",
                str(row["account_id"])[:8],
                row["from_stage"], row["to_stage"], exc,
            )
            break  # cursor не двигается дальше сломанной строки

    if last_success_at is not None and last_success_id is not None:
        await _advance_cursor(conn, last_success_at, last_success_id)
        log.info(
            "batch done: processed=%d/%d cursor→(%s, %s)",
            processed, len(rows),
            last_success_at.isoformat(), str(last_success_id)[:8],
        )
    return processed


# ---------------------------------------------------------------------------
# Alert 2: lag check (WP-310 Ф8)
# ---------------------------------------------------------------------------

async def _check_dispatch_lag(
    conn: asyncpg.Connection,
    last_seen_at: datetime,
    last_seen_id: "_uuid_module.UUID",
) -> None:
    """Alert 2: если oldest unprocessed transition > 30 мин — метрика + critical log.

    Пишет stage_listener_lag_seconds в health.internal_metrics (best-effort).
    Alerter читает freshness этой метрики и сравнивает value_numeric с порогом.
    """
    try:
        lag_sec = await conn.fetchval(
            """
            SELECT EXTRACT(EPOCH FROM (NOW() - MIN(occurred_at)))
            FROM learning.stage_transitions
            WHERE (occurred_at, id) > ($1, $2::uuid)
            """,
            last_seen_at, last_seen_id,
        )
    except Exception as exc:
        log.warning("lag_check query failed: %s", exc)
        return

    if lag_sec is None:
        return  # нет необработанных переходов

    lag_sec = float(lag_sec)
    if lag_sec > _LAG_ALERT_SECONDS:
        log.critical(
            "stage-transition-listener: STUCK! oldest unprocessed transition is %.0f s old "
            "(threshold=%d s) — dispatch pipeline may be broken",
            lag_sec, _LAG_ALERT_SECONDS,
        )

    try:
        await write_internal_metric(
            conn,
            metric_name="stage_listener_lag_seconds",
            worker=WORKER_NAME,
            value_numeric=lag_sec,
            value_jsonb={"lag_seconds": lag_sec, "alert": lag_sec > _LAG_ALERT_SECONDS},
        )
    except Exception as exc:
        log.warning("lag metric write failed: %s", exc)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run_stage_transition_listener(learning_dsn: str) -> None:
    """Главный entrypoint: бесконечный polling loop.

    Использует pg_try_advisory_lock для enforcement single-replica.
    При потере блокировки (Railway redeploy) — следующий инстанс подберёт cursor.
    """
    conn = await asyncpg.connect(learning_dsn, statement_cache_size=0)
    try:
        # Retry lock up to 90s: during Railway redeploy the old container takes
        # 30-60s to be killed (releasing the session-scoped advisory lock).
        # Immediate exit causes workers-neon gather to complete → container stops
        # with exit 0 → Railway ON_FAILURE policy does NOT restart → workers dead.
        locked = False
        for _attempt in range(9):
            locked = await conn.fetchval(
                "SELECT pg_try_advisory_lock($1)", _ADVISORY_LOCK_KEY
            )
            if locked:
                break
            log.info(
                "stage-transition-listener: advisory lock %d held — retrying in 10s (attempt %d/9)",
                _ADVISORY_LOCK_KEY, _attempt + 1,
            )
            await asyncio.sleep(10)
        if not locked:
            log.warning(
                "stage-transition-listener: advisory lock %d not available after 90s — exiting",
                _ADVISORY_LOCK_KEY,
            )
            return

        last_seen_at, last_seen_id = await _init_cursor(conn)
        log.info(
            "stage-transition-listener started: cursor=(%s, %s) poll_interval=%.0fs batch=%d "
            "indicators=%s persona=%s bot=%s",
            last_seen_at.isoformat(), str(last_seen_id)[:8],
            POLL_INTERVAL_SEC, BATCH_SIZE,
            "configured" if _INDICATORS_URL else "stub",
            "configured" if _PERSONA_URL else "stub",
            "configured" if _BOT_TOKEN else "stub",
        )

        while True:
            try:
                n = await _run_batch(conn, last_seen_at, last_seen_id)
                if n > 0:
                    row = await conn.fetchrow(
                        "SELECT last_seen_at, last_seen_id FROM public.stage_listener_cursor WHERE worker_name = $1",
                        WORKER_NAME,
                    )
                    last_seen_at = row["last_seen_at"]
                    last_seen_id = row["last_seen_id"]
                    log.info("stage-transition-listener: processed %d transitions", n)

                # Alert 2 (WP-310 Ф8): неотработанный переход > 30 мин
                await _check_dispatch_lag(conn, last_seen_at, last_seen_id)
            except Exception as exc:
                log.error("stage-transition-listener batch error: %s", exc)

            await asyncio.sleep(POLL_INTERVAL_SEC)
    finally:
        await conn.close()
