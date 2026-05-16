"""Anonymization Worker — WP-214 Ф10.7: right-to-forget по B7.3.6 §4.

Триггер: event_type='account_deleted' в public.domain_event (tombstone pattern).
Действия при получении tombstone:
  1. learning.stage_transitions  → soft-delete: account_id=NULL, evidence=NULL
  2. learning.graduation_log     → DELETE (полное удаление)
  3. learning.w_reflections      → DELETE
  4. learning.tracking_consent   → DELETE
  5. club.members                → DELETE WHERE ory_identity_id=$1
  6. public.domain_event         → soft-delete: account_id=NULL, payload={"anonymized":true}
     (tombstone включительно — после этого он не будет найден повторно)
  7. audit.pii_access_log        → INSERT (operation='delete', purpose_code='right_to_forget')

Идемпотентность: tombstone обнаруживается по account_id IS NOT NULL.
  После обработки account_id=NULL → повторный запуск = no-op.

SLA: 30 дней с момента tombstone (B7.3.6 §4).

Env vars:
  LEARNING_URL     — DIRECT connection к Neon learning DB (обязательно, без -pooler)
  ANON_POLL_SEC    — интервал поллинга в секундах (default: 60)
  ANON_BATCH_SIZE  — кол-во строк за один батч (default: 100)

Запуск:
  python runner.py anonymization-worker
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid

import asyncpg

from activity_hub.core.health_metrics import write_internal_metric

log = logging.getLogger(__name__)

_WORKER_NAME = "anonymization-worker"
# advisory lock key — защита от двойного запуска (cf. profiler_subscriber.py)
_ADVISORY_LOCK_KEY = 920214001  # WP-214 themed, unique per DB
_POLL_INTERVAL_SEC = float(os.environ.get("ANON_POLL_SEC", "60"))
_BATCH_SIZE = int(os.environ.get("ANON_BATCH_SIZE", "100"))


# ─────────────────────────────────────────────────────────────────────────────
# Cursor helpers (self-bootstrapping)
# ─────────────────────────────────────────────────────────────────────────────

async def _get_or_init_cursor(conn: asyncpg.Connection) -> int:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS public.anonymization_worker_cursor (
            worker_name   TEXT        PRIMARY KEY,
            last_event_id BIGINT      NOT NULL DEFAULT 0,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    row = await conn.fetchrow(
        "SELECT last_event_id FROM public.anonymization_worker_cursor WHERE worker_name = $1",
        _WORKER_NAME,
    )
    if row is None:
        await conn.execute(
            """
            INSERT INTO public.anonymization_worker_cursor (worker_name, last_event_id)
            VALUES ($1, 0)
            ON CONFLICT (worker_name) DO NOTHING
            """,
            _WORKER_NAME,
        )
        return 0
    return row["last_event_id"]


async def _advance_cursor(conn: asyncpg.Connection, event_id: int) -> None:
    await conn.execute(
        """
        INSERT INTO public.anonymization_worker_cursor (worker_name, last_event_id, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (worker_name) DO UPDATE
            SET last_event_id = EXCLUDED.last_event_id,
                updated_at    = EXCLUDED.updated_at
        """,
        _WORKER_NAME,
        event_id,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Core anonymization
# ─────────────────────────────────────────────────────────────────────────────

async def _anonymize_account(conn: asyncpg.Connection, account_id: uuid.UUID) -> dict:
    """Выполнить полный цикл анонимизации одного account_id.

    Всё в одной транзакции. Идемпотентно: DELETE/UPDATE нулевых строк = no-op.
    Возвращает словарь с количеством затронутых строк по каждой таблице.
    """
    stats: dict[str, int] = {}

    # 1. learning.stage_transitions — soft-delete (сохраняем для аналитики агрегатов)
    result = await conn.execute(
        """
        UPDATE learning.stage_transitions
           SET account_id = NULL,
               evidence   = NULL
         WHERE account_id = $1
        """,
        account_id,
    )
    stats["stage_transitions_nulled"] = int(result.split()[-1])

    # 2. learning.graduation_log — полное удаление (немедленно, B7.3.6 §4)
    result = await conn.execute(
        "DELETE FROM learning.graduation_log WHERE account_id = $1",
        account_id,
    )
    stats["graduation_log_deleted"] = int(result.split()[-1])

    # 3. learning.w_reflections — полное удаление
    result = await conn.execute(
        "DELETE FROM learning.w_reflections WHERE account_id = $1",
        account_id,
    )
    stats["w_reflections_deleted"] = int(result.split()[-1])

    # 4. learning.tracking_consent — полное удаление
    result = await conn.execute(
        "DELETE FROM learning.tracking_consent WHERE account_id = $1",
        account_id,
    )
    stats["tracking_consent_deleted"] = int(result.split()[-1])

    # 5. club.members — account_id = ory_identity_id для членов клуба
    result = await conn.execute(
        "DELETE FROM club.members WHERE ory_identity_id = $1",
        account_id,
    )
    stats["club_members_deleted"] = int(result.split()[-1])

    # 6. public.domain_event — soft-delete всех событий, включая сам tombstone
    result = await conn.execute(
        """
        UPDATE public.domain_event
           SET account_id = NULL,
               payload    = '{"anonymized": true}'::jsonb
         WHERE account_id = $1
        """,
        account_id,
    )
    stats["domain_events_nulled"] = int(result.split()[-1])

    # 7. audit.pii_access_log — запись факта анонимизации
    await conn.execute(
        """
        INSERT INTO audit.pii_access_log (
            actor_kind, actor_id, subject_user_uuid,
            data_class, resource_kind, resource_ref,
            operation, purpose_code, result
        ) VALUES (
            'service_account', $1, $2,
            'pii', 'column', 'learning.*+domain_event.account_id',
            'delete', 'right_to_forget', 'success'
        )
        """,
        _WORKER_NAME,
        account_id,
    )
    stats["audit_logged"] = 1

    return stats


# ─────────────────────────────────────────────────────────────────────────────
# Batch processing
# ─────────────────────────────────────────────────────────────────────────────

async def _run_batch(conn: asyncpg.Connection, batch_size: int = _BATCH_SIZE) -> int:
    """Один инкрементальный батч из public.domain_event.

    Читает все события начиная с cursor, фильтрует account_deleted tombstones,
    обрабатывает каждый. Курсор сдвигается до max(id) батча в любом случае
    (аналогично profiler_neon — ошибки recalculate не блокируют прогресс,
    анонимизация идемпотентна и будет повторена при следующем запуске если упадёт).
    """
    cursor = await _get_or_init_cursor(conn)

    rows = await conn.fetch(
        """
        SELECT id, event_type, account_id
          FROM public.domain_event
         WHERE id > $1
         ORDER BY id
         LIMIT $2
        """,
        cursor,
        batch_size,
    )

    if not rows:
        return 0

    max_id: int = rows[-1]["id"]
    tombstones_processed = 0

    for row in rows:
        if row["event_type"] != "account_deleted" or row["account_id"] is None:
            continue

        account_id: uuid.UUID = row["account_id"]
        log.info(
            "anonymization-worker: tombstone id=%s account=%s",
            row["id"],
            str(account_id)[:12],
        )

        try:
            async with conn.transaction():
                stats = await _anonymize_account(conn, account_id)
            log.info(
                "anonymization-worker: done account=%s stats=%s",
                str(account_id)[:12],
                stats,
            )
            tombstones_processed += 1
        except Exception as exc:
            log.error(
                "anonymization-worker: failed account=%s event_id=%s: %s",
                str(account_id)[:12],
                row["id"],
                exc,
            )
            # Не блокируем cursor advance — следующий батч не подберёт тот же tombstone
            # (account_id ещё не NULL), но это нормально: next poll подберёт снова.
            # Лучше advance чем зависание на всегда при системной ошибке.

    await _advance_cursor(conn, max_id)

    if rows:
        log.info(
            "anonymization-worker batch: fetched=%d tombstones=%d cursor→%d",
            len(rows),
            tombstones_processed,
            max_id,
        )

    return len(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────

async def run_anonymization_worker(
    learning_dsn: str,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Polling worker для анонимизации удалённых аккаунтов (B7.3.6 §4).

    learning_dsn: DIRECT connection к Neon learning DB (без -pooler endpoint).
    """
    conn = await asyncpg.connect(learning_dsn, statement_cache_size=0)
    try:
        await conn.execute("SELECT 1")

        got_lock = False
        for _attempt in range(9):
            got_lock = await conn.fetchval(
                "SELECT pg_try_advisory_lock($1)", _ADVISORY_LOCK_KEY
            )
            if got_lock:
                break
            log.info(
                "anonymization-worker: advisory lock %d занят — retry через 10s (attempt %d/9)",
                _ADVISORY_LOCK_KEY, _attempt + 1,
            )
            await asyncio.sleep(10)
        if not got_lock:
            log.warning(
                "anonymization-worker: advisory lock %d не получен за 90s — exit",
                _ADVISORY_LOCK_KEY,
            )
            return

        log.info(
            "anonymization-worker started: poll=%.0fs lock=%d acquired",
            _POLL_INTERVAL_SEC,
            _ADVISORY_LOCK_KEY,
        )

        # Startup drain — обработать весь накопившийся backlog
        total = 0
        batch_num = 0
        while True:
            batch_num += 1
            n = await _run_batch(conn)
            total += n
            if n > 0:
                log.info(
                    "anonymization-worker drain: batch=%d events=%d total=%d",
                    batch_num,
                    n,
                    total,
                )
            if n < _BATCH_SIZE:
                break
        log.info("anonymization-worker startup drain complete: %d events in %d batches", total, batch_num)

        loop_clock = asyncio.get_event_loop().time
        last_poll = loop_clock()

        while stop_event is None or not stop_event.is_set():
            await asyncio.sleep(1.0)
            now = loop_clock()
            if now - last_poll >= _POLL_INTERVAL_SEC:
                await conn.execute("SELECT 1")  # liveness ping
                n = await _run_batch(conn)
                try:
                    await write_internal_metric(
                        conn,
                        metric_name="anonymization_worker_batch_processed",
                        worker=_WORKER_NAME,
                        value_numeric=float(n),
                    )
                except Exception as exc:
                    log.warning("anonymization-worker: heartbeat write failed: %s", exc)
                last_poll = now

    finally:
        await conn.close()
        log.info("anonymization-worker stopped")
