"""Profiler Subscriber — WP-109 Ф9: Event-driven trigger для профайлера.

LISTEN activity_event_ingested → фильтр по event_type → recalculate_derived per-user.

Архитектура:
  - Прямой LISTEN на activity_event_ingested (DIRECT connection, без pooler).
  - При получении события с нужным event_type — trigger recalculate_derived(user_id).
  - recalculate_derived использует psycopg2 (sync) → запускается в thread executor.
  - Per-worker cursor в operations.worker_cursors (replay при рестарте).
  - Retry-loop: при ошибке recalculate — cursor не advance, повтор на replay.

Env vars:
  ACTIVITY_HUB_DIRECT_URL — DIRECT connection к activity-hub DB (обязательно, без -pooler.)
  DT_PROFILER_NEON_URL    — connection к digital_twins DB (обязательно)
  DT_PROFILER_LEARNING_URL — connection к learning DB (опционально, для IND.3.2.04)
  DT_PROFILER_INDICATORS_URL — connection к indicators DB для F2 dual-write (опционально)

Запуск:
  python runner.py profiler-subscriber
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import asyncpg

from activity_hub.core.health_metrics import write_internal_metric

log = logging.getLogger(__name__)

CHANNEL = "activity_event_ingested"
WORKER_NAME = "profiler-subscriber"
REPLAY_INTERVAL_SEC = float(os.environ.get("PROFILER_SUB_REPLAY_INTERVAL", "120"))
REPLAY_BATCH = int(os.environ.get("PROFILER_SUB_REPLAY_BATCH", "200"))

# event_type, которые триггерят пересчёт профиля (WP-109 Ф9)
TRIGGER_EVENT_TYPES = frozenset({
    "coding_time",
    "commit_created",
    "learning_session",
    "pomodoro_completed",
    "section_completed",
    "test_passed",
    "day_open",
    "day_close",
    "wp_completed",
})


def _import_recalculate():
    """Lazy import recalculate_all из vendored profiler (psycopg2, sync).

    Vendored из DS-ai-systems/profiler/scripts/ в activity_hub/profiler/.
    При обновлении оригинала — синхронизировать вручную.
    """
    from activity_hub.profiler.recalculate_derived import recalculate_all  # noqa: PLC0415
    return recalculate_all


def _run_recalculate(user_id: str, neon_url: str, learning_url: str | None, indicators_url: str | None) -> dict:
    """Запустить пересчёт для одного пользователя (sync, в thread executor).

    Устанавливает INDICATORS_URL перед запуском для F2 dual-write.
    """
    if indicators_url:
        os.environ["INDICATORS_URL"] = indicators_url
    recalculate_all = _import_recalculate()
    return recalculate_all(neon_url, user_id_filter=user_id, learning_url=learning_url)


async def _get_cursor(conn: asyncpg.Connection) -> int:
    """Получить last_outbox_id из worker_cursors (идемпотентный INSERT при первом запуске)."""
    await conn.execute(
        """
        INSERT INTO operations.worker_cursors (worker_name, last_outbox_id)
        VALUES ($1, 0)
        ON CONFLICT (worker_name) DO NOTHING
        """,
        WORKER_NAME,
    )
    return await conn.fetchval(
        "SELECT last_outbox_id FROM operations.worker_cursors WHERE worker_name = $1",
        WORKER_NAME,
    )


async def _advance_cursor(conn: asyncpg.Connection, outbox_id: int) -> None:
    await conn.execute(
        """
        UPDATE operations.worker_cursors
        SET last_outbox_id = $2, updated_at = NOW()
        WHERE worker_name = $1 AND last_outbox_id < $2
        """,
        WORKER_NAME,
        outbox_id,
    )


async def _process_outbox_row(
    row: asyncpg.Record,
    executor: concurrent.futures.ThreadPoolExecutor,
    neon_url: str,
    learning_url: str | None,
    indicators_url: str | None,
    conn: asyncpg.Connection,
) -> bool:
    """Обработать одну запись outbox. Возвращает True если успешно."""
    if row["event_type"] not in TRIGGER_EVENT_TYPES:
        # Не наш event — молча advance cursor
        await _advance_cursor(conn, row["id"])
        return True

    user_id = str(row["user_uuid"])
    log.info(
        "profiler trigger: outbox_id=%s event_type=%s user=%s",
        row["id"],
        row["event_type"],
        user_id[:12],
    )

    loop = asyncio.get_event_loop()
    try:
        stats = await loop.run_in_executor(
            executor,
            _run_recalculate,
            user_id,
            neon_url,
            learning_url,
            indicators_url,
        )
        log.info(
            "profiler done: outbox_id=%s user=%s recalculated=%s errors=%s",
            row["id"],
            user_id[:12],
            stats.get("recalculated"),
            stats.get("errors"),
        )
        if stats.get("errors", 0) > 0:
            log.warning("profiler partial error for user=%s — cursor NOT advanced (retry)", user_id[:12])
            return False
        await _advance_cursor(conn, row["id"])
        return True
    except Exception as e:
        log.error("profiler recalculate failed: outbox_id=%s user=%s: %s", row["id"], user_id[:12], e)
        return False


async def _replay_from_cursor(
    conn: asyncpg.Connection,
    executor: concurrent.futures.ThreadPoolExecutor,
    neon_url: str,
    learning_url: str | None,
    indicators_url: str | None,
    limit: int = REPLAY_BATCH,
) -> int:
    """Catch-up replay начиная с cursor (recovery при рестарте)."""
    cursor = await _get_cursor(conn)
    rows = await conn.fetch(
        """
        SELECT id, event_type, event_class, user_uuid, payload
        FROM operations.platform_outbox
        WHERE id > $1
        ORDER BY id
        LIMIT $2
        """,
        cursor,
        limit,
    )
    processed = 0
    for row in rows:
        ok = await _process_outbox_row(row, executor, neon_url, learning_url, indicators_url, conn)
        if ok:
            processed += 1
        else:
            # При failure: остановить replay, cursor не advance — retry при следующем run
            log.warning("replay stopped at outbox_id=%s due to failure", row["id"])
            break
    if rows:
        log.info("replay: processed %d/%d outbox rows (cursor was %d)", processed, len(rows), cursor)
    return processed


# ---------------------------------------------------------------------------
# Gap F3+: Neon pipeline — polling loop после WP-268 cut-over.
# После cut-over новые события пилотов идут в learning.domain_event (Neon),
# LISTEN/NOTIFY Railway больше не триггерит профайлер для них.
# ---------------------------------------------------------------------------

_NEON_WORKER_NAME = "profiler-subscriber-neon"
NEON_POLLING_INTERVAL_SEC = float(os.environ.get("PROFILER_NEON_POLL_INTERVAL", "30"))
NEON_BATCH_SIZE = int(os.environ.get("PROFILER_NEON_BATCH", "500"))
# pg_advisory_lock key — защита от двойного запуска (Railway redeploy, scaling).
# Фиксированный integer-ключ (стабильный, не зависит от PYTHONHASHSEED).
# Уникален в пределах БД; для points-worker-neon — другой ключ (см. points_subscriber.py).
_NEON_ADVISORY_LOCK_KEY = 920109001  # arbitrary unique constant for profiler-neon

# Threshold для warn-лога при высокой доле ошибок recalculate (per-batch).
_NEON_ERROR_RATE_WARN = float(os.environ.get("PROFILER_NEON_ERROR_RATE_WARN", "0.5"))


async def _get_or_init_cursor_neon(learning_conn: asyncpg.Connection) -> int:
    """Получить курсор из self-bootstrapping таблицы в learning DB."""
    await learning_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS public.profiler_domain_event_cursor (
            worker_name   TEXT        PRIMARY KEY,
            last_event_id BIGINT      NOT NULL DEFAULT 0,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    row = await learning_conn.fetchrow(
        "SELECT last_event_id FROM public.profiler_domain_event_cursor WHERE worker_name = $1",
        _NEON_WORKER_NAME,
    )
    if row is None:
        await learning_conn.execute(
            """
            INSERT INTO public.profiler_domain_event_cursor (worker_name, last_event_id)
            VALUES ($1, 0)
            ON CONFLICT (worker_name) DO NOTHING
            """,
            _NEON_WORKER_NAME,
        )
        return 0
    return row["last_event_id"]


async def _advance_cursor_neon(learning_conn: asyncpg.Connection, event_id: int) -> None:
    await learning_conn.execute(
        """
        INSERT INTO public.profiler_domain_event_cursor (worker_name, last_event_id, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (worker_name) DO UPDATE
            SET last_event_id = EXCLUDED.last_event_id,
                updated_at    = EXCLUDED.updated_at
        """,
        _NEON_WORKER_NAME,
        event_id,
    )


async def _run_profiler_batch_neon(
    learning_conn: asyncpg.Connection,
    executor: concurrent.futures.ThreadPoolExecutor,
    neon_url: str,
    learning_url: str | None,
    indicators_url: str | None,
    batch_size: int = NEON_BATCH_SIZE,
) -> int:
    """Один инкрементальный батч профайлера из public.domain_event.

    Читает ВСЕ event_types (чтобы cursor двигался равномерно), фильтрует
    TRIGGER_EVENT_TYPES на стороне Python, дедуплицирует по account_id.
    Cursor advance: всегда до max(id) в батче, даже при ошибках
    (рекалькуляция идемпотентна — периодический полл исправит).
    """
    cursor = await _get_or_init_cursor_neon(learning_conn)

    rows = await learning_conn.fetch(
        """
        SELECT id, event_type, account_id AS user_uuid
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

    max_event_id: int = rows[-1]["id"]

    # Один recalculate на пользователя за батч
    trigger_users: set[str] = set()
    for row in rows:
        if row["event_type"] in TRIGGER_EVENT_TYPES and row["user_uuid"] is not None:
            trigger_users.add(str(row["user_uuid"]))

    loop = asyncio.get_event_loop()
    recalculated = 0
    errors = 0
    for user_id in trigger_users:
        log.info(
            "profiler-neon: user=%s trigger in batch [cursor=%d..%d]",
            user_id[:12],
            cursor,
            max_event_id,
        )
        try:
            stats = await loop.run_in_executor(
                executor,
                _run_recalculate,
                user_id,
                neon_url,
                learning_url,
                indicators_url,
            )
            if stats.get("errors", 0) > 0:
                errors += 1
                log.warning(
                    "profiler-neon partial error: user=%s errors=%s",
                    user_id[:12],
                    stats.get("errors"),
                )
            else:
                recalculated += 1
                log.debug(
                    "profiler-neon done: user=%s recalculated=%s",
                    user_id[:12],
                    stats.get("recalculated"),
                )
        except Exception as exc:
            errors += 1
            log.error("profiler-neon recalculate failed: user=%s: %s", user_id[:12], exc)

    # Advance cursor в любом случае (ошибки recalculate не блокируют прогресс)
    await _advance_cursor_neon(learning_conn, max_event_id)

    log.info(
        "profiler-neon batch: fetched=%d trigger_users=%d recalculated=%d errors=%d cursor→%d",
        len(rows),
        len(trigger_users),
        recalculated,
        errors,
        max_event_id,
    )

    # Высокая доля ошибок (>50% по умолчанию) — silent fail risk, явный warn.
    if trigger_users and errors / len(trigger_users) >= _NEON_ERROR_RATE_WARN:
        log.warning(
            "profiler-neon HIGH ERROR RATE: %d/%d users failed (>%.0f%%) — investigate",
            errors,
            len(trigger_users),
            _NEON_ERROR_RATE_WARN * 100,
        )

    return len(rows)


async def run_profiler_subscriber_neon(
    learning_dsn: str,
    neon_url: str,
    learning_url: str | None = None,
    indicators_url: str | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Polling worker для профайлера из Neon learning.domain_event (Gap F3+).

    Заменяет run_profiler_subscriber() для событий, которые после WP-268
    cut-over попадают в Neon, а не в Railway platform_outbox / user_events.
    LISTEN/NOTIFY не работает через Neon pooler — используем polling.

    learning_dsn: connection к Neon learning DB (LEARNING_URL direct).
    neon_url: DT_PROFILER_NEON_URL (digital_twins DB для recalculate_derived).
    """
    learning_conn = await asyncpg.connect(learning_dsn, statement_cache_size=0)
    executor: concurrent.futures.ThreadPoolExecutor | None = None
    try:
        # Fail-fast ping — битый URL падает сразу, а не через 30s timeout рестарт-цикл.
        await learning_conn.execute("SELECT 1")

        # Advisory lock — single-instance гарантия (Railway redeploy / scaling race).
        # session-scoped lock держится пока conn открыт; release при close().
        got_lock = False
        for _attempt in range(9):
            got_lock = await learning_conn.fetchval(
                "SELECT pg_try_advisory_lock($1)", _NEON_ADVISORY_LOCK_KEY
            )
            if got_lock:
                break
            log.info(
                "profiler-subscriber-neon: advisory lock %d занят — retry через 10s (attempt %d/9)",
                _NEON_ADVISORY_LOCK_KEY, _attempt + 1,
            )
            await asyncio.sleep(10)
        if not got_lock:
            log.warning(
                "profiler-subscriber-neon: advisory lock %d не получен за 90s — exit",
                _NEON_ADVISORY_LOCK_KEY,
            )
            return

        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="profiler-neon"
        )
        log.info(
            "profiler-subscriber-neon connected, poll=%.0fs, lock=%d acquired",
            NEON_POLLING_INTERVAL_SEC,
            _NEON_ADVISORY_LOCK_KEY,
        )

        # Startup drain — обработать весь накопившийся backlog с per-batch прогрессом.
        # При большом backlog (50K+) без прогресс-логов — часы тишины.
        total = 0
        batch_num = 0
        while True:
            batch_num += 1
            n = await _run_profiler_batch_neon(
                learning_conn, executor, neon_url, learning_url, indicators_url
            )
            total += n
            if n > 0:
                log.info(
                    "profiler-neon drain progress: batch=%d events=%d total=%d",
                    batch_num,
                    n,
                    total,
                )
            if n < NEON_BATCH_SIZE:
                break
        log.info("profiler-neon startup drain complete: %d events in %d batches", total, batch_num)

        loop_clock = asyncio.get_event_loop().time
        last_poll = loop_clock()

        while stop_event is None or not stop_event.is_set():
            await asyncio.sleep(1.0)
            now = loop_clock()
            if now - last_poll >= NEON_POLLING_INTERVAL_SEC:
                # Liveness-ping lock-conn — если Neon dropped idle conn, advisory lock
                # уже освободился, и второй инстанс мог стартовать. Exception → exit
                # → Railway restart → чистый re-acquire.
                await learning_conn.execute("SELECT 1")
                n = await _run_profiler_batch_neon(
                    learning_conn, executor, neon_url, learning_url, indicators_url
                )
                # Heartbeat — пишется per-poll даже при пустом batch (alerter видит liveness).
                await write_internal_metric(
                    learning_conn,
                    metric_name="profiler_neon_batch_processed",
                    worker=_NEON_WORKER_NAME,
                    value_numeric=float(n),
                )
                last_poll = now
    finally:
        if executor is not None:
            executor.shutdown(wait=False)
        await learning_conn.close()
        log.info("profiler-subscriber-neon stopped")


async def run_profiler_subscriber(
    activity_direct_dsn: str,
    neon_url: str,
    learning_url: str | None = None,
    indicators_url: str | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Главный цикл Profiler Subscriber.

    activity_direct_dsn: DIRECT connection к activity-hub DB (без -pooler.).
    """
    conn = await asyncpg.connect(activity_direct_dsn, statement_cache_size=0)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="profiler")
    log.info("profiler-subscriber connected (direct DSN), worker=%s", WORKER_NAME)

    try:
        # Startup replay
        total = 0
        while True:
            n = await _replay_from_cursor(conn, executor, neon_url, learning_url, indicators_url)
            total += n
            if n < REPLAY_BATCH:
                break
        log.info("startup replay complete: %d rows processed", total)

        notify_queue: asyncio.Queue[str] = asyncio.Queue()

        def _on_notify(_c, _pid, _channel, payload: str) -> None:
            notify_queue.put_nowait(payload)

        await conn.add_listener(CHANNEL, _on_notify)
        log.info("listening on channel %s", CHANNEL)

        loop_clock = asyncio.get_event_loop().time
        last_replay = loop_clock()

        while stop_event is None or not stop_event.is_set():
            try:
                payload = await asyncio.wait_for(notify_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                payload = None

            now = loop_clock()

            if payload is not None and payload != "batch":
                # Single event NOTIFY: fetch и обработать
                try:
                    outbox_id = int(payload)
                except ValueError:
                    log.error("invalid NOTIFY payload: %r", payload)
                else:
                    cursor = await _get_cursor(conn)
                    if outbox_id > cursor:
                        row = await conn.fetchrow(
                            """
                            SELECT id, event_type, event_class, user_uuid, payload
                            FROM operations.platform_outbox
                            WHERE id = $1
                            """,
                            outbox_id,
                        )
                        if row:
                            await _process_outbox_row(
                                row, executor, neon_url, learning_url, indicators_url, conn
                            )

            # Периодический replay — catch-up для batch-path и missed NOTIFY
            if now - last_replay >= REPLAY_INTERVAL_SEC or payload == "batch":
                await _replay_from_cursor(conn, executor, neon_url, learning_url, indicators_url)
                last_replay = now

    finally:
        executor.shutdown(wait=False)
        await conn.close()
        log.info("profiler-subscriber stopped")
