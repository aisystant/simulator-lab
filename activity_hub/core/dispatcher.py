"""Activity Hub Dispatcher — LISTEN activity_event_ingested + routing по event_class.

WP-109 Ф5: Слой 2 Event Bus. Dispatcher — единственный писатель processed_at в outbox.
Подписчики (profiler, points engine и т.д.) используют worker_cursors и слушают тот же
канал напрямую — они НЕ зависят от Dispatcher.

Инвариант:
  - LISTEN соединение ДОЛЖНО быть DIRECT (без -pooler.), иначе Neon PgBouncer блокирует.
  - Replay при старте покрывает missed NOTIFY за время downtime.
  - Dispatcher маркирует outbox.processed_at — только сигнализирует «доставлено».
    Реальная обработка — в subscribers (worker_cursors).
"""

from __future__ import annotations

import asyncio
import logging
import os

import asyncpg

log = logging.getLogger(__name__)

CHANNEL = "activity_event_ingested"
REPLAY_INTERVAL_SEC = float(os.environ.get("DISPATCHER_REPLAY_INTERVAL", "60"))
REPLAY_BATCH = int(os.environ.get("DISPATCHER_REPLAY_BATCH", "500"))


async def _mark_processed(conn: asyncpg.Connection, outbox_id: int) -> None:
    await conn.execute(
        "UPDATE operations.platform_outbox SET processed_at = NOW() WHERE id = $1",
        outbox_id,
    )


async def _route(conn: asyncpg.Connection, row: asyncpg.Record) -> None:
    """Маршрутизация одного outbox-события. Расширяется по мере появления consumers."""
    event_class = row["event_class"]
    log.info(
        "dispatched outbox_id=%s event_type=%s event_class=%s user_uuid=%s",
        row["id"],
        row["event_type"],
        event_class,
        row["user_uuid"],
    )
    # Subscribers (profiler, points engine) работают через собственные cursors/LISTEN.
    # Dispatcher только помечает «processed» = доставлено в outbox, не забыто.
    await _mark_processed(conn, row["id"])


async def _replay_unprocessed(conn: asyncpg.Connection, limit: int = REPLAY_BATCH) -> int:
    """Догнать необработанные outbox-записи (missed NOTIFY / downtime recovery)."""
    rows = await conn.fetch(
        """
        SELECT id, event_id, event_type, event_class, user_uuid, payload, created_at
        FROM operations.platform_outbox
        WHERE processed_at IS NULL
        ORDER BY id
        LIMIT $1
        """,
        limit,
    )
    for row in rows:
        await _route(conn, row)
    if rows:
        log.info("replay: processed %d unprocessed outbox rows", len(rows))
    return len(rows)


async def run_dispatcher(
    activity_dsn: str,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Главный цикл Dispatcher.

    activity_dsn: DIRECT connection (без -pooler.) — обязательно для LISTEN.
    """
    conn = await asyncpg.connect(activity_dsn, statement_cache_size=0)
    log.info("dispatcher connected (direct DSN)")

    try:
        # Replay при старте: обработать всё пропущенное
        total_replayed = 0
        while True:
            n = await _replay_unprocessed(conn)
            total_replayed += n
            if n < REPLAY_BATCH:
                break
        log.info("startup replay complete: %d rows", total_replayed)

        # Реакция на NOTIFY: payload = outbox_id (строка) или 'batch'
        async def _on_notify(_c, _pid, _channel, payload: str) -> None:
            if payload == "batch":
                await _replay_unprocessed(conn)
                return
            try:
                outbox_id = int(payload)
            except ValueError:
                log.error("invalid NOTIFY payload: %r", payload)
                return
            row = await conn.fetchrow(
                """
                SELECT id, event_id, event_type, event_class, user_uuid, payload, created_at
                FROM operations.platform_outbox
                WHERE id = $1
                """,
                outbox_id,
            )
            if row is None:
                log.warning("outbox_id=%s not found (already processed?)", outbox_id)
                return
            if row["processed_at"] is not None:
                return  # idempotent
            await _route(conn, row)

        await conn.add_listener(CHANNEL, _on_notify)
        log.info("listening on channel %s", CHANNEL)

        loop_clock = asyncio.get_event_loop().time
        last_replay = loop_clock()

        while stop_event is None or not stop_event.is_set():
            await asyncio.sleep(1.0)
            # Периодический replay — защита от missed NOTIFY
            now = loop_clock()
            if now - last_replay >= REPLAY_INTERVAL_SEC:
                await _replay_unprocessed(conn)
                last_replay = now

    finally:
        await conn.close()
        log.info("dispatcher stopped")
