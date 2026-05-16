# see DP.SC.025, DP.ROLE.001#R29
"""Transform-worker — silver pipeline: raw_events → user_events.

WP-109 Ф8.4. Читает строки development.raw_events WHERE transform_status='pending',
парсит payload через transforms/<source>.py, делает idempotent upsert в
development.user_events, помечает строки 'done' или 'failed'.

Запуск:
    python runner.py transform                    # все pending, все source
    python runner.py transform --source lms       # только lms
    python runner.py transform --limit 500        # не более 500 строк за запуск
    python runner.py transform --dry-run          # только логи, без записи

Idempotency: user_events имеет UNIQUE (source, external_id) WHERE external_id IS NOT NULL.
Повторный прогон тех же raw_events → 0 новых строк в user_events,
все raw_events помечаются 'done'.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import asyncpg

from activity_hub.core.identity import resolve_user_uuid
from activity_hub.transforms import lms as lms_transform
from activity_hub.transforms import iwe_decisions as iwe_decisions_transform

logger = logging.getLogger(__name__)

# Реестр transform-функций по source.
# Каждая функция: dict → ParsedEvent | None.
# Добавить новый source: импортировать модуль и зарегистрировать здесь.
_TRANSFORMS = {
    "lms": lms_transform.parse_action,
    "iwe": iwe_decisions_transform.parse_decision,
}


@dataclass
class TransformResult:
    source: str
    processed: int = 0   # строк прочитано из raw_events
    done: int = 0        # успешно → user_events + raw_events.status='done'
    skipped: int = 0     # dedup: уже есть в user_events
    failed: int = 0      # parse error или user_not_found → status='failed'
    dry_run: bool = False

    def __str__(self) -> str:
        suffix = " [DRY RUN]" if self.dry_run else ""
        return (
            f"source={self.source} processed={self.processed} "
            f"done={self.done} skipped={self.skipped} failed={self.failed}{suffix}"
        )


async def run_transform(
    pool: asyncpg.Pool,
    source: Optional[str] = None,
    limit: int = 1000,
    dry_run: bool = False,
) -> list[TransformResult]:
    """Запустить transform-worker для одного или всех source.

    Returns: список TransformResult (по одному на каждый обработанный source).
    """
    sources = [source] if source else list(_TRANSFORMS.keys())
    results = []
    for src in sources:
        result = await _transform_source(pool, src, limit=limit, dry_run=dry_run)
        results.append(result)
    return results


async def _transform_source(
    pool: asyncpg.Pool,
    source: str,
    limit: int,
    dry_run: bool,
) -> TransformResult:
    """Transform для одного source."""
    result = TransformResult(source=source, dry_run=dry_run)

    parse_fn = _TRANSFORMS.get(source)
    if parse_fn is None:
        logger.warning("transform: no transform registered for source=%s", source)
        return result

    # Читаем pending строки батчами, чтобы не держать большой cursor открытым.
    # fetch_batch закрывает транзакцию после чтения — worker обрабатывает каждую
    # строку в отдельной транзакции (write + status update атомарно).
    rows = await _fetch_pending(pool, source, limit)
    result.processed = len(rows)

    if not rows:
        logger.info("transform: source=%s — 0 pending rows", source)
        return result

    logger.info("transform: source=%s — %d pending rows", source, len(rows))

    for row in rows:
        raw_id = row["id"]
        raw_payload = row["payload"]
        # asyncpg может вернуть JSONB как строку (Railway Postgres без явного codec)
        if isinstance(raw_payload, str):
            payload = json.loads(raw_payload)
        elif isinstance(raw_payload, dict):
            payload = raw_payload
        else:
            payload = dict(raw_payload)

        # 1. Parse
        parsed = parse_fn(payload)
        if parsed is None:
            logger.warning("transform: parse failed raw_id=%d source=%s", raw_id, source)
            result.failed += 1
            if not dry_run:
                await _mark(pool, raw_id, "failed", "parse_error")
            continue

        # 2. Identity resolution
        async with pool.acquire() as conn:
            user_uuid = await resolve_user_uuid(conn, parsed.user_ref)

        if user_uuid is None:
            logger.warning(
                "transform: user_not_found raw_id=%d user_ref=%s", raw_id, parsed.user_ref
            )
            result.failed += 1
            if not dry_run:
                await _mark(pool, raw_id, "failed", "user_not_found")
            continue

        if dry_run:
            logger.info(
                "transform [DRY RUN]: raw_id=%d → %s user=%s event_type=%s",
                raw_id, source, user_uuid, parsed.event_type,
            )
            result.done += 1
            continue

        # 3. Upsert в user_events + mark done — атомарно в одной транзакции
        inserted = await _upsert_event(pool, raw_id, source, parsed, user_uuid)
        if inserted:
            result.done += 1
        else:
            result.skipped += 1  # dedup: (source, external_id) уже есть

    logger.info("transform: %s", result)
    return result


async def _fetch_pending(
    pool: asyncpg.Pool,
    source: str,
    limit: int,
) -> list[asyncpg.Record]:
    """Получить pending строки из raw_events для данного source."""
    async with pool.acquire() as conn:
        return await conn.fetch(
            """
            SELECT id, payload
            FROM development.raw_events
            WHERE source = $1
              AND transform_status = 'pending'
            ORDER BY fetched_at ASC
            LIMIT $2
            """,
            source, limit,
        )


async def _upsert_event(
    pool: asyncpg.Pool,
    raw_id: int,
    source: str,
    parsed,
    user_uuid,
) -> bool:
    """Upsert parsed event в user_events и пометить raw строку.

    Returns True если вставлено новое событие, False если dedup (уже есть).
    Оба случая → raw_events.transform_status = 'done'.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO development.user_events
                    (user_id, user_uuid, event_type, source, payload,
                     confidence, created_at, external_id)
                VALUES (
                    COALESCE($1, 0),
                    $2, $3, $4, $5, $6, $7, $8
                )
                ON CONFLICT (source, external_id) WHERE external_id IS NOT NULL
                DO NOTHING
                RETURNING id
                """,
                parsed.user_ref.get("telegram_id"),
                user_uuid,
                parsed.event_type,
                source,
                json.dumps(parsed.payload),
                parsed.confidence,
                parsed.occurred_at,
                parsed.external_id,
            )

            # Пометить raw строку независимо от dedup-результата
            await conn.execute(
                """
                UPDATE development.raw_events
                SET transform_status = 'done',
                    transformed_at = NOW()
                WHERE id = $1
                """,
                raw_id,
            )

    inserted = row is not None
    if inserted:
        logger.debug(
            "transform: upserted event raw_id=%d source=%s ext_id=%s user=%s",
            raw_id, source, parsed.external_id, user_uuid,
        )
    else:
        logger.debug(
            "transform: dedup skip raw_id=%d source=%s ext_id=%s",
            raw_id, source, parsed.external_id,
        )
    return inserted


async def _mark(
    pool: asyncpg.Pool,
    raw_id: int,
    status: str,
    error: Optional[str] = None,
) -> None:
    """Пометить raw_events строку статусом без записи в user_events."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE development.raw_events
            SET transform_status = $1,
                transform_error = $2,
                transformed_at = NOW()
            WHERE id = $3
            """,
            status, error, raw_id,
        )
