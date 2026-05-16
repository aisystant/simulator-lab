"""Hub Core — единая точка записи событий."""

import json
import logging
from datetime import datetime
from typing import Optional
from uuid import UUID

import asyncpg

from activity_hub.core.identity import resolve_user_uuid
from activity_hub.core.integrity import validate_event, check_rate_limit
from activity_hub.core.models import RawEvent, EVENT_CLASS_MAP, DEFAULT_EVENT_CLASS

NOTIFY_CHANNEL = "activity_event_ingested"

logger = logging.getLogger(__name__)


async def _quarantine(conn: asyncpg.Connection, event: RawEvent, reason: str):
    """Записать невалидное событие в карантин."""
    await conn.execute(
        """
        INSERT INTO development.quarantined_events
            (source, external_id, user_ref, event_type, payload, reason)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        event.source,
        event.external_id,
        json.dumps(event.user_ref),
        event.event_type,
        json.dumps(event.payload),
        reason,
    )
    logger.warning("Quarantined event: source=%s ext_id=%s reason=%s",
                   event.source, event.external_id, reason)


async def ingest_event(
    conn: asyncpg.Connection,
    event: RawEvent,
    max_age_days: int = 30,
) -> Optional[int]:
    """Основная функция записи события через Integrity Pipeline.

    Шаги:
    1. Schema validation
    2. Identity resolution (user_ref → user_uuid)
    3. Rate limit check
    4. Dedup (ON CONFLICT DO NOTHING)
    5. Write (append-only)
    6. Quarantine (при ошибках валидации)

    Returns: event_id если записано, None если skip/quarantine.
    """
    # 1. Validate
    reason = validate_event(event, max_age_days=max_age_days)
    if reason:
        await _quarantine(conn, event, reason)
        return None

    # 2. Identity
    user_uuid = await resolve_user_uuid(conn, event.user_ref)
    if user_uuid is None:
        await _quarantine(conn, event, "user_not_found")
        return None

    # 3. Rate limit
    is_over_limit = await check_rate_limit(conn, user_uuid, event.source)
    if is_over_limit:
        await _quarantine(conn, event, "rate_limit_exceeded")
        return None

    # 4+5. Write with dedup + outbox + notify (атомарно в одной транзакции)
    try:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO development.user_events
                    (user_id, user_uuid, event_type, source, payload, confidence, created_at, external_id)
                VALUES (
                    COALESCE($1, 0),
                    $2, $3, $4, $5, $6, $7, $8
                )
                ON CONFLICT (source, external_id) WHERE external_id IS NOT NULL
                DO NOTHING
                RETURNING id
                """,
                event.user_ref.get("telegram_id"),
                user_uuid,
                event.event_type,
                event.source,
                json.dumps(event.payload),
                event.confidence,
                event.occurred_at,
                event.external_id,
            )

            if row is None:
                # Dedup: уже существует — транзакция откатывается автоматически
                logger.debug("Dedup skip: source=%s ext_id=%s", event.source, event.external_id)
                return None

            event_id = row["id"]

            # WP-109 Ф5: атомарная запись в outbox + NOTIFY
            event_class = EVENT_CLASS_MAP.get(event.event_type, DEFAULT_EVENT_CLASS)
            outbox_row = await conn.fetchrow(
                """
                INSERT INTO operations.platform_outbox
                    (event_id, event_type, event_class, user_uuid, payload)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id
                """,
                event_id,
                event.event_type,
                event_class,
                user_uuid,
                json.dumps(event.payload),
            )
            await conn.execute(
                "SELECT pg_notify($1, $2)",
                NOTIFY_CHANNEL,
                str(outbox_row["id"]),
            )

    except Exception as e:
        logger.error("Failed to write event: %s — %s", event.external_id, e)
        await _quarantine(conn, event, f"write_error:{e}")
        return None

    logger.info("Ingested event: id=%d source=%s type=%s user=%s class=%s",
                event_id, event.source, event.event_type, user_uuid, event_class)
    return event_id


async def ingest_batch(
    pool: asyncpg.Pool,
    events: list[RawEvent],
    max_age_days: int = 30,
    batch_size: int = 500,
) -> dict:
    """Batch-запись событий. Возвращает статистику.

    Трёхфазный pipeline (всё батчами, минимум round-trips к Neon):
    1. Валидация в памяти (без сети)
    2. Identity resolution (один запрос для всех уникальных user_ref)
    3. Bulk INSERT + bulk quarantine через executemany
    """
    stats = {"written": 0, "skipped": 0, "quarantined": 0, "errors": 0}

    # Фаза 1: валидация в памяти (без сетевых запросов)
    validated = []  # (event, reason_or_None)
    for event in events:
        reason = validate_event(event, max_age_days=max_age_days)
        validated.append((event, reason))

    quarantine_rows = []  # для batch quarantine
    valid_events = []

    for event, reason in validated:
        if reason:
            quarantine_rows.append((
                event.source, event.external_id, json.dumps(event.user_ref),
                event.event_type, json.dumps(event.payload), reason,
            ))
            stats["quarantined"] += 1
        else:
            valid_events.append(event)

    # Фаза 2: identity resolution (batch — предзагрузка кэша)
    ready_rows = []
    async with pool.acquire() as conn:
        for event in valid_events:
            try:
                user_uuid = await resolve_user_uuid(conn, event.user_ref)
                if user_uuid is None:
                    quarantine_rows.append((
                        event.source, event.external_id, json.dumps(event.user_ref),
                        event.event_type, json.dumps(event.payload), "user_not_found",
                    ))
                    stats["quarantined"] += 1
                    continue

                ready_rows.append((
                    event.user_ref.get("telegram_id"),
                    user_uuid,
                    event.event_type,
                    event.source,
                    json.dumps(event.payload),
                    event.confidence,
                    event.occurred_at,
                    event.external_id,
                ))
            except Exception as e:
                logger.error("Identity error for %s: %s", event.external_id, e)
                stats["errors"] += 1

    # Фаза 3a: bulk quarantine
    if quarantine_rows:
        async with pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO development.quarantined_events
                    (source, external_id, user_ref, event_type, payload, reason)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                quarantine_rows,
            )
        logger.info("Quarantined %d events (batch)", len(quarantine_rows))

    # Фаза 3b: bulk INSERT valid events
    if ready_rows:
        async with pool.acquire() as conn:
            for i in range(0, len(ready_rows), batch_size):
                chunk = ready_rows[i:i + batch_size]
                try:
                    await conn.executemany(
                        """
                        INSERT INTO development.user_events
                            (user_id, user_uuid, event_type, source, payload, confidence, created_at, external_id)
                        VALUES (
                            COALESCE($1, 0),
                            $2, $3, $4, $5, $6, $7, $8
                        )
                        ON CONFLICT (source, external_id) WHERE external_id IS NOT NULL
                        DO NOTHING
                        """,
                        chunk,
                    )
                except Exception as e:
                    logger.error("Bulk insert error (chunk %d-%d): %s", i, i + len(chunk), e)
                    stats["errors"] += len(chunk)

        stats["written"] = len(ready_rows) - stats["errors"]

    logger.info("Batch complete: %d valid→written, %d quarantined, %d errors (of %d total)",
                stats["written"], stats["quarantined"], stats["errors"], len(events))
    return stats


async def ingest_batch_to_domain_event(
    learning_pool: asyncpg.Pool,
    events: list[RawEvent],
    batch_size: int = 500,
) -> dict:
    """Batch-запись IWE-событий в learning.domain_event.

    Только для IWE-событий с user_ref={'ory_uuid': uuid}.
    account_id резолвится без DB lookup — ory_uuid IS account_id.
    """
    stats = {"attempted": 0, "errors": 0}

    rows = []
    for event in events:
        ory_uuid = event.user_ref.get("ory_uuid")
        if not ory_uuid:
            logger.warning("domain_event: no ory_uuid in user_ref for %s", event.external_id)
            stats["errors"] += 1
            continue
        try:
            account_id = UUID(ory_uuid)
        except (ValueError, AttributeError) as exc:
            logger.warning("domain_event: invalid UUID %s: %s", ory_uuid, exc)
            stats["errors"] += 1
            continue
        rows.append((
            event.source,
            event.external_id,
            event.event_type,
            json.dumps(event.payload),
            account_id,
            event.occurred_at,
        ))

    if not rows:
        return stats

    async with learning_pool.acquire() as conn:
        for i in range(0, len(rows), batch_size):
            chunk = rows[i:i + batch_size]
            try:
                await conn.executemany(
                    """
                    INSERT INTO domain_event
                        (source, external_id, event_type, payload, account_id, occurred_at)
                    VALUES ($1, $2, $3, $4::jsonb, $5, $6)
                    ON CONFLICT (source, external_id) DO NOTHING
                    """,
                    chunk,
                )
                stats["attempted"] += len(chunk)
            except Exception as exc:
                logger.error("domain_event bulk insert error (chunk %d-%d): %s",
                             i, i + len(chunk), exc)
                stats["errors"] += len(chunk)

    logger.info("domain_event batch: %d attempted, %d errors (of %d events)",
                stats["attempted"], stats["errors"], len(events))
    return stats
