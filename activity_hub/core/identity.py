"""Identity Resolver — маппинг user_ref → user_uuid (Ory UUID)."""

import logging
from typing import Optional
from uuid import UUID

import asyncpg

logger = logging.getLogger(__name__)

# Кэш маппингов на время batch-sync (сбрасывается между запусками)
_cache: dict[str, Optional[UUID]] = {}


def _cache_key(user_ref: dict) -> str:
    """Детерминированный ключ из user_ref."""
    parts = sorted(user_ref.items())
    return "|".join(f"{k}={v}" for k, v in parts)


async def resolve_user_uuid(
    conn: asyncpg.Connection,
    user_ref: dict,
) -> Optional[UUID]:
    """Резолвит user_ref в user_uuid из public.users.

    Поддерживаемые ключи в user_ref:
    - telegram_id → public.users.id (бот-таблица, legacy)
    - lms_user_id → ищем по external mapping
    - ory_uuid → прямое использование
    """
    key = _cache_key(user_ref)
    if key in _cache:
        return _cache[key]

    uuid = None

    if "ory_uuid" in user_ref:
        uuid = UUID(user_ref["ory_uuid"]) if isinstance(user_ref["ory_uuid"], str) else user_ref["ory_uuid"]

    elif "telegram_id" in user_ref:
        row = await conn.fetchrow(
            "SELECT id FROM public.users WHERE telegram_id = $1",
            int(user_ref["telegram_id"]),
        )
        if row and row["id"]:
            uuid = row["id"]

    elif "lms_user_id" in user_ref:
        row = await conn.fetchrow(
            "SELECT user_uuid FROM development.identity_map WHERE source = 'lms' AND external_id = $1",
            str(user_ref["lms_user_id"]),
        )
        if row:
            uuid = row["user_uuid"]

    elif "email" in user_ref:
        row = await conn.fetchrow(
            "SELECT id FROM public.users WHERE LOWER(email) = LOWER($1)",
            user_ref["email"],
        )
        if row and row["id"]:
            uuid = row["id"]

    elif "session_id" in user_ref:
        # IWE decision events: session_id → ory_uuid через identity_map source='iwe'.
        # v1: ищем по session_id в identity_map. Если нет — возвращаем None.
        # v2: когда sessions-таблица появится → JOIN session_id → ory_uuid.
        row = await conn.fetchrow(
            "SELECT user_uuid FROM development.identity_map WHERE source = 'iwe' AND external_id = $1",
            str(user_ref["session_id"]),
        )
        if row:
            uuid = row["user_uuid"]
        else:
            logger.debug("session_id=%s not in identity_map (iwe) — decision event will be quarantined",
                         user_ref["session_id"])

    _cache[key] = uuid

    if uuid is None:
        logger.warning("Cannot resolve user_ref=%s to user_uuid", user_ref)

    return uuid


def clear_cache():
    """Сбросить кэш (между batch-sync)."""
    _cache.clear()
