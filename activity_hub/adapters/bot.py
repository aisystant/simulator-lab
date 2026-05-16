"""Bot Adapter — замена log_event() на ingest_event() через Hub.

Бот импортирует Hub как библиотеку (не по HTTP → 0ms overhead).
Drop-in replacement: та же сигнатура, но через Integrity Pipeline.
"""

import logging
from typing import Optional

import asyncpg

from activity_hub.core.hub import ingest_event
from activity_hub.core.models import RawEvent

logger = logging.getLogger(__name__)


async def bot_ingest_event(
    pool: asyncpg.Pool,
    user_id: int,
    event_type: str,
    payload: Optional[dict] = None,
    confidence: float = 1.0,
    source: str = "bot",
) -> Optional[int]:
    """Drop-in replacement для log_event() в боте.

    Сигнатура максимально близка к оригинальной log_event(),
    чтобы минимизировать diff при переключении.
    """
    event = RawEvent(
        source=source,
        external_id=f"bot-{user_id}-{event_type}-{__import__('time').time_ns()}",
        user_ref={"telegram_id": user_id},
        event_type=event_type,
        payload=payload or {},
        confidence=confidence,
    )

    try:
        async with pool.acquire() as conn:
            return await ingest_event(conn, event)
    except Exception as e:
        logger.warning("bot_ingest_event failed: %s — %s", event_type, e)
        return None
