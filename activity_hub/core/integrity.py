"""Integrity Pipeline — валидация, timestamp sanity, rate limit check."""

import logging
from datetime import datetime, timedelta
from typing import Optional

from activity_hub.core.models import RawEvent, KNOWN_SOURCES, KNOWN_EVENT_TYPES

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Событие не прошло валидацию → quarantine."""

    def __init__(self, reason: str, event: RawEvent):
        self.reason = reason
        self.event = event
        super().__init__(f"{reason}: {event.source}/{event.external_id}")


def validate_event(event: RawEvent, max_age_days: int = 30) -> Optional[str]:
    """Валидация события. Возвращает reason если невалидно, None если ок.

    max_age_days: максимальный возраст события в днях. 30 для ежедневного sync,
    больше для backfill (исторический импорт).
    """

    # 1. Source
    if event.source not in KNOWN_SOURCES:
        return f"unknown_source:{event.source}"

    # 2. Event type
    if event.event_type not in KNOWN_EVENT_TYPES:
        return f"unknown_event_type:{event.event_type}"

    # 3. Confidence range
    if not (0.0 <= event.confidence <= 1.0):
        return f"confidence_out_of_range:{event.confidence}"

    # 4. Timestamp sanity
    now = datetime.utcnow()
    if event.occurred_at > now + timedelta(minutes=5):
        return f"future_timestamp:{event.occurred_at.isoformat()}"
    if event.occurred_at < now - timedelta(days=max_age_days):
        return f"stale_timestamp:{event.occurred_at.isoformat()}"

    # 5. External ID not empty
    if not event.external_id or not event.external_id.strip():
        return "empty_external_id"

    # 6. User ref not empty
    if not event.user_ref:
        return "empty_user_ref"

    return None


async def check_rate_limit(
    conn,
    user_uuid,
    source: str,
    limit: int = 100,
) -> bool:
    """Проверка: >limit events/user/day → flag. Возвращает True если лимит превышен."""
    if user_uuid is None:
        return False

    row = await conn.fetchrow(
        """
        SELECT COUNT(*) as cnt
        FROM development.user_events
        WHERE user_uuid = $1
          AND source = $2
          AND created_at >= NOW() - INTERVAL '1 day'
        """,
        user_uuid,
        source,
    )
    count = row["cnt"] if row else 0

    if count >= limit:
        logger.warning(
            "Rate limit exceeded: user_uuid=%s source=%s count=%d limit=%d",
            user_uuid, source, count, limit,
        )
        return True

    return False
