"""Landing zone — запись сырья в development.raw_events.

Bronze-слой medallion архитектуры (WP-109 Ф8). Adapter забирает ответ
источника и передаёт его сюда **как есть**, без парсинга. Landing складывает
payload один-в-один, присваивая только (source, external_id, fetched_at) —
минимум, нужный для idempotency и партиционирования.

Transform-логика (парсинг payload → event_type, user_ref, occurred_at) живёт
в transforms/*.py и вызывается отдельным transform-worker'ом (Ф8.4), который
читает raw_events.payload и пишет распарсенное в user_events.

Инвариант bronze: payload в raw_events = ровно то, что пришло от источника.
Любая потеря полей на этом шаге — нарушение invariant и ошибка pipeline.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import asyncpg

from activity_hub.core.models import KNOWN_SOURCES

logger = logging.getLogger(__name__)


@dataclass
class RawItem:
    """Один элемент сырья, готовый к записи в bronze.

    external_id — ключ idempotency в пределах source. Вычисляется adapter'ом
    по payload (например, для LMS: f"{action}:{actionId}"). Adapter знает
    формат источника, landing — нет.

    payload — сырой dict из API. Должен быть JSON-сериализуемым.
    fetched_at — момент получения. Если None — landing проставит NOW() в UTC.
    """

    external_id: str
    payload: dict
    fetched_at: Optional[datetime] = None


@dataclass
class WriteResult:
    inserted: int = 0
    skipped: int = 0  # dedup: (source, external_id, fetched_at) уже есть
    failed: int = 0  # JSON-сериализация или INSERT упали

    def __add__(self, other: "WriteResult") -> "WriteResult":
        return WriteResult(
            inserted=self.inserted + other.inserted,
            skipped=self.skipped + other.skipped,
            failed=self.failed + other.failed,
        )


async def write_raw(
    conn: asyncpg.Connection,
    source: str,
    items: list[RawItem],
) -> WriteResult:
    """Записать сырьё в development.raw_events.

    Идемпотентность: UNIQUE (source, external_id, fetched_at) на корневой
    таблице + ON CONFLICT DO NOTHING. Повторный прогон того же окна
    с теми же fetched_at → 0 inserted.

    Устойчивость к битым элементам: невалидные (пустой external_id
    или JSON-сериализация упала) помечаются как failed и логируются,
    но цикл продолжается. Валидные записываются.

    Returns: WriteResult(inserted, skipped, failed).
    """
    if source not in KNOWN_SOURCES:
        raise ValueError(f"Unknown source: {source!r}. Expected one of {KNOWN_SOURCES}.")

    if not items:
        return WriteResult()

    now = datetime.now(timezone.utc)
    result = WriteResult()

    for item in items:
        if not item.external_id:
            logger.warning("landing: skip item with empty external_id (source=%s)", source)
            result.failed += 1
            continue

        try:
            # Без default=str: adapter обязан отдавать JSON-совместимый dict.
            # Молчаливое приведение к строке замаскирует битые payload и
            # нарушит инвариант bronze (сырьё один-в-один).
            payload_json = json.dumps(item.payload, ensure_ascii=False)
        except (TypeError, ValueError) as e:
            logger.warning(
                "landing: JSON serialization failed (source=%s ext_id=%s): %s",
                source, item.external_id, e,
            )
            result.failed += 1
            continue

        fetched_at = item.fetched_at or now

        try:
            row = await conn.fetchrow(
                """
                INSERT INTO development.raw_events
                    (source, external_id, payload, fetched_at)
                VALUES ($1, $2, $3::jsonb, $4)
                ON CONFLICT (source, external_id, fetched_at) DO NOTHING
                RETURNING id
                """,
                source, item.external_id, payload_json, fetched_at,
            )
            if row is None:
                result.skipped += 1
            else:
                result.inserted += 1
        except Exception as e:
            logger.error(
                "landing: INSERT failed (source=%s ext_id=%s): %s",
                source, item.external_id, e,
            )
            result.failed += 1

    logger.info(
        "landing: source=%s inserted=%d skipped=%d failed=%d (of %d)",
        source, result.inserted, result.skipped, result.failed, len(items),
    )
    return result
