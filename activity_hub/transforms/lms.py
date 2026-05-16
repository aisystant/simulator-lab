# see DP.SC.025, DP.ROLE.001#R29
"""Transform: LMS raw_events.payload → ParsedEvent (silver-слой).

WP-109 Ф8.4. Логика парсинга вынесена из adapters/lms.py._parse_action.
Transform-worker (core/transform.py) вызывает parse_action() для каждой
строки raw_events с source='lms'.

Инвариант: этот модуль не знает о БД и asyncpg. Только чистый Python.
Входные данные — dict (payload из bronze). Выходные — ParsedEvent или None.
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Маппинг LMS action → наш event_type (единственный источник истины).
# Копия в adapters/lms.py помечена deprecated и будет удалена после Ф8.4.
ACTION_TYPE_MAP: dict[str, str] = {
    "TEXT": "text_submitted",
    "TABLE": "table_submitted",
    "TEST": "test_passed",
    "TASK": "task_submitted",
    "AI": "ai_interaction",
    "CREATE_TOPIC": "topic_created",
    "CREATE_COMMENT": "comment_created",
    "POMODORO": "pomodoro_completed",
}


@dataclass
class ParsedEvent:
    """Результат парсинга одной строки bronze → готово к уpsert в user_events.

    user_ref — словарь для identity resolution (email, lms_user_id).
    Formат совпадает с RawEvent.user_ref, чтобы transform-worker
    мог передать в resolve_user_uuid() без конвертации.
    """

    external_id: str       # dedup-ключ для user_events: (source, external_id)
    event_type: str        # из ACTION_TYPE_MAP или action.lower()
    user_ref: dict         # {'email': ..., 'lms_user_id': ...} или {'lms_user_id': ...}
    occurred_at: datetime  # UTC naive
    payload: dict          # всё кроме action/actionId/datetime/email
    confidence: float = 0.9


def _parse_timestamp(dt_str: str) -> Optional[datetime]:
    """LMS timestamp → UTC naive datetime.

    LMS отдаёт нестандартные форматы:
    - '2026-04-02T23:59:02.58+05:00[Asia/Yekaterinburg]' (с timezone name)
    - '2026-04-02T23:59:02.58+05:' (обрезанный offset)
    - '2026-04-02T23:59:02.44545' (нестандартные дробные секунды)
    """
    # 1. Убрать timezone name в скобках [...]
    clean = dt_str.split("[")[0].strip()

    # 2. Исправить обрезанный offset: +05: → +05:00
    clean = re.sub(r'([+-]\d{2}):$', r'\1:00', clean)

    # 3. Нормализовать дробные секунды до 6 знаков
    if "." in clean:
        base, rest = clean.split(".", 1)
        m = re.match(r'(\d+)(.*)', rest)
        if m:
            frac = m.group(1)[:6].ljust(6, '0')
            suffix = m.group(2)
            clean = f"{base}.{frac}{suffix}"

    try:
        dt = datetime.fromisoformat(clean)
    except ValueError:
        return None

    # Привести к UTC naive
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def parse_action(payload: dict) -> Optional[ParsedEvent]:
    """Распарсить LMS action payload → ParsedEvent.

    payload — сырой dict из bronze (то, что LMS вернул в JSON).
    Возвращает None если payload невалиден (нет action или datetime).

    Это единственная точка парсинга LMS. adapters/lms.py._parse_action
    устарела и будет удалена после полного перехода на transform-worker.
    """
    action_type = payload.get("action", "")
    action_id = payload.get("actionId")
    dt_str = payload.get("datetime", "")

    if not action_type or not dt_str:
        logger.warning("LMS transform: missing action or datetime in payload: %s", payload)
        return None

    # dedup-ключ (совпадает с тем, что adapter кладёт в raw_events.external_id)
    if action_id:
        external_id = f"{action_type}:{action_id}"
    else:
        external_id = f"{action_type}:{dt_str}"

    event_type = ACTION_TYPE_MAP.get(action_type, action_type.lower())

    occurred_at = _parse_timestamp(dt_str)
    if occurred_at is None:
        logger.warning("LMS transform: cannot parse timestamp: %s", dt_str)
        return None

    # payload silver: всё кроме технических полей
    clean_payload = {
        k: v for k, v in payload.items()
        if k not in ("action", "actionId", "datetime", "email", "userId")
    }

    # user_ref: email (bulk endpoint) + lms_user_id
    email = payload.get("email")
    user_id = payload.get("userId")
    lms_user_id = int(user_id) if user_id is not None else None

    if email and lms_user_id is not None:
        user_ref = {"email": email, "lms_user_id": lms_user_id}
    elif lms_user_id is not None:
        user_ref = {"lms_user_id": lms_user_id}
    elif email:
        user_ref = {"email": email}
    else:
        logger.warning("LMS transform: no user identity in payload: %s", payload)
        return None

    return ParsedEvent(
        external_id=external_id,
        event_type=event_type,
        user_ref=user_ref,
        occurred_at=occurred_at,
        payload=clean_payload,
    )
