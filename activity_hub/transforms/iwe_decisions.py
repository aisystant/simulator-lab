# see DP.SC.025, DP.ROLE.001#R47
"""Transform: IWE decision raw_events.payload → ParsedEvent (silver-слой).

WP-109 Ф7b. Обрабатывает события decision_user, записанные capture-bus.sh
(detector_decision.sh) в development.raw_events (source='iwe').

Формат payload (из detector_decision.sh):
{
  "event_type": "decision_approve|decision_reject|decision_redirect|decision_architectural|decision_strategic",
  "payload": {
    "session_id": "...",
    "user_utterance": "цитата ≤150 символов",
    "cognitive_weight": 1|3|5,
    "source": "iwe",
    "ts": "2026-04-13T10:00:00Z"
  },
  "repo_ctx": {...}
}

Инварианты (WP-206 Ф7 §2a):
- event_type должен быть в DECISION_EVENT_TYPES allowlist
- user_utterance обязателен (schema-фильтр §4a)
- Без user_utterance → ParsedEvent = None (событие отклонено как не-решение)
- source = 'iwe', user_ref через session_id (нет прямого user_id → нужен ory_uuid из сессии)
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Allowlist: только решения пользователя (WP-206 Ф7 §2a)
DECISION_EVENT_TYPES = frozenset({
    "decision_approve",
    "decision_reject",
    "decision_redirect",
    "decision_architectural",
    "decision_strategic",
})

# cognitive_weight по умолчанию если не передан детектором
_DEFAULT_WEIGHT: dict[str, int] = {
    "decision_approve": 1,
    "decision_reject": 3,
    "decision_redirect": 3,
    "decision_architectural": 5,
    "decision_strategic": 5,
}


@dataclass
class ParsedEvent:
    external_id: str
    event_type: str
    user_ref: dict       # {'session_id': ...} — identity resolves по ory_uuid сессии (v2)
    occurred_at: datetime
    payload: dict
    confidence: float = 1.0


def parse_decision(payload: dict) -> Optional[ParsedEvent]:
    """Распарсить IWE decision payload → ParsedEvent.

    payload — сырой dict из raw_events.payload (то, что capture_writer записал).
    Возвращает None если payload невалиден (нет event_type или user_utterance).

    Логика idempotency key (external_id):
    - session_id + event_type + ts → уникальная комбинация для dedup в user_events.
    - Нет session_id → ts + event_type + utterance[:20] (резервный ключ).
    """
    # Распаковываем вложенную структуру от capture_writer
    # payload может быть либо nested {"event_type": ..., "payload": {...}}
    # либо уже плоским {"session_id": ..., "user_utterance": ...}
    inner = payload
    if "payload" in payload and isinstance(payload["payload"], dict):
        inner = payload["payload"]
        # event_type в outer или inner
        if "event_type" not in inner and "event_type" in payload:
            inner["event_type"] = payload["event_type"]

    event_type = inner.get("event_type", "")
    if event_type not in DECISION_EVENT_TYPES:
        logger.warning(
            "iwe_decisions transform: unknown or disallowed event_type=%r. "
            "Allowlist: %s", event_type, DECISION_EVENT_TYPES
        )
        return None

    user_utterance = inner.get("user_utterance", "").strip()
    if not user_utterance:
        logger.warning(
            "iwe_decisions transform: missing user_utterance in payload "
            "(schema-filter §4a). Rejecting as non-decision. payload=%s", inner
        )
        return None

    session_id = inner.get("session_id", "")
    ts_str = inner.get("ts", "")
    cognitive_weight = int(inner.get("cognitive_weight", _DEFAULT_WEIGHT.get(event_type, 1)))

    # occurred_at
    occurred_at: Optional[datetime] = None
    if ts_str:
        try:
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            occurred_at = dt.astimezone(timezone.utc).replace(tzinfo=None)
        except ValueError:
            pass
    if occurred_at is None:
        occurred_at = datetime.utcnow()

    # external_id (dedup key)
    if session_id and ts_str:
        external_id = f"{event_type}:{session_id}:{ts_str}"
    else:
        slug = user_utterance[:20].replace(" ", "_")
        external_id = f"{event_type}:{occurred_at.strftime('%Y%m%dT%H%M%S')}:{slug}"

    # user_ref — предпочитаем ory_uuid (прямой резолвинг, v1: single-user IWE).
    # Fallback: session_id → identity_map (requires session mapping table).
    ory_uuid = inner.get("ory_uuid", "").strip()
    user_ref: dict
    if ory_uuid:
        user_ref = {"ory_uuid": ory_uuid}
    elif session_id:
        user_ref = {"session_id": session_id}
    else:
        logger.warning("iwe_decisions transform: no ory_uuid or session_id in payload, skip.")
        return None

    silver_payload = {
        "event_type": event_type,
        "user_utterance": user_utterance,
        "cognitive_weight": cognitive_weight,
        "session_id": session_id,
    }

    return ParsedEvent(
        external_id=external_id,
        event_type=event_type,
        user_ref=user_ref,
        occurred_at=occurred_at,
        payload=silver_payload,
        confidence=1.0,
    )
