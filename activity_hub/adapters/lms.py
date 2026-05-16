"""LMS Adapter — два режима работы:

1. **Bulk** (v2, основной): `/integration-json/passing-actions` без фильтра user-ids.
   Возвращает все действия всех пользователей с email. Cookie-auth (login → session).
   Согласован с Димой 7 апр 2026.

2. **Per-user** (v1, legacy): `/courses/passing-actions?user-ids=...`.
   Basic Auth, батчи по 100 user-ids. Используется для reconciliation.

Прод: https://aisystant.system-school.ru/systemschool/api/
Auth: Cookie (login → session-token) для bulk, Basic Auth для per-user.
Dedup key: f"{action}:{actionId}" (или f"{action}:{datetime}" для действий без actionId)
"""

import asyncio
import logging
import re
from base64 import b64encode
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from activity_hub.core.landing import RawItem
from activity_hub.core.models import RawEvent

logger = logging.getLogger(__name__)

# Маппинг LMS action → наш event_type
ACTION_TYPE_MAP = {
    "TEXT": "text_submitted",
    "TABLE": "table_submitted",
    "TEST": "test_passed",
    "TASK": "task_submitted",
    "AI": "ai_interaction",
    "CREATE_TOPIC": "topic_created",
    "CREATE_COMMENT": "comment_created",
    "POMODORO": "pomodoro_completed",
}


class LMSAdapter:
    """Адаптер для LMS Aisystant API."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: int = 90,
    ):
        self.base_url = base_url.rstrip("/")
        self._username = username
        self._password = password
        self._auth_header = "Basic " + b64encode(
            f"{username}:{password}".encode()
        ).decode()
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    @staticmethod
    def _parse_timestamp(dt_str: str) -> Optional[datetime]:
        """Парсинг LMS timestamp → UTC naive datetime.

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
            # Отделить дробные от offset (+/-)
            m = re.match(r'(\d+)(.*)', rest)
            if m:
                frac = m.group(1)[:6].ljust(6, '0')
                suffix = m.group(2)
                clean = f"{base}.{frac}{suffix}"

        try:
            dt = datetime.fromisoformat(clean)
        except ValueError:
            return None

        # Привести к UTC naive (для совместимости с datetime.utcnow() в integrity.py)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    def _headers(self) -> dict:
        return {
            "Authorization": self._auth_header,
            "Accept": "application/json",
        }

    @staticmethod
    def _external_id(action: dict) -> Optional[str]:
        """Dedup-ключ для bronze: f"{action}:{actionId}" или f"{action}:{datetime}".

        Единственная деривация из payload, которую делает adapter перед landing.
        Всё остальное (event_type, user_ref, occurred_at) — работа transform-worker.
        """
        action_type = action.get("action", "")
        if not action_type:
            return None
        action_id = action.get("actionId")
        if action_id:
            return f"{action_type}:{action_id}"
        dt_str = action.get("datetime", "")
        if not dt_str:
            return None
        return f"{action_type}:{dt_str}"

    async def fetch_passing_actions(
        self,
        session: aiohttp.ClientSession,
        user_ids: list[int],
        from_date: datetime,
        to_date: datetime,
    ) -> tuple[list[RawEvent], list[RawItem]]:
        """Забрать действия пользователей за период.

        GET /courses/passing-actions?from=&to=&user-ids=
        Ответ: { "userId": { "YYYY-MM-DD": [ {action} ] } }

        Returns: (events, raw_items). events — legacy silver (RawEvent,
        распарсенные через _parse_action для hub.ingest_batch). raw_items —
        bronze (сырой action + external_id для landing.write_raw). Оба пути
        заполняются одним проходом по ответу LMS, но попадают в разные
        таблицы: raw_events (bronze) и user_events (silver). Dual-write
        существует до Ф8.4 (transform-worker), потом silver-путь уходит
        в transform-worker.
        """
        params = {
            "from": from_date.strftime("%Y-%m-%d"),
            "to": to_date.strftime("%Y-%m-%d"),
            "user-ids": ",".join(str(uid) for uid in user_ids),
        }

        url = f"{self.base_url}/courses/passing-actions"
        events: list[RawEvent] = []
        raw_items: list[RawItem] = []

        # Retry с экспоненциальным backoff на transient ошибках (timeout, 5xx, разрыв соединения).
        # Без ретрая таймаут приводит к тихой потере данных — инцидент 9 апр.
        max_attempts = 3
        backoff_s = [1, 3]

        for attempt in range(1, max_attempts + 1):
            try:
                async with session.get(
                    url, params=params, headers=self._headers(), timeout=self.timeout
                ) as resp:
                    if resp.status >= 500:
                        logger.warning(
                            "LMS API %s returned %d (attempt %d/%d)",
                            url, resp.status, attempt, max_attempts,
                        )
                        if attempt < max_attempts:
                            await asyncio.sleep(backoff_s[attempt - 1])
                            continue
                        logger.error("LMS API %s returned %d — giving up", url, resp.status)
                        return [], []

                    if resp.status != 200:
                        logger.error("LMS API %s returned %d", url, resp.status)
                        return [], []

                    data = await resp.json()

                    # Ответ: { "userId": { "date": [ actions ] } }
                    for user_id_str, dates in data.items():
                        if not isinstance(dates, dict):
                            continue
                        user_id = int(user_id_str)
                        for date_str, actions in dates.items():
                            for action in actions:
                                ext_id = self._external_id(action)
                                if ext_id:
                                    raw_items.append(RawItem(
                                        external_id=ext_id,
                                        payload=action,
                                    ))
                                event = self._parse_action(action, user_id)
                                if event:
                                    events.append(event)
                    return events, raw_items

            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                logger.warning(
                    "LMS API transient error on %s: %s: %s (attempt %d/%d)",
                    url, type(e).__name__, e, attempt, max_attempts,
                )
                if attempt < max_attempts:
                    await asyncio.sleep(backoff_s[attempt - 1])
                    continue
                logger.error(
                    "LMS API error: %s — %s: %s (giving up after %d attempts)",
                    url, type(e).__name__, e, max_attempts,
                    exc_info=True,
                )
                return [], []
            except Exception as e:
                logger.error(
                    "LMS API unexpected error: %s — %s: %s",
                    url, type(e).__name__, e,
                    exc_info=True,
                )
                return [], []

        return events, raw_items

    def _parse_action(self, action: dict, user_id: int) -> Optional[RawEvent]:
        """Преобразовать LMS action → RawEvent."""
        action_type = action.get("action", "")
        action_id = action.get("actionId")
        dt_str = action.get("datetime", "")

        if not action_type or not dt_str:
            logger.warning("LMS action missing action or datetime: %s", action)
            return None

        # Dedup key: action:actionId (или action:datetime для действий без actionId)
        if action_id:
            external_id = f"{action_type}:{action_id}"
        else:
            external_id = f"{action_type}:{dt_str}"

        # Маппинг типа
        event_type = ACTION_TYPE_MAP.get(action_type, action_type.lower())

        # Timestamp — нормализовать и привести к UTC naive
        occurred_at = self._parse_timestamp(dt_str)
        if occurred_at is None:
            logger.warning("LMS: cannot parse timestamp: %s", dt_str)
            return None

        # Payload — всё кроме уже извлечённых полей
        payload = {k: v for k, v in action.items()
                   if k not in ("action", "actionId", "datetime", "email")}

        # user_ref: email (для bulk) или lms_user_id (для per-user)
        email = action.get("email")
        if email:
            user_ref = {"email": email, "lms_user_id": user_id}
        else:
            user_ref = {"lms_user_id": user_id}

        return RawEvent(
            source="lms",
            external_id=external_id,
            user_ref=user_ref,
            event_type=event_type,
            payload=payload,
            confidence=0.9,
            occurred_at=occurred_at,
        )

    async def fetch_all_users(
        self,
        session: aiohttp.ClientSession,
        user_ids: list[int],
        from_date: datetime,
        to_date: datetime,
        batch_size: int = 20,
    ) -> tuple[list[RawEvent], list[RawItem]]:
        """Забрать действия пользователей батчами по batch_size (legacy per-user endpoint).

        batch_size=20 — компромисс между connection overhead и LMS response time.
        Снижено с 100 после инцидента 9 апр: 69 user_ids за 7 дней → >30s → timeout.

        Returns: (events, raw_items) — dual-write для Ф8 (bronze + legacy silver).
        """
        all_events: list[RawEvent] = []
        all_raw: list[RawItem] = []

        for i in range(0, len(user_ids), batch_size):
            batch = user_ids[i : i + batch_size]
            logger.info("LMS batch %d-%d of %d users",
                        i + 1, min(i + batch_size, len(user_ids)), len(user_ids))

            events, raw = await self.fetch_passing_actions(session, batch, from_date, to_date)
            all_events.extend(events)
            all_raw.extend(raw)

        logger.info("LMS total: %d events, %d raw items from %d users",
                    len(all_events), len(all_raw), len(user_ids))
        return all_events, all_raw

    async def _login(self, session: aiohttp.ClientSession) -> None:
        """Cookie-based login для bulk endpoint."""
        # base_url = .../systemschool/api → login = .../systemschool/api/auth/login
        login_url = f"{self.base_url.rstrip('/')}/auth/login"

        async with session.post(
            login_url,
            json={
                "user": self._username,
                "password": self._password,
                "rememberMe": True,
            },
            timeout=self.timeout,
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"LMS login failed: HTTP {resp.status}")
            logger.info("LMS cookie login ok")

    async def fetch_bulk(
        self,
        session: aiohttp.ClientSession,
        from_date: datetime,
        to_date: datetime,
    ) -> tuple[list[RawEvent], list[RawItem], dict[str, int]]:
        """Bulk fetch: все пользователи, все действия за период.

        Endpoint: /integration-json/passing-actions?from=...&to=...
        Ответ: [ { userId, email, action, actionId, datetime, ... } ]

        Returns: (events, raw_items, email_to_lms_user_id mapping).
        events — silver (RawEvent). raw_items — bronze (landing). Dual-write.
        """
        await self._login(session)

        # /systemschool/api → /systemschool/integration-json/passing-actions
        base = self.base_url
        if base.endswith("/api"):
            base = base[:-4]
        elif "/api/" in base:
            base = base.split("/api/")[0]
        bulk_url = base + "/integration-json/passing-actions"

        params = {
            "from": from_date.strftime("%Y-%m-%dT00:00:00Z"),
            "to": to_date.strftime("%Y-%m-%dT23:59:59Z"),
        }

        events: list[RawEvent] = []
        raw_items: list[RawItem] = []
        email_to_lms_id: dict[str, int] = {}

        try:
            async with session.get(
                bulk_url, params=params, timeout=self.timeout
            ) as resp:
                if resp.status != 200:
                    logger.error("LMS bulk API returned %d", resp.status)
                    return [], [], {}

                data = await resp.json()
                if not isinstance(data, list):
                    logger.error("LMS bulk API: expected list, got %s", type(data))
                    return [], [], {}

                for action in data:
                    user_id = action.get("userId")
                    email = action.get("email")

                    if email and user_id:
                        email_to_lms_id[email.lower().strip()] = int(user_id)

                    ext_id = self._external_id(action)
                    if ext_id:
                        raw_items.append(RawItem(
                            external_id=ext_id,
                            payload=action,
                        ))

                    event = self._parse_action(action, int(user_id) if user_id else 0)
                    if event:
                        events.append(event)

        except Exception as e:
            logger.error("LMS bulk API error: %s", e)

        logger.info("LMS bulk: %d events, %d raw items, %d unique email→lms_id mappings",
                     len(events), len(raw_items), len(email_to_lms_id))
        return events, raw_items, email_to_lms_id
