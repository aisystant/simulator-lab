"""IWE Adapter (серверный) — сбор фактов из IWE через GitHub API + WakaTime API.

Архитектура (АрхГейт 8.4, решение 19 мар 2026):
- Адаптер работает на СЕРВЕРЕ, не на машине пользователя
- Токены хранятся в Neon (persona.user_integrations)
- Пользователь подключается через бота: /connect github, /connect wakatime
- Адаптер итерирует по всем подключённым пользователям

Принцип: записываем ФАКТ состоявшегося события. Баллы = WP-121 (отдельно).

Запуск: python runner.py sync-iwe [--from-date ...] [--to-date ...]
"""

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import aiohttp
import asyncpg

from activity_hub.core.models import RawEvent

logger = logging.getLogger(__name__)


@dataclass
class UserIntegration:
    """Подключение пользователя к внешнему сервису."""
    user_uuid: str
    service: str
    access_token: str
    metadata: dict


class IWEAdapter:
    """Серверный адаптер для сбора IWE-событий через API."""

    GITHUB_API = "https://api.github.com"
    WAKATIME_API = "https://wakatime.com/api/v1"

    def __init__(self, pool: asyncpg.Pool, encryption_key: str = ""):
        self.pool = pool
        # WP-253 Gap 1 fix: ключ для расшифровки GitHub OAuth токенов.
        # Токены в формате 'pgp:' + base64(pgp_sym_encrypt(token, key)).
        # Старые plaintext строки (без 'pgp:' префикса) читаются без расшифровки.
        self._encryption_key = encryption_key

    # --- Integration management ---

    async def get_connected_users(self) -> list[UserIntegration]:
        """Получить всех пользователей с активными GitHub-интеграциями."""
        async with self.pool.acquire() as conn:
            if self._encryption_key:
                rows = await conn.fetch(
                    """
                    SELECT account_id AS user_uuid, service,
                        CASE WHEN access_token LIKE 'pgp:%'
                            THEN pgp_sym_decrypt(decode(substring(access_token FROM 5), 'base64'), $1)::text
                            ELSE access_token
                        END AS access_token,
                        metadata
                    FROM user_integrations
                    WHERE active = TRUE AND service = 'github'
                    """,
                    self._encryption_key,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT account_id AS user_uuid, service, access_token, metadata
                    FROM user_integrations
                    WHERE active = TRUE AND service = 'github'
                    """
                )
            return [
                UserIntegration(
                    user_uuid=str(r["user_uuid"]),
                    service=r["service"],
                    access_token=r["access_token"],
                    metadata=json.loads(r["metadata"]) if isinstance(r["metadata"], str) else (r["metadata"] or {}),
                )
                for r in rows
            ]

    async def get_wakatime_token(self, user_uuid: str) -> Optional[str]:
        """Получить WakaTime token для пользователя."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT access_token FROM user_integrations
                WHERE account_id = $1 AND service = 'wakatime' AND active = TRUE
                """,
                user_uuid if isinstance(user_uuid, str) else str(user_uuid),
            )
            return row["access_token"] if row else None

    # --- Main entry point ---

    async def collect_all_users(
        self,
        session: aiohttp.ClientSession,
        from_date: datetime,
        to_date: datetime,
    ) -> list[RawEvent]:
        """Собрать события для ВСЕХ подключённых пользователей."""
        users = await self.get_connected_users()
        logger.info("IWE sync: %d connected users", len(users))

        all_events = []
        for user in users:
            user_ref = {"ory_uuid": user.user_uuid}
            try:
                events = await self.collect_user_events(
                    session, user, from_date, to_date, user_ref,
                )
                all_events.extend(events)
                logger.info(
                    "IWE user %s: %d events",
                    user.metadata.get("github_username", user.user_uuid[:8]),
                    len(events),
                )
            except Exception as e:
                logger.error("IWE user %s failed: %s", user.user_uuid[:8], e)

        return all_events

    async def collect_user_events(
        self,
        session: aiohttp.ClientSession,
        user: UserIntegration,
        from_date: datetime,
        to_date: datetime,
        user_ref: dict,
    ) -> list[RawEvent]:
        """Собрать все события одного пользователя."""
        events = []

        # GitHub: коммиты + PR
        gh_username = user.metadata.get("github_username")
        if gh_username:
            repos = await self._github_list_repos(session, user.access_token, gh_username)
            for repo in repos:
                commits = await self._github_get_commits(
                    session, user.access_token, repo, gh_username, from_date, to_date,
                )
                events.extend(self._parse_commits(commits, repo, user_ref))

                # FMT merged PRs
                if repo["name"].startswith("FMT-"):
                    prs = await self._github_get_merged_prs(
                        session, user.access_token, repo, from_date, to_date,
                    )
                    events.extend(self._parse_fmt_prs(prs, repo, user_ref))

        # WakaTime
        waka_token = await self.get_wakatime_token(user.user_uuid)
        if waka_token:
            events.extend(
                await self._collect_coding_time(session, waka_token, from_date, to_date, user_ref)
            )

        return events

    # --- GitHub API ---

    # Префиксы IWE-репо (DS-*, PACK-*, FMT-*, SPF-*, FPF-*, ZP-*)
    IWE_REPO_PREFIXES = ("DS-", "PACK-", "FMT-", "SPF-", "FPF-", "ZP-")

    async def _github_list_repos(
        self,
        session: aiohttp.ClientSession,
        token: str,
        username: str,
    ) -> list[dict]:
        """Получить список IWE-репо пользователя.

        Фильтрует по префиксам (DS-*, PACK-*, FMT-*) — не запрашивает
        коммиты для 100+ нерелевантных репо.
        """
        all_repos = []
        page = 1
        while True:
            async with session.get(
                f"{self.GITHUB_API}/user/repos",
                headers=self._gh_headers(token),
                params={"per_page": 100, "page": page, "type": "all"},
            ) as resp:
                if resp.status != 200:
                    logger.warning("GitHub repos API returned %d", resp.status)
                    break
                data = await resp.json()
                if not data:
                    break
                all_repos.extend(data)
                if len(data) < 100:
                    break
                page += 1

        # Фильтр: только IWE-репо
        iwe_repos = [
            r for r in all_repos
            if any(r["name"].startswith(p) for p in self.IWE_REPO_PREFIXES)
        ]

        logger.info("GitHub: %d IWE repos (из %d всего) для %s", len(iwe_repos), len(all_repos), username)
        return iwe_repos

    async def _github_get_commits(
        self,
        session: aiohttp.ClientSession,
        token: str,
        repo: dict,
        username: str,
        from_date: datetime,
        to_date: datetime,
    ) -> list[dict]:
        """Получить коммиты в репо за период.

        Не фильтруем по author — GitHub API для приватных репо
        требует git email, а не username. Вместо этого берём все коммиты
        (в персональных репо они и так от владельца).
        """
        full_name = repo["full_name"]
        async with session.get(
            f"{self.GITHUB_API}/repos/{full_name}/commits",
            headers=self._gh_headers(token),
            params={
                "since": from_date.isoformat() + "Z",
                "until": to_date.isoformat() + "Z",
                "per_page": 100,
            },
        ) as resp:
            if resp.status != 200:
                return []
            return await resp.json()

    async def _github_get_merged_prs(
        self,
        session: aiohttp.ClientSession,
        token: str,
        repo: dict,
        from_date: datetime,
        to_date: datetime,
    ) -> list[dict]:
        """Получить merged PRs в FMT-* репо."""
        full_name = repo["full_name"]
        async with session.get(
            f"{self.GITHUB_API}/repos/{full_name}/pulls",
            headers=self._gh_headers(token),
            params={"state": "closed", "per_page": 50, "sort": "updated", "direction": "desc"},
        ) as resp:
            if resp.status != 200:
                return []
            prs = await resp.json()
            # Фильтр: только merged и в нужном периоде
            result = []
            for pr in prs:
                if not pr.get("merged_at"):
                    continue
                try:
                    merged_at = datetime.fromisoformat(pr["merged_at"].replace("Z", "+00:00")).replace(tzinfo=None)
                except ValueError:
                    continue
                if from_date <= merged_at <= to_date:
                    result.append(pr)
            return result

    @staticmethod
    def _gh_headers(token: str) -> dict:
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    # --- Commit parsing (та же логика, что и в локальном адаптере) ---

    def _parse_commits(
        self,
        commits: list[dict],
        repo: dict,
        user_ref: dict,
    ) -> list[RawEvent]:
        """Парсинг коммитов → события разных типов."""
        events = []
        repo_name = repo["name"]
        full_name = repo["full_name"]

        seen_day_open: set[str] = set()
        seen_day_close: set[str] = set()

        for c in commits:
            sha = c.get("sha", "")
            commit_data = c.get("commit", {})
            message = commit_data.get("message", "").split("\n")[0]  # первая строка
            date_str = commit_data.get("author", {}).get("date", "")

            try:
                occurred = datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)
            except (ValueError, AttributeError):
                occurred = datetime.utcnow()

            date_key = occurred.strftime("%Y-%m-%d")

            # 1. commit_created (каждый коммит)
            events.append(RawEvent(
                source="iwe",
                external_id=f"commit:{sha}",
                user_ref=user_ref,
                event_type="commit_created",
                payload={"repo": repo_name, "message": message[:200]},
                confidence=1.0,
                occurred_at=occurred,
            ))

            # Дальше — только для strategy-репо
            is_strategy = "strategy" in repo_name.lower()

            # 2. day_open (max 1 per day, только strategy)
            if is_strategy and date_key not in seen_day_open:
                if (
                    re.search(r"^Day\s+(?:Open|Plan)\b", message, re.IGNORECASE)
                    or re.search(r"(?:Day\s*(?:Open|Plan).*(?:creat|\u0441\u043e\u0437\u0434\u0430\u043d))", message, re.IGNORECASE)
                ):
                    seen_day_open.add(date_key)
                    events.append(RawEvent(
                        source="iwe",
                        external_id=f"day_open:{date_key}",
                        user_ref=user_ref,
                        event_type="day_open",
                        payload={"repo": repo_name, "date": date_key},
                        confidence=1.0,
                        occurred_at=occurred,
                    ))

            # 3. day_close (max 1 per day, только strategy)
            if is_strategy and date_key not in seen_day_close:
                if re.search(r"Day\s*Close", message, re.IGNORECASE):
                    seen_day_close.add(date_key)
                    events.append(RawEvent(
                        source="iwe",
                        external_id=f"day_close:{date_key}",
                        user_ref=user_ref,
                        event_type="day_close",
                        payload={"repo": repo_name, "date": date_key},
                        confidence=1.0,
                        occurred_at=occurred,
                    ))

            # 4. week_plan_created (strategy)
            if is_strategy and re.search(r"Week\s*Plan", message, re.IGNORECASE):
                events.append(RawEvent(
                    source="iwe",
                    external_id=f"week_plan:{sha}",
                    user_ref=user_ref,
                    event_type="week_plan_created",
                    payload={"repo": repo_name},
                    confidence=1.0,
                    occurred_at=occurred,
                ))

            # 5. wp_completed (любой репо)
            wp_matches = re.findall(
                r"(?:WP|\u0420\u041f)[- ]?#?(\d+).*(?:done|\u0437\u0430\u043a\u0440\u044b\u0442|\u2705|close)",
                message, re.IGNORECASE,
            )
            for wp_num in wp_matches:
                events.append(RawEvent(
                    source="iwe",
                    external_id=f"wp_done:{wp_num}:{sha}",
                    user_ref=user_ref,
                    event_type="wp_completed",
                    payload={"wp_number": int(wp_num), "repo": repo_name},
                    confidence=1.0,
                    occurred_at=occurred,
                ))

            # 6. content_published (Knowledge-Index)
            if "Knowledge-Index" in repo_name:
                if re.search(r"(?:\u043f\u043e\u0441\u0442|publish|\u043e\u043f\u0443\u0431\u043b\u0438\u043a)", message, re.IGNORECASE):
                    events.append(RawEvent(
                        source="iwe",
                        external_id=f"content_pub:{sha}",
                        user_ref=user_ref,
                        event_type="content_published",
                        payload={"repo": repo_name, "message": message[:200]},
                        confidence=0.9,
                        occurred_at=occurred,
                    ))

            # 7. knowledge_extracted + pack_updated (PACK-*)
            if repo_name.startswith("PACK-"):
                events.append(RawEvent(
                    source="iwe",
                    external_id=f"ke:{sha}",
                    user_ref=user_ref,
                    event_type="knowledge_extracted",
                    payload={"pack": repo_name, "message": message[:200]},
                    confidence=1.0,
                    occurred_at=occurred,
                ))
                events.append(RawEvent(
                    source="iwe",
                    external_id=f"pack_upd:{sha}",
                    user_ref=user_ref,
                    event_type="pack_updated",
                    payload={"pack": repo_name, "message": message[:200]},
                    confidence=1.0,
                    occurred_at=occurred,
                ))

            # 8. note_to_capture (strategy, captures.md)
            if is_strategy and ("capture" in message.lower() or "ke" in message.lower()):
                events.append(RawEvent(
                    source="iwe",
                    external_id=f"capture:{sha}",
                    user_ref=user_ref,
                    event_type="note_to_capture",
                    payload={"repo": repo_name},
                    confidence=0.9,
                    occurred_at=occurred,
                ))

            # 9. week_plan_closed (strategy, "Week Close")
            # Gap-А: если в сообщении есть маркер q:N (0-5) — включить в payload
            # Пример: "docs(W20): Week Close q:4" → quality=4
            if is_strategy and re.search(r"Week[\s\-]*Close", message, re.IGNORECASE):
                wc_payload: dict = {"repo": repo_name, "message": message[:200]}
                _qm = re.search(r"\bq:([0-5])\b", message)
                if _qm:
                    wc_payload["quality"] = int(_qm.group(1))
                events.append(RawEvent(
                    source="iwe",
                    external_id=f"week_close:{sha}",
                    user_ref=user_ref,
                    event_type="week_plan_closed",
                    payload=wc_payload,
                    confidence=1.0,
                    occurred_at=occurred,
                ))

            # 10. month_plan_closed (strategy, "Month Close")
            if is_strategy and re.search(r"Month[\s\-]*Close", message, re.IGNORECASE):
                events.append(RawEvent(
                    source="iwe",
                    external_id=f"month_close:{sha}",
                    user_ref=user_ref,
                    event_type="month_plan_closed",
                    payload={"repo": repo_name, "message": message[:200]},
                    confidence=1.0,
                    occurred_at=occurred,
                ))

            # 11. strategy_session_completed (strategy, "Strategy Session")
            if is_strategy and re.search(r"Strategy[\s\-]*Session", message, re.IGNORECASE):
                events.append(RawEvent(
                    source="iwe",
                    external_id=f"strategy_session:{sha}",
                    user_ref=user_ref,
                    event_type="strategy_session_completed",
                    payload={"repo": repo_name, "message": message[:200]},
                    confidence=1.0,
                    occurred_at=occurred,
                ))

        return events

    def _parse_fmt_prs(
        self,
        prs: list[dict],
        repo: dict,
        user_ref: dict,
    ) -> list[RawEvent]:
        """FMT merged PRs → fmt_commit_merged."""
        events = []
        full_name = repo["full_name"]
        for pr in prs:
            try:
                merged_at = datetime.fromisoformat(
                    pr["merged_at"].replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except (ValueError, KeyError):
                continue

            events.append(RawEvent(
                source="iwe",
                external_id=f"fmt_pr:{full_name}:{pr['number']}",
                user_ref=user_ref,
                event_type="fmt_commit_merged",
                payload={
                    "repo": full_name,
                    "pr_number": pr["number"],
                    "title": pr.get("title", "")[:200],
                    "author": pr.get("user", {}).get("login", ""),
                },
                confidence=1.0,
                occurred_at=merged_at,
            ))
        return events

    # --- WakaTime API ---

    async def _collect_coding_time(
        self,
        session: aiohttp.ClientSession,
        token: str,
        from_date: datetime,
        to_date: datetime,
        user_ref: dict,
    ) -> list[RawEvent]:
        """WakaTime: время за каждый день в периоде.

        WakaTime API использует Basic Auth: base64(api_key).
        Аналог fetch-wakatime.sh.
        """
        import base64
        # waka_tok_* = OAuth token → Bearer. Plain API key → Basic base64.
        if token.startswith("waka_tok_"):
            auth_header = f"Bearer {token}"
        else:
            auth_header = "Basic " + base64.b64encode(token.encode()).decode()

        events = []
        current = from_date
        while current.date() <= to_date.date():
            date_str = current.strftime("%Y-%m-%d")
            try:
                async with session.get(
                    f"{self.WAKATIME_API}/users/current/summaries",
                    headers={"Authorization": auth_header},
                    params={"start": date_str, "end": date_str},
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        summaries = data.get("data", [])
                        if summaries:
                            total_seconds = summaries[0].get("grand_total", {}).get("total_seconds", 0)
                            if total_seconds > 0:
                                # Для прошлых дней — конец дня. Для сегодня — текущее время
                                # (иначе timestamp sanity check отсечёт как future)
                                now = datetime.utcnow()
                                if current.date() >= now.date():
                                    event_time = now
                                else:
                                    event_time = datetime(current.year, current.month, current.day, 23, 59, 59)

                                events.append(RawEvent(
                                    source="iwe",
                                    external_id=f"wakatime:{user_ref['ory_uuid']}:{date_str}",
                                    user_ref=user_ref,
                                    event_type="coding_time",
                                    payload={
                                        "date": date_str,
                                        "total_seconds": total_seconds,
                                        "human_readable": summaries[0].get("grand_total", {}).get("text", ""),
                                    },
                                    confidence=1.0,
                                    occurred_at=event_time,
                                ))
                    elif resp.status == 429:
                        logger.warning("WakaTime %s rate-limited (429), sleeping 60s", date_str)
                        await asyncio.sleep(60)
                        continue
                    else:
                        logger.warning("WakaTime %s returned %d", date_str, resp.status)
            except Exception as e:
                logger.warning("WakaTime fetch failed for %s: %s", date_str, e)

            current += timedelta(days=1)
            await asyncio.sleep(1.2)

        return events
