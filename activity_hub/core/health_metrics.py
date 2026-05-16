"""Health metrics writer — best-effort INSERT в health.internal_metrics.

Конвенция совпадает с event-gateway/src/db.ts (writeInternalMetric):
  - Worker name = константа на воркер (DP.ROLE.032 / DP.ROLE.NNN).
  - Best-effort: ошибка writer'а НЕ валит основной flow (log.warning, continue).
  - Schema fallback: HEALTH_DB_SCHEMA env var → default "health".

Читатели:
  - alerter.py (multi-domain-projection-worker) — детект stall по freshness measured_at.
  - Grafana dashboard (aist_bot_newarchitecture/monitoring/grafana-dashboard.json).
  - reliability gate verifier (CI).

Source-of-truth схемы: DS-IT-systems/neon-migrations/mvp/005-health-internal-metrics.sql
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import asyncpg

log = logging.getLogger(__name__)


def _health_schema() -> str:
    return os.environ.get("HEALTH_DB_SCHEMA", "health")


async def write_internal_metric(
    conn: asyncpg.Connection,
    *,
    metric_name: str,
    worker: str,
    value_numeric: float | None = None,
    value_jsonb: dict[str, Any] | None = None,
) -> None:
    """Best-effort INSERT в health.internal_metrics.

    Args:
        conn: открытое asyncpg connection к learning DB (там живёт health schema).
        metric_name: например 'profiler_neon_batch_processed', 'points_neon_earned'.
        worker: имя воркера, например 'profiler-subscriber-neon'.
        value_numeric: числовое значение метрики.
        value_jsonb: дополнительный контекст (опц.) — батч-статистика, флаги.
    """
    schema = _health_schema()
    payload = json.dumps(value_jsonb) if value_jsonb is not None else None
    try:
        await conn.execute(
            f"""
            INSERT INTO {schema}.internal_metrics
                (metric_name, worker, value_numeric, value_jsonb)
            VALUES ($1, $2, $3, $4::jsonb)
            """,
            metric_name,
            worker,
            value_numeric,
            payload,
        )
    except Exception as exc:
        # Health-метрика не должна валить hot-path. Warn в лог — Railway logs заберёт.
        log.warning(
            "[health.internal_metrics] write failed: metric=%s worker=%s: %s",
            metric_name,
            worker,
            exc,
        )
