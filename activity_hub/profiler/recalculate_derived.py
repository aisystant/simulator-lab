#!/usr/bin/env python3
"""
recalculate_derived.py — standalone runtime R28 Profiler (WP-218).

Единая точка расчёта 3_derived из 2_collected в digital_twins (Neon).

Принципы WP-218 (ADR 7 принципов):
  1. ЦД = единственный калькулятор (этот скрипт — единственный писатель 3_derived).
  2. Метамодель = реестр индикаторов.
  3. Метамодельная трассируемость (выход calculate_derived идёт в 3_* по IND-кодам).
  4. Stateless-интерфейсы (читаем БД, считаем, пишем — без in-memory state).
  5. Single writer в 3_derived (только этот runtime, никаких параллельных вычислителей).
  6. Event-driven целевой режим (пока — manual/cron).
  7. Мёртвого кода не оставлять (calculate_derived удаляется из бота параллельным коммитом).

Отличия от sync_engagement_to_dt (который остаётся в боте как collector):
  - Не читает engagement view (это работа collector'а).
  - Не пишет в 2_collected (это работа collector'ов — бот, dt-collect-neon, collectors.d).
  - Итерирует ВСЕ digital_twins.data где есть 2_collected, независимо от свежести events.
  - Читает ПОЛНЫЙ data['2_collected'] из БД — включая секции от других writers
    (2_5_community, 2_6_coding, 2_7_iwe, 2_8_ecosystem, 2_9_knowledge и т.д.).
  - Читает learning_history напрямую (BKT mastery).
  - Пишет ТОЛЬКО в data['3_derived'] через SQL deep merge.

Использование:
    # Полный пересчёт всех пользователей в Neon prod
    NEON_URL="postgresql://..." python3 recalculate_derived.py

    # Пересчёт одного пользователя
    NEON_URL="postgresql://..." python3 recalculate_derived.py --user-id 25d91dbb-...

    # Dry-run (показать новые значения, не писать в БД)
    NEON_URL="postgresql://..." python3 recalculate_derived.py --dry-run --user-id 25d91dbb-...

Env:
    NEON_URL — connection string к digital_twins БД (обязательно)

Зависимости:
    psycopg2-binary (sync, чтобы не плодить asyncio в CLI)

WP-218 Ф1-Ф2: вынос runtime из бота.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

# Локальный импорт библиотеки dt_calc (из той же директории)
sys.path.insert(0, str(Path(__file__).parent))
from dt_calc import calculate_derived  # noqa: E402

_log = logging.getLogger(__name__)

# F2 dual-write counters (WP-253, 28 апр) — module-level state для финальной сводки.
# NOT THREAD-SAFE: только sync sequential runs (текущая модель cron 04:30 MSK).
# При параллельном запуске (asyncio.gather, ProcessPool) counters race — refactor нужен.
_dual_write_stats = {"ok": 0, "failed": 0, "skipped": 0}


def _connect(neon_url: str):
    """Подключиться к Neon через psycopg2 (sync)."""
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        _log.critical("psycopg2-binary not installed. Run: pip install psycopg2-binary")
        sys.exit(2)

    conn = psycopg2.connect(neon_url)
    conn.autocommit = False
    return conn


def _load_learning_history(conn, user_uuid: str) -> list[dict] | None:
    """Загрузить learning_history v2 для BKT mastery расчёта.

    Returns:
        list[dict] в формате, который ожидает calc_mastery_by_area, или None.
    """
    try:
        cur = conn.cursor()
        # Используем savepoint чтобы откатить только этот SELECT, не трогая
        # накопленные _write_derived в текущей транзакции (WP-227).
        cur.execute("SAVEPOINT lh_check")
        cur.execute('''
            SELECT element_id, element_type, area, depth, passed, created_at
            FROM development.learning_history
            WHERE schema_version = 2
              AND user_uuid::text = %s
              AND element_id IS NOT NULL
            ORDER BY created_at DESC
        ''', (user_uuid,))
        rows = cur.fetchall()
        cur.execute("RELEASE SAVEPOINT lh_check")
        cur.close()

        if not rows:
            return None

        return [
            {
                "element_id": r[0],
                "element_type": r[1],
                "area": r[2],
                "depth": r[3],
                "passed": r[4],
            }
            for r in rows
        ]
    except Exception as e:
        # View/table может не существовать в dev / old instance — не критично.
        # ВАЖНО: откатить ТОЛЬКО сломанный SELECT через savepoint, не трогая
        # предыдущие _write_derived в транзакции. WP-227: digitaltwin БД не
        # имеет development.learning_history — это ожидаемо.
        try:
            cur2 = conn.cursor()
            cur2.execute("ROLLBACK TO SAVEPOINT lh_check")
            cur2.execute("RELEASE SAVEPOINT lh_check")
            cur2.close()
        except Exception:
            pass
        _log.warning("learning_history unavailable: %s", e)
        return None


def _check_learning_history_schema(conn) -> bool:
    """Однократная проверка наличия element_id в development.learning_history (v2 schema).

    Вызывается один раз до цикла по пользователям, результат кэшируется.
    Без этого _load_learning_history выдаёт warn на каждого пользователя при v1 schema.
    """
    try:
        cur = conn.cursor()
        cur.execute("SAVEPOINT lh_schema_check")
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'development'
              AND table_name = 'learning_history'
              AND column_name = 'element_id'
        """)
        exists = cur.fetchone() is not None
        cur.execute("RELEASE SAVEPOINT lh_schema_check")
        cur.close()
        return exists
    except Exception as e:
        try:
            cur2 = conn.cursor()
            cur2.execute("ROLLBACK TO SAVEPOINT lh_schema_check")
            cur2.execute("RELEASE SAVEPOINT lh_schema_check")
            cur2.close()
        except Exception:
            pass
        _log.warning("learning_history schema check failed: %s", e)
        return False


def _load_rcs_metrics(conn_learning, user_id: str, first_seen_at=None) -> dict | None:
    """§RCS: загрузить метрики RCS-пути из domain_event для calc_student_stage.

    WP-214 Ф10.3 — собирает данные для '2_rcs' секции в 2_collected.
    Читает события с activity_domain='practice' (саморазвитие, не рабочий код).

    RLS требование (B7.3.6): каждый запрос содержит явный WHERE account_id = $1
    (не полагаемся только на RLS — уязвимость L2-PRIVACY при BYPASSRLS).

    Returns:
        dict с полями для calc_rcs_indices(), или None при ошибке / нет данных.
    """
    if conn_learning is None:
        return None
    try:
        cur = conn_learning.cursor()
        cur.execute("SAVEPOINT rcs_metrics")
        now_interval = "NOW()"

        # M1: активные дни — любое practice/learning событие (WP-214 Ф10.5 уточнение)
        # Изменено с day_plan_closed-only на любую активность:
        # - day_close ≠ собранность; эмиссия событий разреженная (4 события за 60 дней)
        # - Собранность = ≥1 практическое действие в день (commit, wp, session, урок и т.д.)
        cur.execute("""
            SELECT
              COUNT(DISTINCT DATE(occurred_at)) FILTER (
                WHERE occurred_at >= NOW() - INTERVAL '14 days'
              ) AS streak_14d,
              COUNT(DISTINCT DATE(occurred_at)) FILTER (
                WHERE occurred_at >= NOW() - INTERVAL '60 days'
              ) AS streak_42d,
              COUNT(DISTINCT DATE(occurred_at)) FILTER (
                WHERE occurred_at >= NOW() - INTERVAL '70 days'
              ) AS streak_63d,
              COUNT(DISTINCT DATE(occurred_at)) FILTER (
                WHERE occurred_at >= NOW() - INTERVAL '30 days'
              ) AS day_plan_30d
            FROM domain_event
            WHERE account_id = %s::uuid
              AND activity_domain IN ('practice', 'learning')
        """, (user_id,))
        m1_row = cur.fetchone()

        # M2: pack_updated + knowledge_extracted + iwe_session за 30 дней
        cur.execute("""
            SELECT COUNT(*)
            FROM domain_event
            WHERE account_id = %s::uuid
              AND event_type IN ('pack_updated', 'knowledge_extracted', 'iwe_session')
              AND activity_domain IN ('practice', 'learning')
              AND occurred_at >= NOW() - INTERVAL '30 days'
        """, (user_id,))
        m2_row = cur.fetchone()

        # M4: wp_closed + strategy_session за 60 дней
        cur.execute("""
            SELECT
              COUNT(*) AS total,
              COUNT(*) FILTER (
                WHERE payload->>'verification_class' IN ('open-loop', 'problem-framing')
              ) AS quality
            FROM domain_event
            WHERE account_id = %s::uuid
              AND event_type IN ('wp_closed', 'wp_completed', 'strategy_session_completed')
              AND activity_domain IN ('practice', 'learning')
              AND occurred_at >= NOW() - INTERVAL '60 days'
        """, (user_id,))
        m4_row = cur.fetchone()

        # activity_30d: любые practice/learning события за 30д (для SR.002 gate)
        cur.execute("""
            SELECT COUNT(DISTINCT DATE(occurred_at))
            FROM domain_event
            WHERE account_id = %s::uuid
              AND activity_domain IN ('practice', 'learning')
              AND occurred_at >= NOW() - INTERVAL '30 days'
        """, (user_id,))
        activity_row = cur.fetchone()

        # Teaching и real_world_impact (для SR.004)
        cur.execute("""
            SELECT
              COUNT(*) FILTER (
                WHERE event_type = 'teaching_session'
                  AND occurred_at >= NOW() - INTERVAL '90 days'
              ) AS teaching_90d,
              COUNT(*) FILTER (
                WHERE event_type = 'real_world_impact'
                  AND occurred_at >= NOW() - INTERVAL '180 days'
              ) AS impact_180d
            FROM domain_event
            WHERE account_id = %s::uuid
        """, (user_id,))
        impact_row = cur.fetchone()

        # W-рефлексии (из learning.w_reflections)
        cur.execute("""
            SELECT
              COALESCE(MAX(quality_score), 0) AS w_score,
              COUNT(*) >= 1 AS w_calibrated,
              COUNT(*) FILTER (WHERE quality_score >= 3) AS w_reflection_quality
            FROM learning.w_reflections
            WHERE account_id = %s::uuid
        """, (user_id,))
        w_row = cur.fetchone()

        cur.execute("RELEASE SAVEPOINT rcs_metrics")
        cur.close()

        from datetime import timedelta
        now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
        age_weeks = 0
        if first_seen_at:
            try:
                delta = now - first_seen_at
                age_weeks = delta.days // 7
            except Exception:
                pass

        return {
            "streak_14d": int(m1_row[0] or 0),
            "streak_42d": int(m1_row[1] or 0),
            "streak_63d": int(m1_row[2] or 0),
            "day_plan_30d": int(m1_row[3] or 0),
            "m2_events_30d": int(m2_row[0] or 0),
            "m4_events_60d": int(m4_row[0] or 0),
            "m4_quality_60d": int(m4_row[1] or 0),
            "activity_30d": int(activity_row[0] or 0),
            "teaching_90d": int(impact_row[0] or 0),
            "impact_180d": int(impact_row[1] or 0),
            "w_score": int(w_row[0] or 0),
            "w_calibrated": bool(w_row[1]),
            "w_reflection_quality": int(w_row[2] or 0),
            "age_weeks": age_weeks,
        }
    except Exception as e:
        try:
            cur2 = conn_learning.cursor()
            cur2.execute("ROLLBACK TO SAVEPOINT rcs_metrics")
            cur2.execute("RELEASE SAVEPOINT rcs_metrics")
            cur2.close()
        except Exception:
            pass
        _log.warning("rcs_metrics unavailable (tables may not exist yet): %s", e)
        return None


def _load_event_rows(conn_learning, user_id: str, days: int = 7) -> list[dict] | None:
    """Загрузить domain_event строки для расчёта IND.3.2.04 (мультипликатор IWE).

    Args:
        conn_learning: psycopg2 connection к learning БД (NEON_LEARNING_URL).
        user_id: Ory UUID пользователя (совпадает с digital_twins.user_id для T1+).
        days: глубина выборки (7 дней = rolling window для multiplier_7d_avg).

    Returns:
        list[dict] с полями {event_type, occurred_at, payload} или None при ошибке.
    """
    if conn_learning is None:
        return None
    try:
        cur = conn_learning.cursor()
        cur.execute("SAVEPOINT er_check")
        cur.execute(
            """
            SELECT event_type, occurred_at, payload
            FROM domain_event
            WHERE account_id = %s::uuid
              AND event_type IN ('coding_time', 'wp_completed', 'day_close')
              AND occurred_at >= NOW() - INTERVAL %s
            ORDER BY occurred_at DESC
            """,
            (user_id, f"{days} days"),
        )
        rows = cur.fetchall()
        cur.execute("RELEASE SAVEPOINT er_check")
        cur.close()

        if not rows:
            return []  # пустой список != None: данных нет, но таблица доступна

        return [
            {
                "event_type": r[0],
                "occurred_at": r[1],
                "payload": r[2] if isinstance(r[2], dict) else (json.loads(r[2]) if r[2] else {}),
            }
            for r in rows
        ]
    except Exception as e:
        try:
            cur2 = conn_learning.cursor()
            cur2.execute("ROLLBACK TO SAVEPOINT er_check")
            cur2.execute("RELEASE SAVEPOINT er_check")
            cur2.close()
        except Exception:
            pass
        _log.warning("domain_event unavailable: %s", e)
        return None


def _fetch_twins(conn, user_id_filter: str | None = None) -> list[tuple[str, dict]]:
    """Прочитать все digital_twins с 2_collected из Neon.

    Returns:
        [(user_id, data_dict), ...] — только те, у кого есть 2_collected.
    """
    cur = conn.cursor()
    if user_id_filter:
        cur.execute('''
            SELECT user_id, data
            FROM digital_twins
            WHERE user_id = %s
              AND data ? '2_collected'
        ''', (user_id_filter,))
    else:
        cur.execute('''
            SELECT user_id, data
            FROM digital_twins
            WHERE user_id IS NOT NULL
              AND data ? '2_collected'
        ''')
    rows = cur.fetchall()
    cur.close()

    result = []
    for user_id, data in rows:
        # psycopg2 возвращает JSONB как dict автоматически, но проверим
        if isinstance(data, str):
            data = json.loads(data)
        result.append((user_id, data))
    return result


def _write_derived(conn, user_id: str, derived: dict) -> None:
    """Обновить 3_derived через SQL deep merge (не трогая 2_collected).

    F2 dual-write (WP-253, 28 апр):
    1. Legacy: UPDATE platform.digital_twins.data->'3_derived' (текущий путь)
    2. Mirror: UPSERT indicators.calculated_profile.indicators.3_derived
       через _write_to_indicators(). Cross-DB write, отдельная connection,
       не разделяет transaction. Failures логируются warning'ом.

    Pre-condition: env var INDICATORS_URL set с writer-credentials.
    **Рекомендация (28 апр):** использовать `DATABASE_URL_PROFILER_INDICATORS`
    (роль `profiler_writer_indicators` с SELECT+INSERT+UPDATE на public.calculated_profile,
    создана миграцией `mvp/029-profiler-writer-grants.sql`).

    НЕ использовать:
      - `DATABASE_URL_INDICATORS_PROJECTION` — `projection_writer_indicators` имеет
        SELECT+UPDATE, нет INSERT → F2 ON CONFLICT падает на новых users.
      - `DATABASE_URL_INDICATORS_POOLED` (neondb_owner) — full DB access в Profiler runtime
        = security smell.

    Если INDICATORS_URL не set — silently skip indicators write, legacy продолжает работать.

    SLA: drift между БД должен оставаться < 24h (R28 cron daily). Drift > 48h
    на > 5% users → manual recalc. Counter в stats: indicators_dual_write_failed.
    """
    # Legacy write (как было)
    cur = conn.cursor()
    cur.execute('''
        UPDATE digital_twins
        SET data = COALESCE(data, '{}'::jsonb)
                   || jsonb_build_object('3_derived',
                       COALESCE(data->'3_derived', '{}'::jsonb) || %s::jsonb
                   ),
            updated_at = NOW()
        WHERE user_id = %s
    ''', (json.dumps(derived), user_id))
    cur.close()

    # F2 dual-write (прямая запись в indicators)
    _write_to_indicators(user_id, derived)

    # F1.A event-driven (если EVENT_GATEWAY_URL задан)
    _emit_dt_recalc_event(user_id, derived)


def _emit_dt_recalc_event(user_id: str, derived: dict) -> None:
    """POST dt_recalc event к event-gateway (F1.A event-driven path).

    Payload: {"account_id": user_id, "indicators": {"3_derived": derived}}
    projection_rules rule #4 читает $.payload.indicators и jsonb_merge-ит в
    indicators.calculated_profile.indicators (см. migration 108).

    Отключено если EVENT_GATEWAY_URL не задан —
    F2 dual-write продолжает работать без event bus.
    Gateway авторизует по source field (ALLOWED_SOURCES), не по токену.
    """
    gateway_url = os.environ.get("EVENT_GATEWAY_URL")
    if not gateway_url:
        return  # F1.A disabled; F2 dual-write остаётся основным путём

    try:
        import uuid as _uuid_mod
        import urllib.request

        import datetime as _dt
        body = json.dumps({
            "source": "profiler",
            "event_type": "dt_recalc",
            "schema_version": "v1",
            "external_id": f"recalc-{user_id}-{_uuid_mod.uuid4().hex[:8]}",
            "occurred_at": _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "account_id": user_id,
            "payload": {
                "account_id": user_id,
                "indicators": {"3_derived": derived},
            },
        }).encode()

        req = urllib.request.Request(
            gateway_url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "aisystant-profiler/1.0",  # CF Bot Fight Mode блокирует Python-urllib
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status not in (200, 201, 202):
                _log.warning("event-gateway HTTP %s for %s", resp.status, user_id[:12])
    except Exception as e:
        _log.warning("event-gateway emit failed for %s: %s", user_id[:12], e)


def _write_to_indicators(user_id: str, derived: dict) -> None:
    """Mirror write в indicators.calculated_profile.indicators jsonb.

    Cross-database write: отдельная connection к indicators БД, не разделяет
    transaction с legacy write. Idempotent через ON CONFLICT DO UPDATE.
    Failures НЕ прерывают работу Profiler'а (warning + продолжение).

    Source-version помечен 'profiler-dual-write-2026-04-28' для идентификации
    F2-записей в audit log (vs F1.B DT MCP path в будущем).

    Permission requirement: INDICATORS_URL пользователь должен иметь INSERT + UPDATE
    на public.calculated_profile. **Создан 28 апр:** `profiler_writer_indicators`
    с правильными правами (миграция `mvp/029-profiler-writer-grants.sql`).
    URL: `DATABASE_URL_PROFILER_INDICATORS` в .deploy.env.
    """
    indicators_url = os.environ.get("INDICATORS_URL")
    if not indicators_url:
        # silently skip if not configured (для dev-окружения / старого setup)
        _dual_write_stats["skipped"] += 1
        return

    try:
        import psycopg2
        conn = psycopg2.connect(indicators_url, connect_timeout=5)
        try:
            cur = conn.cursor()
            cur.execute('''
                INSERT INTO public.calculated_profile (
                    account_id, indicators, last_recalc_at, source_version
                ) VALUES (
                    %s::uuid,
                    jsonb_build_object('3_derived', %s::jsonb),
                    NOW(),
                    'profiler-dual-write-2026-04-28'
                )
                ON CONFLICT (account_id) DO UPDATE
                SET indicators = COALESCE(public.calculated_profile.indicators, '{}'::jsonb)
                                 || jsonb_build_object('3_derived',
                                     COALESCE(public.calculated_profile.indicators->'3_derived', '{}'::jsonb) || %s::jsonb
                                 ),
                    last_recalc_at = NOW(),
                    source_version = EXCLUDED.source_version
            ''', (user_id, json.dumps(derived), json.dumps(derived)))
            conn.commit()
            _dual_write_stats["ok"] += 1
        finally:
            conn.close()
    except Exception as e:
        _dual_write_stats["failed"] += 1
        _log.warning("indicators dual-write failed for %s: %s", user_id[:12], e)


def recalculate_all(
    neon_url: str,
    user_id_filter: str | None = None,
    dry_run: bool = False,
    learning_url: str | None = None,
) -> dict:
    """Пересчитать 3_derived для всех (или одного) пользователей.

    Args:
        neon_url: connection string к digital_twins БД (обязательно).
        user_id_filter: если задан — пересчитать только этого пользователя.
        dry_run: показать diff без записи в БД.
        learning_url: connection string к learning БД (NEON_LEARNING_URL, опционально).
            При наличии — загружает domain_event для IND.3.2.04 (мультипликатор IWE).
            При отсутствии — multiplier_today=null, multiplier_7d_avg=null.

    Returns:
        stats dict: {recalculated, skipped, errors, first_error}
    """
    stats = {"recalculated": 0, "skipped": 0, "errors": 0, "first_error": None, "changes": []}
    now = datetime.now(timezone.utc)

    conn = _connect(neon_url)
    # Опциональное подключение к learning БД для domain_event (IND.3.2.04)
    conn_learning = None
    if learning_url:
        try:
            conn_learning = _connect(learning_url)
            _log.info("Learning DB connected (event_rows for IND.3.2.04 multiplier)")
        except Exception as e:
            _log.warning("Learning DB connect failed — multiplier will be null: %s", e)

    try:
        twins = _fetch_twins(conn, user_id_filter)
        _log.info("Loaded %d digital_twins with 2_collected", len(twins))

        # Однократная проверка схемы до цикла — исключает N предупреждений при v1 schema.
        has_lh_v2 = _check_learning_history_schema(conn)
        if not has_lh_v2:
            _log.warning("development.learning_history: element_id absent (v1 schema) — BKT mastery skipped for all users")

        for user_id, data in twins:
            try:
                if not data.get('2_collected'):
                    stats["skipped"] += 1
                    continue

                # Determine the correct key for learning_history lookup.
                # digital_twins.user_id может быть либо Ory UUID (dt_user_id) либо engagement UUID.
                # learning_history.user_uuid — это engagement UUID.
                # Пробуем сразу по user_id (если совпадает) — корректно для большинства T1+.
                learning_rows = _load_learning_history(conn, user_id) if has_lh_v2 else None

                # WP-218 Ф8.3: domain_event rows для мультипликатора IWE (7-дневное окно)
                event_rows = _load_event_rows(conn_learning, user_id)

                # WP-214 Ф10.3: RCS-метрики для stage-evaluation (M1/M2/M4/W из domain_event)
                # Пишем в data['2_collected']['2_rcs'] — calc_student_stage() читает оттуда
                account_data = (data.get('2_collected') or {}).get('2_1_account') or {}
                first_seen_at = (
                    account_data.get('first_seen_at')
                    or account_data.get('created_at')
                    or account_data.get('first_event_at')  # WP-303: fallback на actual collector field
                )
                if isinstance(first_seen_at, str):
                    try:
                        first_seen_at = datetime.fromisoformat(first_seen_at.replace('Z', '+00:00'))
                    except (ValueError, AttributeError):
                        first_seen_at = None
                rcs_metrics = _load_rcs_metrics(conn_learning, user_id, first_seen_at)
                if rcs_metrics:
                    if '2_collected' not in data:
                        data['2_collected'] = {}
                    data['2_collected']['2_rcs'] = rcs_metrics
                else:
                    _log.warning("user=%s: rcs_metrics=None — stage calc falls back to legacy path", user_id)

                # v2.0: передаём полный data + event_rows для IND.3.2.04.
                derived = calculate_derived(data, learning_rows=learning_rows, event_rows=event_rows, as_of=now)
                if not derived:
                    stats["skipped"] += 1
                    continue

                # Snapshot before для diff
                old_integral = (data.get('3_derived') or {}).get('3_10_integral') or {}
                new_integral = derived.get('3_10_integral') or {}
                old_idx = old_integral.get('index')
                new_idx = new_integral.get('index')

                if dry_run:
                    stats["changes"].append((user_id, old_idx, new_idx))
                    _log.info("[dry-run] %s: %s → %s", user_id[:12], old_idx, new_idx)
                else:
                    _write_derived(conn, user_id, derived)
                    stats["changes"].append((user_id, old_idx, new_idx))
                    if old_idx != new_idx:
                        _log.info("%s: %s → %s", user_id[:12], old_idx, new_idx)

                stats["recalculated"] += 1

            except Exception as e:
                stats["errors"] += 1
                if not stats["first_error"]:
                    stats["first_error"] = f"{user_id}: {e}"
                _log.error("%s: %s", user_id[:12], e)

        if not dry_run:
            conn.commit()
            _log.info("Committed %d updates", stats["recalculated"])
        else:
            conn.rollback()
            _log.info("Dry run — no DB changes")

    finally:
        conn.close()
        if conn_learning is not None:
            try:
                conn_learning.close()
            except Exception:
                pass

    return stats


def main():
    parser = argparse.ArgumentParser(description="R28 Profiler — recalculate 3_derived from 2_collected")
    parser.add_argument("--user-id", help="Recalculate only one user_id (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Show diff without writing to DB")
    args = parser.parse_args()

    logging.basicConfig(format="%(levelname)s %(name)s %(message)s", level=logging.INFO)
    neon_url = os.environ.get("NEON_URL")
    if not neon_url:
        _log.critical("NEON_URL env var required")
        sys.exit(1)

    # Опциональный URL к learning БД для IND.3.2.04 (мультипликатор IWE, WP-218 Ф8.3)
    learning_url = os.environ.get("NEON_LEARNING_URL")

    _log.info("R28 Profiler — recalculate_derived (%s)", datetime.now(timezone.utc).isoformat())
    _log.info("Mode: %s", 'dry-run' if args.dry_run else 'write')
    if args.user_id:
        _log.info("Filter: user_id=%s", args.user_id)
    if learning_url:
        _log.info("IND.3.2.04: NEON_LEARNING_URL set — multiplier will be calculated")
    else:
        _log.info("IND.3.2.04: NEON_LEARNING_URL not set — multiplier will be null")

    stats = recalculate_all(neon_url, user_id_filter=args.user_id, dry_run=args.dry_run, learning_url=learning_url)

    _log.info("Done: %d recalculated, %d skipped, %d errors", stats["recalculated"], stats["skipped"], stats["errors"])

    # F2 dual-write summary (WP-253, 28 апр)
    dw = _dual_write_stats
    total_writes = dw["ok"] + dw["failed"] + dw["skipped"]
    if total_writes > 0:
        _log.info("Indicators dual-write: ok=%d failed=%d skipped=%d", dw["ok"], dw["failed"], dw["skipped"])
        # Fail-fast 1: если 100% writes skip И есть recalculations — INDICATORS_URL не задан, misconfig
        if dw["skipped"] == total_writes and stats["recalculated"] > 0 and not args.dry_run:
            _log.critical("100% indicators writes skipped (INDICATORS_URL не задан). Это misconfig — F2 dual-write не работает.")
            sys.exit(4)
        # Fail-fast 2 (verifier 28 апр): failure rate > 50% при ≥10 attempts —
        # ловит permission errors (например INDICATORS_URL=DATABASE_URL_INDICATORS_PROJECTION
        # = read-only role без INSERT). Раннее падение лучше silent failure 122 рантаймов.
        if dw["failed"] >= 10 and dw["failed"] / max(total_writes, 1) > 0.50 and not args.dry_run:
            _log.critical("indicators dual-write failure rate %d/%d > 50%% (>=10 attempts). Likely INDICATORS_URL permission denied или connection issue.", dw["failed"], total_writes)
            sys.exit(5)
        # Soft warn: failure rate > 10% — operational issue, не sys.exit
        if dw["failed"] > 0 and dw["failed"] / max(total_writes, 1) > 0.10:
            _log.warning("indicators dual-write failure rate %d/%d > 10%% — investigate", dw["failed"], total_writes)

    if stats["first_error"]:
        _log.error("First error: %s", stats["first_error"])
        sys.exit(3)


if __name__ == "__main__":
    main()
