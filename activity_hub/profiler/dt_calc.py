"""
Calculation Engine v1.0 — derived indicators из 2_collected + learning_history (WP-151 Ф7a).

Вычисляет IND.3 (derived) из IND.2 (collected) данных.
Источники: engagement + notification_log + coding (WakaTime) + IWE (git/WP) + learning_history (BKT).

Индикаторы:
  IND.3.1.02  slot_regularity        — доля активных дней (→ агентность)
  IND.3.4.01  student_stage           — ступень ученика (0-4, threshold rules)
  IND.3.10.1  integral_agency_index   — агрегированный индекс (0-100)
  IND.3.5.*   mastery_by_area         — BKT P(mastery) по 5 областям из learning_history
  IND.3.6.*   worldview_gaps          — мемы CAT.001 с gap (P(mastery) < порога по ступени)
  IND.3.7.*   mastery_gaps            — практики CAT.002/003 с gap > 0
  IND.3.8.01  qualification_degree    — степень квалификации (из LMS, Методсовет МИМ)
  IND.3.9.01  it_level               — ИТ-уровень (0-3, DigComp-адаптация)
  IND.3.12.01 delivery_style         — рекомендуемый стиль подачи (авто-адаптация)
  IND.3.13.01 notification_responsiveness — отзывчивость на уведомления (0-100)
  IND.3.14.01 learning_autonomy      — учебная автономность (0-100)

v1.0 (WP-151 Ф7a): Production-формулы 5 осей MVP + notification/autonomy.
  Разблокирует WP-149 (Портной), WP-117 (nudge), WP-135 (интерфейс ЦД).

v0.8 (WP-151 Ф5): Полноценный BKT (Bayesian Knowledge Tracing) по мемам CAT.001.
  Вместо MAX depth — вероятностная модель P(mastery) по каждому мему.
  4 параметра: P(L0)=0.1, P(T)=0.3, P(G)=0.25, P(S)=0.1 (литературные значения).
  mastery_by_area = средний P(mastery) по области.
  worldview_gaps использует P(mastery) < порога вместо current_depth < target_depth.

v0.7 (WP-175 Ф5): BKT из learning_history → mastery_by_area, worldview_gaps, mastery_gaps.
  calculate_derived() принимает learning_rows (из development.learning_history).
  При learning_rows=None — возвращает [] для gaps (fallback PD.SPEC.001 §3).

v0.6 (WP-174): Builder Path — альтернативные пороги для Stage 2-4.
  Пользователи с высокой coding/IWE-активностью (T3-T4) могут достичь
  ступени через builder-метрики (2_6_coding, 2_7_iwe) вместо учебных.
  АрхГейт: 62/70 ЭМОГССБ, принцип #5 Evolvability-first.

Пороги: из метамодели DS-MCP/digital-twin-mcp/metamodel/3_derived/.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════
# STUDENT STAGES (PD.FORM.003)
# ═══════════════════════════════════════════════════════════

STAGE_RANDOM = 0        # Случайный
STAGE_PRACTICING = 1    # Практикующий
STAGE_SYSTEMATIC = 2    # Систематический
STAGE_DISCIPLINED = 3   # Дисциплинированный
STAGE_PROACTIVE = 4     # Проактивный

STAGE_NAMES = {
    0: "STG.Student.Random",
    1: "STG.Student.Practicing",
    2: "STG.Student.Systematic",
    3: "STG.Student.Disciplined",
    4: "STG.Student.Proactive",
}

STAGE_NAMES_RU = {
    0: "Случайный",
    1: "Практикующий",
    2: "Систематический",
    3: "Дисциплинированный",
    4: "Проактивный",
}


# ═══════════════════════════════════════════════════════════
# Ф5: CAT.001 каталог мемов (BKT-данные для GAP-профиля)
# Источник: DS-principles-curriculum/data/curriculum/CAT.001/
# Формат: {meme_id: {area: int 1-5, entry_stage: int 0-4}}
# ═══════════════════════════════════════════════════════════

_CAT001_META: dict[str, dict] = {
    "M-001": {"area": 1, "entry_stage": 1},
    "M-002": {"area": 1, "entry_stage": 0},
    "M-003": {"area": 1, "entry_stage": 1},
    "M-004": {"area": 1, "entry_stage": 1},
    "M-005": {"area": 1, "entry_stage": 1},
    "M-006": {"area": 1, "entry_stage": 1},
    "M-007": {"area": 1, "entry_stage": 1},
    "M-008": {"area": 1, "entry_stage": 0},
    "M-009": {"area": 1, "entry_stage": 1},
    "M-010": {"area": 1, "entry_stage": 0},
    "M-011": {"area": 1, "entry_stage": 0},
    "M-012": {"area": 1, "entry_stage": 1},
    "M-013": {"area": 2, "entry_stage": 1},
    "M-014": {"area": 2, "entry_stage": 0},
    "M-015": {"area": 2, "entry_stage": 1},
    "M-016": {"area": 2, "entry_stage": 1},
    "M-017": {"area": 2, "entry_stage": 0},
    "M-018": {"area": 2, "entry_stage": 0},
    "M-019": {"area": 2, "entry_stage": 1},
    "M-020": {"area": 3, "entry_stage": 0},
    "M-021": {"area": 3, "entry_stage": 0},
    "M-022": {"area": 3, "entry_stage": 0},
    "M-023": {"area": 3, "entry_stage": 1},
    "M-024": {"area": 3, "entry_stage": 1},
    "M-025": {"area": 3, "entry_stage": 1},
    "M-026": {"area": 3, "entry_stage": 1},
    "M-027": {"area": 3, "entry_stage": 1},
    "M-028": {"area": 3, "entry_stage": 1},
    "M-029": {"area": 3, "entry_stage": 0},
    "M-030": {"area": 3, "entry_stage": 1},
    "M-031": {"area": 3, "entry_stage": 1},
    "M-032": {"area": 4, "entry_stage": 0},
    "M-033": {"area": 4, "entry_stage": 0},
    "M-034": {"area": 4, "entry_stage": 1},
    "M-035": {"area": 4, "entry_stage": 1},
    "M-036": {"area": 4, "entry_stage": 0},
    "M-037": {"area": 4, "entry_stage": 0},
    "M-038": {"area": 4, "entry_stage": 1},
    "M-039": {"area": 4, "entry_stage": 1},
    "M-040": {"area": 5, "entry_stage": 0},
    "M-041": {"area": 5, "entry_stage": 1},
    "M-042": {"area": 5, "entry_stage": 0},
    "M-043": {"area": 5, "entry_stage": 1},
    "M-044": {"area": 5, "entry_stage": 1},
    "M-045": {"area": 5, "entry_stage": 0},
    "M-046": {"area": 3, "entry_stage": 0},
    "M-047": {"area": 3, "entry_stage": 0},
    "M-048": {"area": 3, "entry_stage": 0},
    "M-049": {"area": 4, "entry_stage": 1},
    "M-050": {"area": 4, "entry_stage": 1},
    "M-051": {"area": 1, "entry_stage": 1},
    "M-052": {"area": 1, "entry_stage": 1},
    "M-053": {"area": 5, "entry_stage": 1},
    "M-054": {"area": 5, "entry_stage": 1},
    "M-055": {"area": 5, "entry_stage": 1},
    "M-056": {"area": 5, "entry_stage": 0},
    "M-057": {"area": 5, "entry_stage": 0},
    "M-058": {"area": 5, "entry_stage": 0},
    "M-059": {"area": 5, "entry_stage": 0},
    "M-060": {"area": 5, "entry_stage": 1},
    "M-061": {"area": 5, "entry_stage": 0},
    "M-062": {"area": 5, "entry_stage": 0},
    "M-063": {"area": 5, "entry_stage": 0},
    "M-064": {"area": 5, "entry_stage": 1},
}

# Нормативная целевая глубина мемов по ступени и области (PD.FORM.080 §9).
# target_depth[student_stage][area] = int 1-3 (макс. глубина мема по фазе)
# Ступень 0 (Случайный): цель depth=1 только по ведущим осям (1=Знания, 5=Организм)
# Ступень 1→2 (Практикующий): ведущие оси 2=Инструменты, 3=Ограничения → depth=1 все
# Ступень 2→3 (Систематический): depth=2 для Знания(1), Ограничения(3), Окружение(4)
# Ступень 3→4 (Дисциплинированный): depth=3 для Знания(1), Окружение(4)
# Ступень 4 (Проактивный): depth=3 для всех
_TARGET_DEPTH: dict[int, dict[int, int]] = {
    0: {1: 1, 2: 1, 3: 1, 4: 1, 5: 1},
    1: {1: 1, 2: 1, 3: 1, 4: 1, 5: 1},
    2: {1: 2, 2: 1, 3: 2, 4: 2, 5: 1},
    3: {1: 3, 2: 2, 3: 2, 4: 3, 5: 2},
    4: {1: 3, 2: 3, 3: 3, 4: 3, 5: 3},
}


# ═══════════════════════════════════════════════════════════
# BKT — Bayesian Knowledge Tracing (WP-151 Ф5)
# ═══════════════════════════════════════════════════════════

# Литературные значения BKT-параметров (Corbett & Anderson, 1994).
# Калибровка по реальным данным — Ф7 (Лаборатория).
_BKT_P_L0 = 0.1     # начальная вероятность усвоения
_BKT_P_T = 0.3      # вероятность перехода к усвоению после попытки
_BKT_P_G = 0.25     # вероятность угадывания (не знает, но ответил верно)
_BKT_P_S = 0.1      # вероятность промаха (знает, но ошибся)

# Порог P(mastery) для признания мема усвоенным на данной глубине.
# Консервативный: 0.8 (стандартный BKT threshold).
_BKT_MASTERY_THRESHOLD = 0.8


def _bkt_update(p_l: float, correct: bool) -> float:
    """Одно обновление BKT: P(L_n) → P(L_{n+1}).

    Формула (Corbett & Anderson, 1994):
      P(L|correct)  = P(L) * (1 - P(S)) / (P(L) * (1 - P(S)) + (1 - P(L)) * P(G))
      P(L|incorrect) = P(L) * P(S) / (P(L) * P(S) + (1 - P(L)) * (1 - P(G)))
      P(L_next) = P(L|obs) + (1 - P(L|obs)) * P(T)

    Args:
        p_l: текущая P(mastery) [0.0–1.0]
        correct: результат попытки

    Returns:
        обновлённая P(mastery) [0.0–1.0]
    """
    if correct:
        numerator = p_l * (1 - _BKT_P_S)
        denominator = p_l * (1 - _BKT_P_S) + (1 - p_l) * _BKT_P_G
    else:
        numerator = p_l * _BKT_P_S
        denominator = p_l * _BKT_P_S + (1 - p_l) * (1 - _BKT_P_G)

    if denominator == 0:
        p_l_given_obs = p_l
    else:
        p_l_given_obs = numerator / denominator

    # Transition: даже если не усвоил, есть шанс усвоить после попытки
    return p_l_given_obs + (1 - p_l_given_obs) * _BKT_P_T


def _calc_bkt_per_meme(learning_rows: list[dict]) -> dict[str, dict]:
    """Вычислить BKT-состояние для каждого мема из learning_history.

    Группирует попытки по meme_id и depth, прогоняет BKT для каждой пары.
    Возвращает per-meme агрегат: P(mastery) на каждой глубине + общий.

    Args:
        learning_rows: записи из learning_history (element_type='meme'),
            отсортированные по created_at DESC (новые первые).
            Ключи: element_id, element_type, area, depth, passed.

    Returns:
        {meme_id: {
            "area": int,
            "p_mastery": float,         # общая P(mastery) = min по глубинам
            "max_depth_mastered": int,   # макс. глубина с P >= порога
            "attempts": int,            # общее число попыток
            "by_depth": {depth: {"p": float, "attempts": int, "correct": int}},
        }}
    """
    # Собрать попытки по (meme_id, depth) в хронологическом порядке
    # learning_rows приходят DESC — разворачиваем
    meme_attempts: dict[str, list[tuple[int, bool]]] = {}
    meme_areas: dict[str, int] = {}

    for row in reversed(learning_rows):
        if row.get("element_type") != "meme":
            continue
        eid = row.get("element_id")
        if not eid:
            continue
        meme_id = eid.split(".")[-1] if "." in eid else eid
        depth = row.get("depth") or 0
        passed = bool(row.get("passed"))
        area = row.get("area")

        if meme_id not in meme_attempts:
            meme_attempts[meme_id] = []
        meme_attempts[meme_id].append((depth, passed))
        if area:
            meme_areas[meme_id] = area

    result: dict[str, dict] = {}

    for meme_id, attempts in meme_attempts.items():
        # BKT по каждой глубине отдельно
        depth_state: dict[int, dict] = {}

        for depth, passed in attempts:
            if depth not in depth_state:
                depth_state[depth] = {"p": _BKT_P_L0, "attempts": 0, "correct": 0}
            ds = depth_state[depth]
            ds["p"] = _bkt_update(ds["p"], passed)
            ds["attempts"] += 1
            if passed:
                ds["correct"] += 1

        # Общая P(mastery) = min P по всем глубинам ≤ max_depth_attempted
        # (нужно усвоить на ВСЕХ уровнях, не только на одном)
        max_depth_mastered = 0
        for d in sorted(depth_state.keys()):
            if depth_state[d]["p"] >= _BKT_MASTERY_THRESHOLD:
                max_depth_mastered = d

        all_ps = [ds["p"] for ds in depth_state.values()]
        p_mastery = min(all_ps) if all_ps else 0.0
        total_attempts = sum(ds["attempts"] for ds in depth_state.values())

        result[meme_id] = {
            "area": meme_areas.get(meme_id, 0),
            "p_mastery": round(p_mastery, 3),
            "max_depth_mastered": max_depth_mastered,
            "attempts": total_attempts,
            "by_depth": {d: {"p": round(ds["p"], 3), "attempts": ds["attempts"], "correct": ds["correct"]}
                         for d, ds in sorted(depth_state.items())},
        }

    return result


# ═══════════════════════════════════════════════════════════
# IND.3.5 — Mastery by Area (WP-151 Ф5, BKT)
# ═══════════════════════════════════════════════════════════

AREA_KEY_MAP = {1: "knowledge", 2: "tools", 3: "constraints", 4: "environment", 5: "organism"}


def calc_mastery_by_area(learning_rows: list[dict]) -> dict:
    """BKT P(mastery) по каждой из 5 областей из learning_history.

    IND.3.5.*: mastery_by_area[area_key] = средний P(mastery) по мемам области.
    Обратная совместимость: max_depth сохранён для потребителей, которые его используют.

    Args:
        learning_rows: list of dicts с ключами element_id, element_type, area, depth, passed

    Returns:
        {
            "knowledge": float,  # средний P(mastery) [0.0–1.0]
            "tools": float,
            ...
            "max_depth": {"knowledge": int, ...},  # обратная совместимость
            "details": {meme_id: {"p_mastery": float, "attempts": int, ...}, ...}
        }
    """
    bkt = _calc_bkt_per_meme(learning_rows)

    # Средний P(mastery) по области
    area_ps: dict[str, list[float]] = {v: [] for v in AREA_KEY_MAP.values()}
    area_max_depth: dict[str, int] = {v: 0 for v in AREA_KEY_MAP.values()}

    for meme_id, state in bkt.items():
        area = state["area"]
        if area not in AREA_KEY_MAP:
            continue
        key = AREA_KEY_MAP[area]
        area_ps[key].append(state["p_mastery"])
        area_max_depth[key] = max(area_max_depth[key], state["max_depth_mastered"])

    result = {}
    for key in AREA_KEY_MAP.values():
        ps = area_ps[key]
        result[key] = round(sum(ps) / len(ps), 3) if ps else 0.0

    result["max_depth"] = area_max_depth
    result["details"] = {mid: {k: v for k, v in s.items() if k != "by_depth"}
                         for mid, s in bkt.items()}

    return result


# ═══════════════════════════════════════════════════════════
# IND.3.6 — Worldview Gaps (WP-151 Ф5, BKT)
# ═══════════════════════════════════════════════════════════

# Порог P(mastery) по ступени: чем выше ступень, тем строже требование.
_MASTERY_THRESHOLD_BY_STAGE: dict[int, float] = {
    0: 0.6,
    1: 0.65,
    2: 0.7,
    3: 0.8,
    4: 0.9,
}


def calc_worldview_gaps(learning_rows: list[dict], student_stage: int) -> list[dict]:
    """Мемы CAT.001 с P(mastery) ниже порога по ступени (BKT).

    IND.3.6: only мемы, relevant для текущей ступени (entry_stage <= student_stage).
    Использует BKT P(mastery) вместо бинарного current_depth < target_depth.
    Обратная совместимость: current_depth и target_depth сохранены.

    Args:
        learning_rows: записи из learning_history (element_type='meme')
        student_stage: текущая ступень (0-4)

    Returns:
        list[dict] — мемы с gap, отсортированные по area.
        Каждый dict содержит:
          id, area, p_mastery, mastery_threshold, current_depth, target_depth,
          attempts, can_do_passed.
    """
    bkt = _calc_bkt_per_meme(learning_rows)
    threshold = _MASTERY_THRESHOLD_BY_STAGE.get(student_stage, 0.7)
    target_map = _TARGET_DEPTH.get(student_stage, _TARGET_DEPTH[0])
    gaps = []

    for meme_id, meta in _CAT001_META.items():
        area = meta["area"]
        entry_stage = meta["entry_stage"]
        if entry_stage > student_stage:
            continue

        target_depth = target_map.get(area, 1)
        meme_state = bkt.get(meme_id)

        if meme_state:
            p_mastery = meme_state["p_mastery"]
            current_depth = meme_state["max_depth_mastered"]
            attempts = meme_state["attempts"]
            can_do = current_depth > 0
        else:
            p_mastery = 0.0
            current_depth = 0
            attempts = 0
            can_do = False

        # Gap если: P(mastery) ниже порога ИЛИ глубина не достигнута
        if p_mastery < threshold or current_depth < target_depth:
            gaps.append({
                "id": meme_id,
                "area": area,
                "p_mastery": round(p_mastery, 3),
                "mastery_threshold": threshold,
                "current_depth": current_depth,
                "target_depth": target_depth,
                "attempts": attempts,
                "can_do_passed": can_do,
            })

    gaps.sort(key=lambda x: (x["area"], x["p_mastery"]))
    return gaps


# ═══════════════════════════════════════════════════════════
# IND.3.1.02 — Slot Regularity (доля дней со слотом)
# ═══════════════════════════════════════════════════════════

def calc_slot_regularity(collected: dict, as_of: Optional[datetime] = None) -> float:
    """Доля активных дней от общего числа дней с первого события.

    IND.3.1.02: days_with_activity / total_days_since_start.
    Пороги (days/week): Random <1, Practicing ≥3, Systematic ≥5,
                        Disciplined ≥6, Proactive ≥6.7.

    Builder Path (WP-218 Ф2):
      active_days = max(bot_active_days, coding_active_days_30d)
      Пользователь, работающий в IWE (коммиты/WakaTime) но не в боте,
      должен получать справедливую оценку регулярности.
      Окно 30 дней для coding — это ближайшая аппроксимация к bot active_days
      (которые по факту агрегируются по последнему окну активности).

    Args:
        collected: данные 2_collected из digital_twins
        as_of: точка отсчёта «сейчас» (UTC). None = datetime.now(timezone.utc).
               Передавай явно при on-demand вызовах чтобы все пользователи
               в одном batch считались на один момент времени.

    Returns:
        float 0.0–1.0 (ratio) or 0.0 if insufficient data.
    """
    time_data = collected.get('2_4_time') or {}
    account = collected.get('2_1_account') or {}
    coding = collected.get('2_6_coding') or {}

    bot_active_days = time_data.get('active_days', 0) or 0
    coding_active_days = coding.get('coding_active_days_30d', 0) or 0
    active_days = max(bot_active_days, coding_active_days)

    first_event = account.get('first_event_at')

    if not first_event or active_days == 0:
        return 0.0

    try:
        if isinstance(first_event, str):
            # Handle both ISO formats
            first_dt = datetime.fromisoformat(first_event.replace('Z', '+00:00'))
        else:
            first_dt = first_event

        if first_dt.tzinfo is None:
            first_dt = first_dt.replace(tzinfo=timezone.utc)

        now = as_of if as_of is not None else datetime.now(timezone.utc)
        total_days = (now - first_dt).days
        if total_days <= 0:
            return 1.0  # Same day

        # Для builder path ограничиваем окном 30 дней (coding метрики 30-дневные)
        # иначе недавно начавший builder несправедливо наказан историей
        if coding_active_days > bot_active_days:
            total_days = min(total_days, 30)

        return min(active_days / total_days, 1.0)
    except (ValueError, TypeError):
        return 0.0


# ═══════════════════════════════════════════════════════════
# IND.3.4.02 — RCS Indices (M1/M2/M4/W из domain_event)
# WP-214 Ф10.3: RCS-путь определения ступени через практику IWE.
# Данные поступают из 2_collected['2_rcs'] — заполняет rcs-collector
# (recalculate_derived.py §RCS или dt-collect-neon plugin).
# ═══════════════════════════════════════════════════════════

def calc_rcs_indices(rcs: dict) -> dict:
    """Вычислить RCS-индексы M1/M2/M4/W из собранных событий domain_event.

    Входные данные (2_collected['2_rcs']):
      streak_14d: int   — дней с ≥1 practice/learning событием за 14 дней
      streak_42d: int   — дней с ≥1 practice/learning событием за 60 дней (target 42)
      streak_63d: int   — дней с ≥1 practice/learning событием за 70 дней (target 63)
      m2_events_30d: int — pack_updated + knowledge_extracted + iwe_session за 30д
      m4_events_60d: int — wp_closed + strategy_session_completed за 60д
      m4_quality_60d: int — из m4_events, только verification_class open-loop/problem-framing
      w_score: int      — W-индекс от R28 Диагноста (0=нет оценки, 1-5)
      w_calibrated: bool — есть ли актуальная оценка W (w_calibrated_at не None)
      teaching_90d: int — teaching_session events за 90д
      impact_180d: int  — real_world_impact events за 180д
      w_reflection_quality: int — quality score W-рефлексий (число сессий ≥3)
      age_weeks: int    — недель с first_seen

    Returns:
        {
            "M1": {"idx": 0-5, "streak_42d": int, "streak_63d": int},
            "M2": {"idx": 0-5, "events_30d": int},
            "M4": {"idx": 0-5, "events_60d": int, "quality_60d": int},
            "W":  {"idx": 0-5, "calibrated": bool},
        }
    """
    streak_14 = rcs.get("streak_14d", 0) or 0
    streak_42 = rcs.get("streak_42d", 0) or 0
    streak_63 = rcs.get("streak_63d", 0) or 0
    m2 = rcs.get("m2_events_30d", 0) or 0
    m4 = rcs.get("m4_events_60d", 0) or 0
    m4q = rcs.get("m4_quality_60d", 0) or 0
    w_raw = rcs.get("w_score", 0) or 0
    w_cal = rcs.get("w_calibrated", False)

    # M1: собранность — регулярность ≥1 practice/learning действия в день
    if streak_63 >= 63:
        m1_idx = 5
    elif streak_42 >= 42:
        m1_idx = 4
    elif streak_42 >= 24:
        m1_idx = 3
    elif streak_14 >= 6:
        m1_idx = 2
    elif streak_14 >= 1:
        m1_idx = 1
    else:
        m1_idx = 0

    # M2: инструменты — pack + knowledge + iwe
    if m2 >= 8:
        m2_idx = 5
    elif m2 >= 7:
        m2_idx = 4
    elif m2 >= 4:
        m2_idx = 3
    elif m2 >= 2:
        m2_idx = 2
    elif m2 >= 1:
        m2_idx = 1
    else:
        m2_idx = 0

    # M4: системное производство — wp_closed + strategy_session
    if m4q >= 7:
        m4_idx = 5
    elif m4 >= 5:
        m4_idx = 4
    elif m4 >= 3:
        m4_idx = 3
    elif m4 >= 2:
        m4_idx = 2
    elif m4 >= 1:
        m4_idx = 1
    else:
        m4_idx = 0

    # W: мировоззрение — от R28 Диагноста (без оценки = 0; не calibrated = fallback 1)
    if not w_cal:
        w_idx = min(w_raw, 1)  # без диалога максимум 1
    else:
        w_idx = min(w_raw, 5)

    return {
        "M1": {"idx": m1_idx, "streak_42d": streak_42, "streak_63d": streak_63},
        "M2": {"idx": m2_idx, "events_30d": m2},
        "M4": {"idx": m4_idx, "events_60d": m4, "quality_60d": m4q},
        "W":  {"idx": w_idx, "calibrated": w_cal},
    }


def check_rcs_stage(rcs: dict, indices: dict) -> int:
    """Определить ступень по RCS-индексам (SR.001-SR.004 логика).

    Возвращает максимальную ступень (0-4 в системе dt_calc, соотв. 1-5 в SR-нотации).
    SR использует нотацию 1-5; dt_calc — 0-4 (STAGE_RANDOM..STAGE_PROACTIVE).
    Маппинг: SR-ст.1=STAGE_RANDOM(0), SR-ст.2=STAGE_PRACTICING(1), ...
    """
    m1 = indices["M1"]["idx"]
    m2 = indices["M2"]["idx"]
    m4 = indices["M4"]["idx"]
    w = indices["W"]["idx"]

    age_weeks = rcs.get("age_weeks", 0) or 0
    teaching = rcs.get("teaching_90d", 0) or 0
    impact = rcs.get("impact_180d", 0) or 0
    w_reflection = rcs.get("w_reflection_quality", 0) or 0
    streak_42 = indices["M1"]["streak_42d"]
    streak_63 = indices["M1"]["streak_63d"]

    # SR.004: Дисциплинированный → Проактивный (dt: stage 4)
    if (m1 >= 5 and streak_63 >= 63 and
            m2 >= 5 and
            m4 >= 5 and
            w >= 4 and
            teaching >= 1 and
            impact >= 1 and
            w_reflection >= 3):
        return STAGE_PROACTIVE  # 4

    # SR.003: Систематический → Дисциплинированный (dt: stage 3)
    if (m1 >= 4 and streak_42 >= 42 and
            m2 >= 3 and
            m4 >= 3 and
            w >= 3 and
            age_weeks >= 12):
        return STAGE_DISCIPLINED  # 3

    # SR.002: Практикующий → Систематический (dt: stage 2)
    activity_30d = rcs.get("activity_30d", 0) or 0
    if (m1 >= 3 and
            w >= 2 and
            activity_30d >= 12 and
            age_weeks >= 4):
        return STAGE_SYSTEMATIC  # 2

    # SR.001: Случайный → Практикующий (dt: stage 1)
    activity_30d_min = rcs.get("day_plan_30d", 0) or 0
    age_days = age_weeks * 7
    if (m1 >= 2 and
            activity_30d_min >= 3 and
            age_days >= 14):
        return STAGE_PRACTICING  # 1

    return STAGE_RANDOM  # 0


# ═══════════════════════════════════════════════════════════
# IND.3.4.01 — Student Stage (ступень ученика)
# ═══════════════════════════════════════════════════════════

def calc_student_stage(collected: dict, as_of: Optional[datetime] = None) -> dict:
    """Определить ступень ученика через RCS-слоты (SR.001-SR.004 gate-логика).

    IND.3.4.01: categorical enum STG.Student.*.

    v2.0 (WP-214 Ф10.4, 11 мая 2026): единая RCS-логика через `check_rcs_stage`
    (SR.001-SR.004 правила в PACK-agent-rules). Legacy builder/learner-путь
    оставлен только как safety-net на случай недоступности БД learning
    (когда `_load_rcs_metrics` возвращает None, не пустой dict).

    Раньше (v1.1) RCS считался первым, но при rcs_stage == 0 переходил на
    builder/learner. Это переплетало два подхода и давало завышенные ступени
    рабочим коммитам (DP.FM.014 — builder ≠ саморазвитие).

    Returns:
        {
            "stage": int (0-4),
            "stage_id": "STG.Student.Random",
            "stage_name_ru": "Случайный",
            "path": "rcs" | "legacy",
            "evidence": {...},
            "rcs_indices": {...},  # при path='rcs'
        }
    """
    rcs_raw = collected.get('2_rcs')

    # ─── RCS-путь: единственный канонический путь (программа ЛР) ───
    if rcs_raw is not None:
        rcs_indices = calc_rcs_indices(rcs_raw)
        rcs_stage = check_rcs_stage(rcs_raw, rcs_indices)
        evidence = {
            "M1_idx": rcs_indices["M1"]["idx"],
            "M2_idx": rcs_indices["M2"]["idx"],
            "M4_idx": rcs_indices["M4"]["idx"],
            "W_idx": rcs_indices["W"]["idx"],
            "streak_42d": rcs_indices["M1"]["streak_42d"],
            "streak_63d": rcs_indices["M1"]["streak_63d"],
            "m2_events_30d": rcs_indices["M2"]["events_30d"],
            "m4_events_60d": rcs_indices["M4"]["events_60d"],
            "m4_quality_60d": rcs_indices["M4"]["quality_60d"],
            "activity_30d": rcs_raw.get("activity_30d", 0) or 0,
            "age_weeks": rcs_raw.get("age_weeks", 0) or 0,
            "w_calibrated": rcs_indices["W"]["calibrated"],
        }
        return {
            "stage": rcs_stage,
            "stage_id": STAGE_NAMES[rcs_stage],
            "stage_name_ru": STAGE_NAMES_RU[rcs_stage],
            "path": "rcs",
            "evidence": evidence,
            "rcs_indices": rcs_indices,
        }

    # ─── Legacy safety-net: только когда learning БД недоступна (rcs_raw is None) ───
    # Сохраняет старые builder/learner правила для деградационного режима.
    # Не путать с rcs_raw == {} (БД доступна, но событий нет) — этот случай выше.
    time_data = collected.get('2_4_time') or {}
    account = collected.get('2_1_account') or {}
    courses = collected.get('2_2_courses') or {}
    practice = collected.get('2_3_practice') or {}
    coding = collected.get('2_6_coding') or {}
    iwe = collected.get('2_7_iwe') or {}

    active_days = time_data.get('active_days', 0) or 0
    events_7d = time_data.get('events_last_7d', 0) or 0
    events_30d = time_data.get('events_last_30d', 0) or 0
    sessions_total = account.get('sessions_total', 0) or 0
    marathon_steps = courses.get('marathon_steps_total', 0) or 0
    training_passed = practice.get('training_passed_total', 0) or 0
    coding_hours_30d = (coding.get('coding_seconds_30d', 0) or 0) / 3600
    coding_active_days = coding.get('coding_active_days_30d', 0) or 0
    commits_30d = iwe.get('commits_30d', 0) or 0
    wp_completed = iwe.get('wp_completed_total', 0) or iwe.get('registry_done', 0) or 0

    regularity = calc_slot_regularity(collected, as_of=as_of)
    days_per_week = regularity * 7

    evidence = {
        "active_days": active_days,
        "events_7d": events_7d,
        "events_30d": events_30d,
        "days_per_week": round(days_per_week, 1),
        "sessions_total": sessions_total,
        "marathon_steps": marathon_steps,
        "training_passed": training_passed,
        "regularity": round(regularity, 3),
        "coding_hours_30d": round(coding_hours_30d, 1),
        "coding_active_days": coding_active_days,
        "commits_30d": commits_30d,
        "wp_completed": wp_completed,
        "_warning": "RCS unavailable — using legacy builder/learner thresholds (degraded mode)",
    }

    stage = STAGE_RANDOM
    path = "legacy"

    if sessions_total >= 3 and events_7d >= 2 and days_per_week >= 2:
        stage = STAGE_PRACTICING

    learner_s2 = (sessions_total >= 10 and events_7d >= 5
                  and days_per_week >= 4 and training_passed >= 3)
    builder_s2 = (coding_hours_30d >= 40 and coding_active_days >= 15
                  and days_per_week >= 4)
    if learner_s2 or builder_s2:
        stage = STAGE_SYSTEMATIC

    learner_s3 = (sessions_total >= 30 and events_7d >= 10
                  and days_per_week >= 5.5
                  and training_passed >= 10 and marathon_steps >= 5)
    builder_s3 = (coding_hours_30d >= 80 and commits_30d >= 50
                  and days_per_week >= 5.5)
    if learner_s3 or builder_s3:
        stage = STAGE_DISCIPLINED

    learner_s4 = (sessions_total >= 50 and days_per_week >= 6
                  and events_30d >= 60 and training_passed >= 20)
    builder_s4 = (coding_hours_30d >= 120 and commits_30d >= 100
                  and wp_completed >= 3)
    if learner_s4 or builder_s4:
        stage = STAGE_PROACTIVE

    return {
        "stage": stage,
        "stage_id": STAGE_NAMES[stage],
        "stage_name_ru": STAGE_NAMES_RU[stage],
        "path": path,
        "evidence": evidence,
    }


# ═══════════════════════════════════════════════════════════
# IND.3.10.1 — Integral Agency Index (0–100)
# ═══════════════════════════════════════════════════════════

def calc_integral_agency_index(collected: dict, as_of: Optional[datetime] = None) -> dict:
    """Агрегированный индекс агентности из групп 2_1–2_7.

    IND.3.10.1: weighted sum of normalized metrics → 0-100 scale.

    Компоненты (веса):
      - Регулярность (slot_regularity):       30%
      - Активность (events + coding + git):   25%
      - Обучение (courses + practice):         25%
      - Реакция на уведомления (notifications): 10%
      - Стаж (account + coding longevity):     10%

    Builder Path (WP-218 Ф2):
      activity и longevity поддерживают два источника:
        - Learner: bot events (2_4_time)
        - Builder: coding activity (2_6_coding) + git activity (2_7_iwe)
      code_signal = max(coding_score, git_score)  # корреляция внутри IWE
      activity_score = max(learner_activity, code_signal)
      longevity_score = max(learner_longevity, code_longevity)
      Цель: пользователь, работающий в IWE (коммиты, WakaTime) но не в боте,
      должен получить справедливую оценку агентности.

    Returns:
        {
            "index": float (0-100),
            "components": {...},  # breakdowns
            "path": "learner" | "builder" | "mixed",  # какой путь дал высшую активность
        }
    """
    time_data = collected.get('2_4_time') or {}
    account = collected.get('2_1_account') or {}
    courses = collected.get('2_2_courses') or {}
    practice = collected.get('2_3_practice') or {}
    notifications = collected.get('2_5_notifications') or {}
    coding = collected.get('2_6_coding') or {}
    iwe = collected.get('2_7_iwe') or {}

    # 1. Regularity (30%) — slot_regularity normalized to 0-100
    regularity = calc_slot_regularity(collected, as_of=as_of)
    regularity_score = min(regularity * 100 / 0.8, 100)  # 80%+ = 100

    # 2. Activity intensity (25%) — learner OR builder path
    # Learner: bot events intensity
    events_30d = time_data.get('events_last_30d', 0) or 0
    # 60+ events/30d = full score (2/day)
    activity_score_bot = min(events_30d / 60 * 100, 100)

    # Builder: coding hours + git commits (корреляция: IWE = markdown + git)
    # TODO WP-218: откалибровать пороги на ≥3 профилях (Ф2b)
    coding_seconds_30d = coding.get('coding_seconds_30d', 0) or 0
    coding_hours_30d = coding_seconds_30d / 3600
    commits_30d = iwe.get('commits_30d', 0) or 0
    # 40h/мес = full coding score (≈10h/нед стабильной работы)
    activity_score_coding = min(coding_hours_30d / 40 * 100, 100)
    # 30 commits/мес = full git score (≈1/день)
    activity_score_git = min(commits_30d / 30 * 100, 100)
    # code_signal: max внутри IWE (коррелируют — один сигнал)
    activity_score_code = max(activity_score_coding, activity_score_git)
    # Двухуровневый max: max(bot, code_signal)
    activity_score = max(activity_score_bot, activity_score_code)

    # 3. Learning (25%) — learner OR builder path (WP-218 Ф2)
    # Learner: marathon + feed + training в боте
    marathon_steps = courses.get('marathon_steps_total', 0) or 0
    feed_completed = courses.get('feed_completed_total', 0) or 0
    training_passed = practice.get('training_passed_total', 0) or 0
    # Normalized: 20 steps + 10 feed + 10 training = full score
    learning_raw = (
        min(marathon_steps / 20, 1) * 40
        + min(feed_completed / 10, 1) * 30
        + min(training_passed / 10, 1) * 30
    )
    learning_score_learner = min(learning_raw, 100)

    # Builder learning signals (WP-218 Ф2):
    #   1. LMS qualification — curated ступень МИМ (главный milestone)
    #   2. Club publications — публикации в Клубе/канале (knowledge-sharing)
    #   3. Pack knowledge graph — entities в pack-репо (personal corpus)
    #   4. Decision-making depth — вес решений/нед (cognitive work, WP-109 Ф7)
    # TODO WP-218 Ф2b: откалибровать пороги на ≥3 профилях (сейчас: Церен, Андрей, Агнесса)
    # TODO WP-109: добавить LMS course progress, knowledge verbalizations
    ecosystem = collected.get('2_8_ecosystem') or {}
    knowledge = collected.get('2_9_knowledge') or {}
    decisions = collected.get('2_8_decisions') or {}

    # LMS квалификация — шкала Методсовета МИМ (из dt_sync _QUAL_LEVEL_MAP):
    # Интересант (L05=5), Определяющийся (L08=8), Первокурсник (L1=10),
    # Ученик (L2=20), Работник (L25=25), Стратег (L3=30), Специалист (L4=40),
    # Практик (L5=50), Мастер (L6=60), Реформатор (L7=70), Деятель (L8=80).
    # Нормализация: L3 Стратег (30) = 50%, L5 Практик (50) = 80%, L7+ = 100%.
    qual = courses.get('qualification_level') or {}
    qual_numeric = qual.get('numeric', 0) if isinstance(qual, dict) else 0
    # 60 (Мастер L6) = full score — объективная верификация глубокой квалификации
    qual_score = min(qual_numeric / 60 * 100, 100)

    # Публикации в Клубе/канале — сильный сигнал knowledge-sharing
    publications_30d = ecosystem.get('publications_30d', 0) or 0
    # 15 публикаций/мес = full (≈1 через день) — intensive knowledge worker
    publication_score = min(publications_30d / 15 * 100, 100)

    # Knowledge graph depth — pack entities (personal corpus)
    pack_entities = knowledge.get('pack_total_entities', 0) or 0
    # 300 entities = full (≈среднее для 4-5 активных packs)
    knowledge_score = min(pack_entities / 300 * 100, 100)

    # Decision weight avg (WP-109 Ф7; пока = 0 до fix hook writer)
    decision_weight_7d_avg = decisions.get('decision_weight_7d_avg', 0) or 0
    # 5 weight/день (35/нед среднее) = full
    decision_score = min(decision_weight_7d_avg / 5 * 100, 100)

    # Builder learning = weighted combination (сигналы дополняют друг друга)
    learning_score_builder = (
        qual_score * 0.35           # curated milestone — сильнейший объективный сигнал
        + publication_score * 0.30  # активный вклад знаний в экосистему
        + knowledge_score * 0.20    # depth personal corpus
        + decision_score * 0.15     # cognitive throughput (когда hook писатель работает)
    )

    learning_score = max(learning_score_learner, learning_score_builder)

    # 4. Notification responsiveness (10%)
    notif_total = notifications.get('notifications_total', 0) or 0
    notif_30d = notifications.get('notifications_30d', 0) or 0
    # Having notifications means the system is active; 10+ notifications/30d = engaged
    notif_score = min(notif_30d / 10 * 100, 100) if notif_total > 0 else 50

    # 5. Longevity (10%) — learner OR builder path
    # Learner: active_days from bot events
    active_days = time_data.get('active_days', 0) or 0
    longevity_score_bot = min(active_days / 30 * 100, 100)
    # Builder: coding active days
    coding_active_days = coding.get('coding_active_days_30d', 0) or 0
    longevity_score_code = min(coding_active_days / 30 * 100, 100)
    longevity_score = max(longevity_score_bot, longevity_score_code)

    # Weighted sum
    index = (
        regularity_score * 0.30
        + activity_score * 0.25
        + learning_score * 0.25
        + notif_score * 0.10
        + longevity_score * 0.10
    )

    # Determine dominant path (для трассировки/отладки)
    if activity_score_code > activity_score_bot and longevity_score_code > longevity_score_bot:
        path = "builder"
    elif activity_score_bot >= activity_score_code and longevity_score_bot >= longevity_score_code:
        path = "learner"
    else:
        path = "mixed"

    return {
        "index": round(index, 1),
        "components": {
            "regularity": round(regularity_score, 1),
            "activity": round(activity_score, 1),
            "learning": round(learning_score, 1),
            "notifications": round(notif_score, 1),
            "longevity": round(longevity_score, 1),
        },
        "activity_breakdown": {
            "bot": round(activity_score_bot, 1),
            "coding": round(activity_score_coding, 1),
            "git": round(activity_score_git, 1),
            "code_signal": round(activity_score_code, 1),
        },
        "learning_breakdown": {
            "learner": round(learning_score_learner, 1),
            "builder": round(learning_score_builder, 1),
            "qualification": round(qual_score, 1),
            "publication": round(publication_score, 1),
            "knowledge": round(knowledge_score, 1),
            "decision": round(decision_score, 1),
        },
        "longevity_breakdown": {
            "bot": round(longevity_score_bot, 1),
            "coding": round(longevity_score_code, 1),
        },
        "path": path,
    }


# ═══════════════════════════════════════════════════════════
# IND.3.8.01 — Qualification Degree (LMS, Методсовет МИМ, WP-151 fix)
# ═══════════════════════════════════════════════════════════

# Степень квалификации (адаптация EQF): уровень системного мышления.
# Отражает глубину работы с предметной областью.
# Source-of-truth: LMS qualification_level_event (Методсовет МИМ).
# dt_sync записывает в 2_collected.2_2_courses.qualification_level при каждом sync.
# При появлении LMS-интеграции — формулу уточнить в Ф7b (Лаборатория).

def calc_qualification_degree(collected: dict, learning_rows: list[dict] | None = None) -> dict:
    """Степень квалификации из LMS (Методсовет МИМ).

    IND.3.8.01: Source-of-truth = LMS qualification_level_event.
    НЕ вычисляется из поведенческих данных — читается из 2_collected.
    dt_sync записывает qualification_level из LMS DB при каждом sync.

    Шкала МИМ: Интересант (L05) → Определяющийся (L08) → Первокурсник (L1) →
    Ученик (L2) → Работник (L25) → Стратег (L3) → Специалист (L4) →
    Практик (L5) → Мастер (L6) → Реформатор (L7) → Деятель (L8).

    Args:
        collected: digital_twins.data['2_collected']
        learning_rows: не используется (оставлен для совместимости сигнатуры)

    Returns:
        {"level": str, "code": str, "numeric": int, "event_date": str|None,
         "reason": str|None, "source": "lms"|"unknown"}
    """
    courses = collected.get('2_2_courses') or {}
    qual = courses.get('qualification_level')

    if qual and isinstance(qual, dict) and qual.get('level'):
        return {
            "level": qual['level'],
            "code": qual.get('code', ''),
            "numeric": qual.get('numeric', 0),
            "event_date": qual.get('event_date'),
            "reason": qual.get('reason'),
            "source": "lms",
        }

    # Нет данных — квалификация не назначена или LMS не подключен
    return {
        "level": "",
        "code": "",
        "numeric": 0,
        "event_date": None,
        "reason": None,
        "source": "unknown",
    }


# ═══════════════════════════════════════════════════════════
# IND.3.9.01 — IT Level (DigComp-адаптация, WP-151 Ф7a)
# ═══════════════════════════════════════════════════════════

def calc_it_level(collected: dict) -> dict:
    """ИТ-уровень (0-3) из поведенческих данных.

    IND.3.9.01: дополняет декларативный IND.1 it_level.
    Если есть данные coding/IWE → вычисляем объективно.
    Иначе → fallback на декларативный (передаётся через 1_declarative, не здесь).

    0 = не может установить ничего сам
    1 = может с подробной инструкцией
    2 = может с поддержкой (полный набор IWE)
    3 = может сам + помогает другим

    Args:
        collected: digital_twins.data['2_collected']

    Returns:
        {"it_level": int, "source": "auto"|"insufficient_data", "evidence": dict}
    """
    coding = collected.get('2_6_coding') or {}
    iwe = collected.get('2_7_iwe') or {}
    time_data = collected.get('2_4_time') or {}

    coding_seconds_30d = coding.get('coding_seconds_30d', 0) or 0
    coding_hours_30d = coding_seconds_30d / 3600
    coding_active_days = coding.get('coding_active_days_30d', 0) or 0
    commits_30d = iwe.get('commits_30d', 0) or 0
    day_opens = iwe.get('day_opens_total', 0) or 0
    ai_chats = time_data.get('ai_chats_total', 0) or 0

    evidence = {
        "coding_hours_30d": round(coding_hours_30d, 1),
        "coding_active_days": coding_active_days,
        "commits_30d": commits_30d,
        "day_opens": day_opens,
        "ai_chats": ai_chats,
    }

    has_coding_data = coding_hours_30d > 0 or commits_30d > 0

    if not has_coding_data:
        return {"it_level": None, "source": "insufficient_data", "evidence": evidence}

    level = 0

    # Level 1: базовое использование (есть AI-чаты или начал coding)
    if ai_chats >= 3 or coding_hours_30d >= 1:
        level = 1

    # Level 2: регулярное использование IWE (coding + commits + day_opens)
    if coding_hours_30d >= 10 and commits_30d >= 5:
        level = 2

    # Level 3: продвинутый (систематический coding + IWE + day_opens)
    if coding_hours_30d >= 40 and commits_30d >= 20 and day_opens >= 5:
        level = 3

    return {"it_level": level, "source": "auto", "evidence": evidence}


# ═══════════════════════════════════════════════════════════
# IND.3.12.01 — Delivery Style Adaptation (WP-151 Ф7a)
# ═══════════════════════════════════════════════════════════

def calc_delivery_style(collected: dict, student_stage: int) -> dict:
    """Рекомендуемый стиль подачи из поведенческих данных + ступени.

    IND.3.12.01: дополняет декларативный IND.1.5 style.
    Авто-адаптация: если поведение указывает на другой стиль, чем заявленный.

    Логика:
    - Ступень 0-1: detailed + examples, 15-20 мин
    - Ступень 2: mixed, 20-30 мин
    - Ступень 3-4: concise + tasks, 30-60 мин

    Коррекция по данным: если пользователь быстро проходит уроки → сократить.
    Если часто просит подробности → расширить.

    Returns:
        {"format": str, "duration_min": int, "complexity": str, "source": "auto"}
    """
    courses = collected.get('2_2_courses') or {}
    practice = collected.get('2_3_practice') or {}
    operations = collected.get('2_8_operations') or {}

    marathon_steps = courses.get('marathon_steps_total', 0) or 0
    marathon_tasks = practice.get('marathon_tasks_total', 0) or 0
    feed_completed = courses.get('feed_completed_total', 0) or 0

    # Коэффициент практичности: доля заданий от уроков
    practice_ratio = marathon_tasks / max(marathon_steps, 1)

    # Базовый стиль по ступени
    if student_stage <= 1:
        fmt = "detailed"
        duration = 20
        complexity = "accessible"
    elif student_stage == 2:
        fmt = "mixed"
        duration = 25
        complexity = "standard"
    else:
        fmt = "concise"
        duration = 30
        complexity = "professional"

    # Коррекция: высокая практичность → больше задач
    if practice_ratio > 0.6 and marathon_tasks >= 5:
        fmt = "tasks-first"

    # Коррекция: много дайджестов → предпочитает краткий формат
    if feed_completed >= 20 and marathon_steps < 5:
        fmt = "digest"
        duration = 15

    return {
        "format": fmt,
        "duration_min": duration,
        "complexity": complexity,
        "source": "auto",
        "evidence": {
            "practice_ratio": round(practice_ratio, 2),
            "marathon_steps": marathon_steps,
            "feed_completed": feed_completed,
        },
    }


# ═══════════════════════════════════════════════════════════
# IND.3.13.01 — Notification Responsiveness (WP-151 Ф7a, WP-117)
# ═══════════════════════════════════════════════════════════

def calc_notification_responsiveness(collected: dict) -> dict:
    """Отзывчивость на уведомления (0-100).

    IND.3.13.01: используется nudge-системой (WP-117) для адаптации
    частоты и типа уведомлений.

    Логика:
    - Доля открытых напоминаний от доставленных
    - Разнообразие типов уведомлений, на которые реагирует
    - Тренд: 7d vs 30d (улучшается или ухудшается)

    Returns:
        {"score": float 0-100, "trend": "improving"|"stable"|"declining", "evidence": dict}
    """
    notifications = collected.get('2_5_notifications') or {}
    operations = collected.get('2_8_operations') or {}

    notif_total = notifications.get('notifications_total', 0) or 0
    notif_7d = notifications.get('notifications_7d', 0) or 0
    notif_30d = notifications.get('notifications_30d', 0) or 0
    notif_types = notifications.get('notification_types', 0) or 0

    reminders_delivered = operations.get('reminders_delivered', 0) or 0
    reminders_opened = operations.get('reminders_opened', 0) or 0

    evidence = {
        "notif_total": notif_total,
        "notif_7d": notif_7d,
        "notif_30d": notif_30d,
        "notif_types": notif_types,
        "reminders_delivered": reminders_delivered,
        "reminders_opened": reminders_opened,
    }

    if notif_total == 0:
        return {"score": 50.0, "trend": "stable", "evidence": evidence}

    # Компонент 1: конверсия напоминаний (50%)
    reminder_rate = reminders_opened / max(reminders_delivered, 1)
    reminder_score = min(reminder_rate * 100, 100)

    # Компонент 2: разнообразие типов (20%)
    # 6+ типов = полный балл
    type_score = min(notif_types / 6 * 100, 100)

    # Компонент 3: интенсивность за последние 30 дней (30%)
    intensity_score = min(notif_30d / 15 * 100, 100)

    score = reminder_score * 0.50 + type_score * 0.20 + intensity_score * 0.30

    # Тренд: сравниваем 7d rate с 30d rate
    if notif_30d > 0:
        weekly_rate = notif_7d / 7
        monthly_rate = notif_30d / 30
        if monthly_rate > 0:
            ratio = weekly_rate / monthly_rate
            if ratio > 1.3:
                trend = "improving"
            elif ratio < 0.7:
                trend = "declining"
            else:
                trend = "stable"
        else:
            trend = "stable"
    else:
        trend = "stable"

    return {"score": round(score, 1), "trend": trend, "evidence": evidence}


# ═══════════════════════════════════════════════════════════
# IND.3.14.01 — Learning Autonomy (WP-151 Ф7a, WP-117)
# ═══════════════════════════════════════════════════════════

def calc_learning_autonomy(collected: dict, student_stage: int) -> dict:
    """Учебная автономность (0-100).

    IND.3.14.01: мера самостоятельности ученика. Используется nudge-системой
    для определения интенсивности подталкивания: высокая автономность → меньше nudge.

    Компоненты:
    - Инициативность: session_start без предварительного напоминания
    - Регулярность: дни/неделю
    - Разнообразие: сколько режимов использует (марафон, лента, тренировки, AI)
    - Ступень: baseline от student_stage

    Returns:
        {"score": float 0-100, "components": dict}
    """
    time_data = collected.get('2_4_time') or {}
    account = collected.get('2_1_account') or {}
    courses = collected.get('2_2_courses') or {}
    practice = collected.get('2_3_practice') or {}
    operations = collected.get('2_8_operations') or {}
    notifications = collected.get('2_5_notifications') or {}

    sessions_total = account.get('sessions_total', 0) or 0
    events_7d = time_data.get('events_last_7d', 0) or 0
    active_days = time_data.get('active_days', 0) or 0
    marathon_steps = courses.get('marathon_steps_total', 0) or 0
    feed_completed = courses.get('feed_completed_total', 0) or 0
    training_passed = practice.get('training_passed_total', 0) or 0
    ai_chats = time_data.get('ai_chats_total', 0) or 0
    reminders_delivered = operations.get('reminders_delivered', 0) or 0

    # 1. Инициативность (30%): сессии без напоминания
    # Аппроксимация: sessions - reminders_delivered = самостоятельные входы
    self_initiated = max(sessions_total - reminders_delivered, 0)
    initiative_ratio = self_initiated / max(sessions_total, 1)
    initiative_score = min(initiative_ratio * 100, 100)

    # 2. Регулярность (25%): дни/неделю нормализовано
    regularity_raw = events_7d / 7
    regularity_score = min(regularity_raw / 0.8 * 100, 100)

    # 3. Разнообразие режимов (20%): сколько разных типов активности
    modes_used = sum([
        1 if marathon_steps > 0 else 0,
        1 if feed_completed > 0 else 0,
        1 if training_passed > 0 else 0,
        1 if ai_chats > 0 else 0,
    ])
    diversity_score = min(modes_used / 3 * 100, 100)  # 3 из 4 = 100%

    # 4. Ступень baseline (25%): Stage прямо отражает автономность
    stage_score = min(student_stage / 4 * 100, 100)

    score = (
        initiative_score * 0.30
        + regularity_score * 0.25
        + diversity_score * 0.20
        + stage_score * 0.25
    )

    return {
        "score": round(score, 1),
        "components": {
            "initiative": round(initiative_score, 1),
            "regularity": round(regularity_score, 1),
            "diversity": round(diversity_score, 1),
            "stage_baseline": round(stage_score, 1),
        },
    }


# ═══════════════════════════════════════════════════════════
# IND.3.2.04 — МУЛЬТИПЛИКАТОР IWE (WP-218 Ф3/Ф8.3)
# ═══════════════════════════════════════════════════════════

def calc_IND_3_2_04_daily_multiplier(event_rows: list[dict] | None, date_str: str) -> float | None:
    """Мультипликатор IWE за день.

    Priority:
      1. Если есть domain_event[event_type=day_close] за date_str — берём multiplier
         из payload напрямую (WP-299 Ф5, commit 98dc07b).
      2. Fallback: вычисляем из domain_event[coding_time] + domain_event[wp_completed]
         (legacy режим, до деплоя day_close эмиссии).

    Unit: ratio (e.g. 5.2 = 5.2x AI-leverage)
    Guard: fallback режим — если coding_time < 0.5h → None (нет значимых данных WakaTime)
    """
    if not event_rows:
        return None  # данные появятся после WP-218 Ф8.3 (интеграция domain_event в профайлер)

    # ── Priority 1: direct day_close multiplier (WP-299 Ф5) ──
    day_close_events = [
        e for e in event_rows
        if e.get("event_type") == "day_close"
        and (str(e.get("occurred_at") or ""))[:10] == date_str
    ]
    if day_close_events:
        # Берём multiplier из первого day_close события за день
        # (эмиссия идемпотентна: external_id = "day-close-YYYY-MM-DD" → одно событие/день)
        multiplier = (day_close_events[0].get("payload") or {}).get("multiplier")
        if multiplier is not None:
            try:
                return round(float(multiplier), 2)
            except (ValueError, TypeError):
                pass  # corrupted payload → fallback

    # ── Priority 2: fallback calculation from coding_time + wp_completed ──
    waka_seconds = sum(
        (e.get("payload") or {}).get("total_seconds", 0)
        for e in event_rows
        if e.get("event_type") == "coding_time"
        and (str(e.get("occurred_at") or ""))[:10] == date_str
    )
    waka_hours = waka_seconds / 3600

    if waka_hours < 0.5:
        return None

    budget_hours = sum(
        float((e.get("payload") or {}).get("budget_hours", 0) or 0)
        for e in event_rows
        if e.get("event_type") == "wp_completed"
        and ((e.get("payload") or {}).get("date") or "") == date_str
    )

    return round(budget_hours / waka_hours, 2) if waka_hours > 0 else None


# ═══════════════════════════════════════════════════════════
# ПРОФИЛЬ ВЫПУСКНИКА — PD.FORM.093 (WP-151 Calc engine v2.0)
# ═══════════════════════════════════════════════════════════

def _to_idx_1to5(value: float, thresholds: tuple) -> int:
    """Перевод непрерывного значения в порядковый 1-5 по 4 порогам."""
    if value < thresholds[0]:
        return 1
    elif value < thresholds[1]:
        return 2
    elif value < thresholds[2]:
        return 3
    elif value < thresholds[3]:
        return 4
    return 5


def calc_graduate_profile(
    collected: dict,
    rcs_indices: Optional[dict] = None,
    agency_index: Optional[float] = None,
    as_of: Optional[datetime] = None,
) -> dict:
    """Профиль 8 характеристик выпускника программы ЛР (PD.FORM.093 §5.2-5.3).

    Calc engine v2.0 (WP-151 Ф7a v2.0, 2026-05-11).
    Слой auto (непрерывный): прокси из domain events / RCS.
    Слой self (опрос): stress_resilience, resourcefulness — None до Ф10 (GAD-7).

    Характеристика ≠ рычаг (PD.FORM.093 §2, HD pending): это выходные свойства,
    не механизмы воздействия (рычаги = M1/M2/M4/W из RCS, FORM.089).

    Returns:
        {clarity_idx, agency_idx, composure_idx, regularity_idx,
         production_capacity_idx, productivity_idx, resourcefulness_idx,
         stress_resilience_idx, metric_gate_passed, confidence,
         computed_at, evidence}
    """
    now = as_of if as_of is not None else datetime.now(timezone.utc)
    rcs = rcs_indices or {}
    evidence: dict = {}

    # ── 1. Ясность (clarity): W + M4 → прокси через «понимание цели и пути»
    w_idx = (rcs.get("W") or {}).get("idx") or 0
    m4_idx = (rcs.get("M4") or {}).get("idx") or 0
    if w_idx or m4_idx:
        clarity_idx = max(1, round((w_idx + m4_idx) / 2.0))
        evidence["clarity"] = {"w_idx": w_idx, "m4_idx": m4_idx, "proxy": "W+M4/2"}
    else:
        clarity_idx = 1
        evidence["clarity"] = {"proxy": "W+M4 absent, defaulted to 1"}

    # ── 2. Агентность (agency): integral_agency_index → 1-5 шкала
    if agency_index is not None:
        agency_idx = _to_idx_1to5(agency_index, (20.0, 40.0, 60.0, 80.0))
        evidence["agency"] = {"integral_index": agency_index}
    else:
        agency_idx = 1
        evidence["agency"] = {"proxy": "integral_agency absent, defaulted to 1"}

    # ── 3. Собранность (composure): M1 (все ступени) → прямое зеркало
    m1_idx = (rcs.get("M1") or {}).get("idx") or 0
    composure_idx = max(1, m1_idx)
    evidence["composure"] = {"m1_idx": m1_idx}

    # ── 4. Регулярность (regularity): дней учёбы в неделю → 1-5
    regularity_raw = calc_slot_regularity(collected, as_of=now)
    days_per_week = round(regularity_raw * 7, 2)
    regularity_idx = _to_idx_1to5(days_per_week, (1.0, 3.0, 5.0, 6.5))
    evidence["regularity"] = {"days_per_week": days_per_week, "slot_regularity": regularity_raw}

    # ── 5. Способность производить (production_capacity): РП/мес
    wps_list = collected.get("work_products") or []
    wp_completed = [
        wp for wp in wps_list
        if isinstance(wp, dict) and wp.get("status") in ("done", "closed", "completed")
    ]
    first_wp_date: Optional[datetime] = None
    for wp in wps_list:
        if not isinstance(wp, dict):
            continue
        for field in ("created_at", "started_at", "date"):
            raw = wp.get(field)
            if raw:
                try:
                    parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                    if first_wp_date is None or parsed < first_wp_date:
                        first_wp_date = parsed
                except (ValueError, TypeError):
                    pass
    months_active = 1.0
    if first_wp_date:
        first_naive = (
            first_wp_date.astimezone(timezone.utc).replace(tzinfo=None)
            if first_wp_date.tzinfo is not None else first_wp_date
        )
        now_naive = now.astimezone(timezone.utc).replace(tzinfo=None) if now.tzinfo is not None else now
        delta_days = (now_naive - first_naive).days
        months_active = max(1.0, delta_days / 30.0)
    wp_per_month = len(wp_completed) / months_active
    production_capacity_idx = _to_idx_1to5(wp_per_month, (0.5, 1.5, 3.0, 5.0))
    evidence["production_capacity"] = {
        "wp_completed_total": len(wp_completed),
        "months_active": round(months_active, 1),
        "wp_per_month": round(wp_per_month, 2),
    }

    # ── 6. Продуктивность (productivity): day_open_ratio прокси
    day_opens = collected.get("day_opens_last_30d") or collected.get("day_open_count_30d")
    if day_opens is not None:
        ratio = min(float(day_opens) / 22.0, 1.0)  # 22 рабочих дня
        productivity_idx = _to_idx_1to5(ratio, (0.25, 0.50, 0.75, 0.90))
        evidence["productivity"] = {"day_opens_30d": day_opens, "ratio_22d": round(ratio, 2), "proxy": "day_open_ratio"}
    else:
        productivity_idx = None
        evidence["productivity"] = {"proxy": "day_open_count absent — pending Ф10"}

    # ── 7. Ресурсность (resourcefulness): WakaTime стабильность прокси
    waka_days = collected.get("waka_active_days_last_30d")
    if waka_days is not None:
        stability = min(float(waka_days) / 22.0, 1.0)
        resourcefulness_idx = _to_idx_1to5(stability, (0.25, 0.50, 0.75, 0.90))
        evidence["resourcefulness"] = {"waka_active_days_30d": waka_days, "stability": round(stability, 2), "proxy": "waka_stability"}
    else:
        resourcefulness_idx = None
        evidence["resourcefulness"] = {"proxy": "waka absent — pending Ф10/WakaTime integration"}

    # ── 8. Стрессоустойчивость (stress_resilience): GAD-7 — pending Ф10
    stress_resilience_idx = None
    evidence["stress_resilience"] = {"proxy": "GAD-7 pending Ф10"}

    # ── Gate (PD.FORM.093 §5.4)
    # Gate возвращается ТОЛЬКО при полных данных (все 8 характеристик != None).
    # При неполных supporting (Ф10 GAD-7 не реализован) — gate=None, чтобы не давать
    # false-positive «прошёл» по 5 gate-характеристикам с пустыми supporting.
    gate_fields = [clarity_idx, agency_idx, composure_idx, regularity_idx, production_capacity_idx]
    support_fields = [productivity_idx, resourcefulness_idx, stress_resilience_idx]
    gate_passed: Optional[bool] = None
    if all(v is not None for v in gate_fields) and all(v is not None for v in support_fields):
        gate_ok = (
            clarity_idx >= 4
            and agency_idx >= 4
            and composure_idx >= 4
            and regularity_idx >= 4
            and production_capacity_idx >= 4
        )
        support_ok = all(v >= 3 for v in support_fields)
        gate_passed = gate_ok and support_ok

    # ── Уверенность: доля заполненных характеристик (None = нет данных)
    all_indices = gate_fields + support_fields
    filled = sum(1 for v in all_indices if v is not None)
    confidence = round(filled / len(all_indices), 2)

    return {
        "clarity_idx": clarity_idx,
        "agency_idx": agency_idx,
        "composure_idx": composure_idx,
        "regularity_idx": regularity_idx,
        "production_capacity_idx": production_capacity_idx,
        "productivity_idx": productivity_idx,
        "resourcefulness_idx": resourcefulness_idx,
        "stress_resilience_idx": stress_resilience_idx,
        "metric_gate_passed": gate_passed,
        "confidence": confidence,
        "computed_at": now.isoformat(),
        "evidence": evidence,
    }


# ═══════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════

def calculate_derived(data: dict, learning_rows: list[dict] | None = None, event_rows: list[dict] | None = None, as_of: Optional[datetime] = None) -> dict:
    """Вычислить все derived-индикаторы из digital_twins.data (v2.0+).

    Args:
        data: digital_twins.data (полный) ИЛИ 2_collected секция (legacy backward compat).
            v2.0+: принимает полный data, извлекает data['2_collected'] внутри.
            legacy: если нет ключа '2_collected' — трактует как уже-извлечённую секцию.
        learning_rows: список dict из development.learning_history (v0.8+).
            Каждый dict: {element_id, element_type, area, depth, passed, ...}.
            При None — mastery_by_area возвращает нули, gaps — пустые списки (PD.SPEC.001 §3).
        event_rows: список dict из learning.domain_event (v2.0+, WP-218 Ф8.3).
            Каждый dict: {event_type, occurred_at, payload}.
            При None — IND.3.2.04 (мультипликатор) возвращает null.

    Returns:
        dict для записи в digital_twins.data['3_derived']:
        {
            "3_1_agency": {"slot_regularity": float, ...},
            "3_2_mastery": {"multiplier_today": float|None, "multiplier_7d_avg": float|None},
            "3_4_qualification": {"stage": int, "stage_id": str, "path": str, ...},
            "3_5_mastery": {"mastery_by_area": {...}},
            "3_6_worldview": {"worldview_gaps": [...]},
            "3_8_degree": {"level": str, "code": str, "numeric": int, ...},
            "3_9_it_level": {"it_level": int|None, ...},
            "3_10_integral": {"index": float, ...},
            "3_12_delivery_style": {"format": str, ...},
            "3_13_notification_resp": {"score": float, ...},
            "3_14_learning_autonomy": {"score": float, ...},
            "3_GP_graduate": {"clarity_idx": int, ..., "metric_gate_passed": bool|None, "confidence": float, ...},
            "calculated_at": ISO timestamp,
            "engine_version": "2.0",
        }
    """
    # v2.0 backward compat: поддержка и полного data, и legacy 2_collected-only dict
    if "2_collected" in data:
        collected = data.get("2_collected") or {}
    else:
        collected = data  # legacy: caller уже передал extracted 2_collected

    if not collected:
        return {}

    try:
        from datetime import timedelta
        now = as_of if as_of is not None else datetime.now(timezone.utc)
        stage_result = calc_student_stage(collected, as_of=now)
        agency_result = calc_integral_agency_index(collected, as_of=now)
        regularity = calc_slot_regularity(collected, as_of=now)
        student_stage = stage_result.get("stage", 0)

        # ── Ф3/Ф8.3: IND.3.2.04 мультипликатор (7d rolling) ─────────────────
        today_str = now.strftime("%Y-%m-%d")
        multiplier_today = calc_IND_3_2_04_daily_multiplier(event_rows, today_str)
        multiplier_7d: float | None = None
        if event_rows:
            daily_mults = [
                m for m in (
                    calc_IND_3_2_04_daily_multiplier(
                        event_rows, (now - timedelta(days=i)).strftime("%Y-%m-%d")
                    )
                    for i in range(7)
                )
                if m is not None
            ]
            multiplier_7d = round(sum(daily_mults) / len(daily_mults), 2) if daily_mults else None

        derived = {
            "3_1_agency": {
                "slot_regularity": round(regularity, 3),
                "slot_days_per_week": round(regularity * 7, 1),
            },
            "3_2_mastery": {
                "multiplier_today": multiplier_today,
                "multiplier_7d_avg": multiplier_7d,
            },
            "3_4_qualification": stage_result,
            "3_10_integral": agency_result,
            "calculated_at": now.isoformat(),
            "engine_version": "2.0",
        }

        # ── Ф5: BKT из learning_history (WP-151 Ф5) ─────────────────────────
        if learning_rows is not None:
            mastery = calc_mastery_by_area(learning_rows)
            gaps = calc_worldview_gaps(learning_rows, student_stage)
            derived["3_5_mastery"] = {"mastery_by_area": mastery}
            derived["3_6_worldview"] = {
                "worldview_gaps": gaps,
                "bkt_params": {
                    "p_l0": _BKT_P_L0,
                    "p_t": _BKT_P_T,
                    "p_g": _BKT_P_G,
                    "p_s": _BKT_P_S,
                    "mastery_threshold": _BKT_MASTERY_THRESHOLD,
                },
            }

        # ── Ф7a: 5 осей MVP + notification/autonomy (WP-151 Ф7a) ──────────
        derived["3_8_degree"] = calc_qualification_degree(collected, learning_rows)
        derived["3_9_it_level"] = calc_it_level(collected)
        derived["3_12_delivery_style"] = calc_delivery_style(collected, student_stage)
        derived["3_13_notification_resp"] = calc_notification_responsiveness(collected)
        derived["3_14_learning_autonomy"] = calc_learning_autonomy(collected, student_stage)

        # ── Calc v2.0: профиль выпускника (WP-151 Calc engine v2.0, PD.FORM.093) ──
        rcs_indices = stage_result.get("rcs_indices") or {}
        agency_idx_value = agency_result.get("index") if agency_result else None
        derived["3_GP_graduate"] = calc_graduate_profile(
            collected,
            rcs_indices=rcs_indices,
            agency_index=agency_idx_value,
            as_of=now,
        )

        return derived

    except Exception as e:
        logger.error(f"[DT Calc] Error calculating derived: {e}")
        return {}
