"""LLM-парсер текстовых сценариев симулятора — WP-319 Ф4.

# see DP.SC.133, DP.ROLE.043

Принимает свободный текст → извлекает параметры симуляции через Claude Haiku
tool_use с prompt-кешем (минимум 2048 токенов для активации кеша).
Fallback: confidence < CONFIDENCE_THRESHOLD (0.7) → bh_overrides={}, fallback_sliders=True.
"""
from __future__ import annotations

import copy
import logging
import os
from typing import Any

import anthropic

logger = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5"

_client: anthropic.AsyncAnthropic | None = None


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is not None:
        return _client
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ANTHROPIC_API_KEY не задан. Установите переменную окружения."
        )
    _client = anthropic.AsyncAnthropic(api_key=api_key)
    return _client


# ── Системный промпт (≥2048 токенов для активации prompt-cache у Haiku) ───────

_SYSTEM_PROMPT = """\
Ты — парсер текстовых запросов для симулятора поведенческих характеристик Созидателя.
Извлеки параметры симуляции из произвольного текста. Отвечай только через инструмент parse_scenario.

## Сценарии

- **s1** (Траектория ступени): пользователь описывает паттерн занятий — сколько часов/нед, дней/нед, какие перерывы
- **s2** (Траектория баллов): пользователь описывает активность — уроки, рабочие продукты, коммиты, закрытие дней
- **s3** (Когортная динамика): пользователь спрашивает про группу/когорту («что будет со всей первой когортой»)

## Параметры S1

| Параметр | Тип | Примеры фраз |
|---|---|---|
| hours_per_week | float | «6ч/нед», «6 часов в неделю», «начну учиться 6 часов» |
| days_per_week | float | «5 дней в неделю», «каждый день»=7, «через день»=3.5 |
| max_gap_days | int | «не пропускать больше 3 дней», «перерыв до 5 дней»=5, «не делать паузу больше недели»=7 |
| m_delta_per_week | float | «буду проходить уроки», «буду читать Pack» ≈ 0.1 |
| w_delta_per_week | float | «буду закрывать недели», «буду закрывать неделю с оценкой 4» ≈ 0.1–0.15 |
| a_delta_per_week | float | «буду делать рабочие продукты», «буду делать WP», «буду создавать РП» ≈ 0.1 |

## Параметры S2

| Параметр | Тип | Примеры фраз |
|---|---|---|
| lesson_completed_per_week | float | «2 урока в нед», «буду проходить 3 урока каждую неделю» |
| wp_completed_per_month | float | «1 РП в месяц», «закрывать 2 рабочих продукта в месяц», «1 проект в месяц» |
| commit_created_per_week | float | «5 коммитов в нед», «буду делать 3 коммита в неделю» |
| day_close_per_week | float | «буду закрывать день каждый день»=7, «закрываю 3 дня в нед», «ежедневные итоги» |
| knowledge_extracted_per_week | float | «1 извлечение знаний в нед», «буду делать заметки раз в неделю» |

## Параметры S3

| Параметр | Тип | Значения и когда использовать |
|---|---|---|
| cohort_type | str | «wave1» — если упоминается «первая когорта», «7 пилотов», «наша группа», «первая волна»; «preset_all_stages» — если «все ступени», «разные ступени», «типовые профили», «от 1 до 5» |

## Правила сопоставления

- «перестану делать X» / «что если перестать делать X» / «брошу X» → параметр X = 0
- «каждый день» → days_per_week = 7; «через день» → 3.5; «5 дней» → 5; «рабочие дни» → 5
- Если параметр не упомянут — НЕ включать в bh_overrides
- «полгода» → horizon_weeks = 26; «год» → 52; «3 месяца» → 13; «квартал» → 13; по умолчанию → 12
- «неделя» в горизонте: «через 8 недель» → 8; «через 2 недели» → 2

## Правила confidence

Confidence = твоя уверенность 0.0–1.0 что ты правильно распознал намерение:
- Числовые параметры в явном виде («6 часов», «5 дней») → confidence ≥ 0.85
- **Delta-параметры без числа** (упомянуто закрытие недели / прохождение уроков / рабочие продукты):
  сигнал однозначен, даже без количественной оценки → confidence = 0.75
- Только качественное описание без параметров («буду стараться», «хочу улучшить») → confidence ≤ 0.5
- Противоречивые сигналы → confidence ≤ 0.4
- Полностью неинформативный текст (приветствие, случайный вопрос) → confidence ≤ 0.2

## Примеры разбора

**Пример 1:** «начну учиться 6ч/нед»
→ scenario_id=s1, bh_overrides={hours_per_week: 6.0}, horizon_weeks=12, confidence=0.9
→ explanation: «Пользователь планирует учиться 6 часов в неделю.»

**Пример 2:** «буду закрывать неделю с оценкой 4»
→ scenario_id=s1, bh_overrides={w_delta_per_week: 0.12}, horizon_weeks=12, confidence=0.75
→ explanation: «Пользователь планирует регулярно закрывать недели — сигнал роста осведомлённости.»

**Пример 3:** «что если перестать делать рабочие продукты»
→ scenario_id=s1, bh_overrides={a_delta_per_week: 0.0}, horizon_weeks=12, confidence=0.85
→ explanation: «Пользователь хочет увидеть что будет, если перестать создавать рабочие продукты.»

**Пример 4:** «буду проходить 2 урока в неделю и закрывать 1 рабочий продукт в месяц»
→ scenario_id=s2, bh_overrides={lesson_completed_per_week: 2.0, wp_completed_per_month: 1.0}, horizon_weeks=12, confidence=0.9

**Пример 5:** «что будет с первой когортой через 12 недель»
→ scenario_id=s3, bh_overrides={cohort_type: "wave1"}, horizon_weeks=12, confidence=0.85

**Пример 6:** «начну учиться 8 часов в неделю, посмотрим через полгода»
→ scenario_id=s1, bh_overrides={hours_per_week: 8.0}, horizon_weeks=26, confidence=0.9

**Пример 7:** «привет как дела»
→ scenario_id=s1, bh_overrides={}, horizon_weeks=12, confidence=0.1
→ explanation: «Текст не содержит параметров симуляции.»

## Важно

Никогда не включай параметры, которые явно не упомянуты в тексте.
Не угадывай — если сигнала нет, не добавляй параметр.
Если несколько сценариев подходят одновременно — выбирай тот, который упомянут явнее."""


# ── Tool definition ───────────────────────────────────────────────────────────

_PARSE_TOOL: dict[str, Any] = {
    "name": "parse_scenario",
    "description": (
        "Извлечь параметры симуляции из текстового запроса. "
        "Вернуть scenario_id, bh_overrides (только явно упомянутые параметры), "
        "horizon_weeks, confidence и explanation."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "scenario_id": {
                "type": "string",
                "enum": ["s1", "s2", "s3"],
                "description": "Наиболее подходящий сценарий",
            },
            "bh_overrides": {
                "type": "object",
                "description": "Переопределения параметров — только явно упомянутые",
                "additionalProperties": True,
            },
            "horizon_weeks": {
                "type": "integer",
                "description": "Горизонт симуляции в неделях",
                "minimum": 1,
                "maximum": 104,
            },
            "confidence": {
                "type": "number",
                "description": "Уверенность 0.0–1.0",
                "minimum": 0.0,
                "maximum": 1.0,
            },
            "explanation": {
                "type": "string",
                "description": "Краткое объяснение распознавания (1-2 предложения на русском)",
            },
        },
        "required": [
            "scenario_id", "bh_overrides", "horizon_weeks", "confidence", "explanation",
        ],
    },
}

CONFIDENCE_THRESHOLD = 0.7

_VALID_SCENARIO_IDS = frozenset({"s1", "s2", "s3"})

_FALLBACK: dict[str, Any] = {
    "scenario_id": "s1",
    "bh_overrides": {},
    "horizon_weeks": 12,
    "confidence": 0.0,
    "explanation": "Не удалось распознать параметры сценария.",
    "fallback_sliders": True,
}


def _make_fallback(explanation: str = "") -> dict[str, Any]:
    result = copy.deepcopy(_FALLBACK)
    if explanation:
        result["explanation"] = explanation
    return result


# ── Public API ────────────────────────────────────────────────────────────────

async def parse_scenario_text(text: str) -> dict[str, Any]:
    """Парсить текстовый запрос → параметры симуляции.

    Возвращает dict:
      {
        "scenario_id": "s1",
        "bh_overrides": {"hours_per_week": 6.0, ...},
        "horizon_weeks": 12,
        "confidence": 0.85,
        "explanation": "Пользователь планирует учиться 6 часов в неделю.",
        "fallback_sliders": False,
      }

    Если confidence < CONFIDENCE_THRESHOLD — fallback_sliders=True,
    bh_overrides может быть пустым (UI показывает слайдеры).

    Transient ошибки API (rate limit, connection): логируются + fallback.
    EnvironmentError (нет ключа): re-raise — это конфигурационная проблема.
    """
    client = _get_client()  # EnvironmentError если нет ключа — не глотать

    try:
        response = await client.messages.create(
            model=MODEL,
            max_tokens=512,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=[_PARSE_TOOL],
            tool_choice={"type": "tool", "name": "parse_scenario"},
            messages=[{"role": "user", "content": text}],
        )
    except anthropic.RateLimitError as e:
        logger.warning("[llm_parser] RateLimitError: %s", e)
        return _make_fallback("Превышен лимит запросов к модели. Используйте слайдеры.")
    except anthropic.APIConnectionError as e:
        logger.warning("[llm_parser] APIConnectionError: %s", e)
        return _make_fallback("Нет подключения к модели. Используйте слайдеры.")
    except anthropic.APIStatusError as e:
        logger.warning("[llm_parser] APIStatusError %s: %s", e.status_code, e.message)
        return _make_fallback("Ошибка модели. Используйте слайдеры.")
    except Exception as e:
        logger.warning("[llm_parser] Unexpected error: %s", e)
        return _make_fallback()

    tool_input: dict[str, Any] = {}
    for block in response.content:
        if block.type == "tool_use" and block.name == "parse_scenario":
            tool_input = block.input
            break

    if not tool_input:
        logger.warning("[llm_parser] tool_use block missing; stop_reason=%s", response.stop_reason)
        return _make_fallback()

    scenario_id = tool_input.get("scenario_id", "s1")
    if scenario_id not in _VALID_SCENARIO_IDS:
        logger.warning("[llm_parser] invalid scenario_id=%r, falling back to s1", scenario_id)
        scenario_id = "s1"

    horizon = int(tool_input.get("horizon_weeks", 12))
    horizon = max(1, min(104, horizon))

    confidence = float(tool_input.get("confidence", 0.0))
    confidence = max(0.0, min(1.0, confidence))

    return {
        "scenario_id": scenario_id,
        "bh_overrides": tool_input.get("bh_overrides", {}),
        "horizon_weeks": horizon,
        "confidence": confidence,
        "explanation": tool_input.get("explanation", ""),
        "fallback_sliders": confidence < CONFIDENCE_THRESHOLD,
    }


# Enable LangFuse tracing if configured (langfuse v4 @observe, graceful fallback)
if os.environ.get("LANGFUSE_SECRET_KEY"):
    try:
        from langfuse.decorators import observe as _lf_observe
        parse_scenario_text = _lf_observe(parse_scenario_text)
        logger.info("[LLM Parser] LangFuse tracing enabled")
    except ImportError:
        logger.warning("[LLM Parser] langfuse not installed — tracing disabled")
