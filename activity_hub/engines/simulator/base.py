"""Simulator engine base — WP-319 Ф2.

# see DP.SC.133, DP.ROLE.043

Абстракция для всех сценариев симулятора. Принцип: один файл = один сценарий.
Новый сценарий = наследник Scenario, без правки engine или UI.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class SimulatorProfile:
    """Поведенческий профиль пилота — входные данные для симуляции."""
    account_id: str = ""
    # bh-индексы (0–5)
    s: int = 0    # bh.sys  систематичность
    t: int = 0    # bh.inv  инвестированное время
    m: int = 0    # bh.met  методичность
    w: int = 0    # bh.awr  осведомлённость
    a: int = 0    # bh.agn  агентность
    stb: int = 0  # bh.stb  устойчивость
    # Сырые метрики (для пересчёта при изменении паттерна)
    hours_per_week: float = 0.0
    days_per_week: float = 0.0
    total_hours: float = 0.0
    max_gap_days: int = 30
    # Источник (real = Neon, preset = типовой профиль)
    source: str = "preset"
    # Подтверждённая ступень: max(cp_assessments, stage_transitions)
    confirmed_stage: int = 0


@dataclass
class ScenarioRow:
    """Одна строка в результате — состояние на конкретной неделе."""
    week: int
    stage: int
    bh_sys: int
    bh_inv: int
    bh_met: int
    bh_awr: int
    bh_agn: int
    bh_stb: int
    total_hours: float
    bottleneck: str = ""

    def as_dict(self) -> dict:
        return {
            "week": self.week,
            "stage": self.stage,
            "bh.sys": self.bh_sys,
            "bh.inv": self.bh_inv,
            "bh.met": self.bh_met,
            "bh.awr": self.bh_awr,
            "bh.agn": self.bh_agn,
            "bh.stb": self.bh_stb,
            "total_hours": self.total_hours,
            "bottleneck": self.bottleneck,
        }


@dataclass
class ScenarioResult:
    """Результат сценария — траектория + метаданные."""
    scenario_id: str
    rows: list[ScenarioRow] = field(default_factory=list)
    rows_dicts: list[dict] = field(default_factory=list)  # для S2/S3 (нет ScenarioRow)
    bottleneck_key: str = ""
    bottleneck_label: str = ""
    pilot_text: str = ""          # готовый текст для Pilot-mode (без кодов)
    recommendation: str = ""      # одна конкретная рекомендация
    config_version: str = ""

    def as_dicts(self) -> list[dict]:
        """Универсальный геттер: ScenarioRow-список или rows_dicts (S2/S3)."""
        if self.rows_dicts:
            return self.rows_dicts
        return [r.as_dict() for r in self.rows]


class Scenario(ABC):
    """Базовый класс сценария. Один файл scenarios/SN.py = один наследник."""

    scenario_id: str = ""
    name: str = ""
    description: str = ""

    @abstractmethod
    def run(
        self,
        profile: SimulatorProfile,
        params: dict,
        horizon_weeks: int = 12,
    ) -> ScenarioResult:
        """Запустить симуляцию.

        params — словарь переопределений паттерна:
          hours_per_week, days_per_week, max_gap_days, etc.
        Возвращает ScenarioResult с заполненными rows.
        """
        ...
