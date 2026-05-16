"""Контракт: Adapter → Hub Core."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class RawEvent:
    """Единый формат события от любого адаптера.

    Adapter заполняет все поля. Hub Core не знает, как adapter
    получил данные (HTTP API, SQL, webhook, файлы).
    """

    source: str  # 'lms', 'bot', 'club', 'iwe'
    external_id: str  # уникальный ID из источника (source-scoped)
    user_ref: dict  # {'telegram_id': 123} или {'lms_user_id': 456}
    event_type: str  # 'section_completed', 'answer_submitted', ...
    payload: dict = field(default_factory=dict)
    confidence: float = 1.0  # 0.0–1.0
    occurred_at: Optional[datetime] = None  # когда произошло (UTC)

    def __post_init__(self):
        if self.occurred_at is None:
            self.occurred_at = datetime.utcnow()


KNOWN_SOURCES = frozenset({"bot", "lms", "club", "iwe"})

# WP-109 Ф5: event_class для platform_outbox routing
EVENT_CLASS_MAP: dict[str, str] = {
    # LEARNING — образование, IWE-деятельность, прогресс
    "section_completed": "LEARNING",
    "course_started": "LEARNING",
    "course_completed": "LEARNING",
    "text_submitted": "LEARNING",
    "table_submitted": "LEARNING",
    "test_passed": "LEARNING",
    "test_failed": "LEARNING",
    "task_submitted": "LEARNING",
    "assignment_submitted": "LEARNING",
    "learning_session": "LEARNING",
    "pomodoro_completed": "LEARNING",
    "qualification_changed": "LEARNING",
    "coding_time": "LEARNING",
    "commit_created": "LEARNING",
    "wp_completed": "LEARNING",
    "content_published": "LEARNING",
    "knowledge_extracted": "LEARNING",
    "pack_updated": "LEARNING",
    "distinction_added": "LEARNING",
    "method_described": "LEARNING",
    "fmt_commit_merged": "LEARNING",
    "day_open": "LEARNING",
    "day_close": "LEARNING",
    "week_plan_created": "LEARNING",
    "note_to_capture": "LEARNING",
    "workbook_push": "LEARNING",
    "session_completed": "LEARNING",
    "day_plan_created": "LEARNING",
    "dt_collect_snapshot": "LEARNING",
    "marathon_start": "LEARNING",
    "marathon_complete": "LEARNING",
    "answer_submitted": "LEARNING",
    "feed_action": "LEARNING",
    "note_saved": "LEARNING",
    "decision_approve": "LEARNING",
    "decision_reject": "LEARNING",
    "decision_redirect": "LEARNING",
    "decision_architectural": "LEARNING",
    "decision_strategic": "LEARNING",
    # ECONOMIC — монетизация, AI-взаимодействие
    "ai_chat": "ECONOMIC",
    "ai_interaction": "ECONOMIC",
    # IDENTITY — профиль, роли (пока не задействован)
    # SOCIAL — сообщество
    "post_created": "SOCIAL",
    "like_given": "SOCIAL",
    "topic_created": "SOCIAL",
    "comment_created": "SOCIAL",
}
DEFAULT_EVENT_CLASS = "LEARNING"

KNOWN_EVENT_TYPES = frozenset({
    # bot
    "ai_chat", "marathon_start", "marathon_complete",
    "answer_submitted", "feed_action", "note_saved",
    # lms (mapped from ACTION_TYPE_MAP in adapters/lms.py)
    "text_submitted", "table_submitted", "test_passed", "task_submitted",
    "ai_interaction", "topic_created", "comment_created", "pomodoro_completed",
    # lms (legacy names)
    "section_completed", "course_started", "course_completed",
    "test_failed", "assignment_submitted",
    "learning_session", "qualification_changed",
    # club
    "post_created", "like_given",
    # iwe — систематичность
    "day_open", "day_close", "week_plan_created",
    "note_to_capture",
    # iwe — время
    "coding_time", "commit_created",
    # iwe — РП в физическом мире
    "wp_completed", "content_published",
    # iwe — качество (вклад в знание)
    "knowledge_extracted", "pack_updated", "distinction_added", "method_described",
    # iwe — развитие платформы и сообщества
    "fmt_commit_merged",
    # iwe — webhook (SC.020)
    "workbook_push",
    # iwe — dt-collect snapshot (ADR-009 dual-write)
    "dt_collect_snapshot",
    # iwe (legacy)
    "day_plan_created", "session_completed",
    # iwe — decision register (WP-109 Ф7, PD.SOTA.003)
    # Решения пользователя во время Claude-сессии внутри IWE.
    "decision_approve", "decision_reject", "decision_redirect",
    "decision_architectural", "decision_strategic",
})
