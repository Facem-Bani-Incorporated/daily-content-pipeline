from pydantic import BaseModel, Field, ConfigDict
from pydantic.alias_generators import to_camel
from typing import List, Optional
from datetime import date
from enum import Enum


class CamelModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )


class EventCategory(str, Enum):
    WAR_CONFLICT = "war_conflict"
    TECH_INNOVATION = "tech_innovation"
    SCIENCE_DISCOVERY = "science_discovery"
    POLITICS_STATE = "politics_state"
    CULTURE_ARTS = "culture_arts"
    NATURAL_DISASTER = "natural_disaster"
    EXPLORATION = "exploration"
    RELIGION_PHIL = "religion_phil"


class Translations(CamelModel):
    en: str
    ro: str
    es: str
    de: str
    fr: str


# ── Quiz ──

class QuizOption(CamelModel):
    id: str
    text: str

class QuizQuestion(CamelModel):
    id: str
    question: str
    options: List[QuizOption]
    correct_id: str
    explanation: str

class QuizTranslations(CamelModel):
    en: List[QuizQuestion]
    ro: List[QuizQuestion]
    es: List[QuizQuestion]
    de: List[QuizQuestion]
    fr: List[QuizQuestion]


# ── Event + Payload ──

class EventDetail(CamelModel):
    category: EventCategory
    year: int
    event_date: date
    source_url: str
    title_translations: Translations
    narrative_translations: Translations
    impact_score: float = Field(..., ge=0, le=100)
    page_views_30d: int = 0
    gallery: List[str] = []
    quiz: Optional[QuizTranslations] = None


class DailyPayload(CamelModel):
    date_processed: date
    events: List[EventDetail]
    metadata: dict = Field(default_factory=dict)