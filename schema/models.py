from pydantic import BaseModel, Field, ConfigDict
from pydantic.alias_generators import to_camel
from typing import List, Optional, Dict
from datetime import date
from enum import Enum

# 1. Clasa de bază care face magia: folosești snake_case în Python, dar exportă camelCase
class CamelModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True # Îți permite să folosești snake_case la inițializare
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

class DailyPayload(CamelModel):
    date_processed: date
    events: List[EventDetail]
    metadata: dict = Field(default_factory=dict)