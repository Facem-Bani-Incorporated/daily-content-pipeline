from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import date
from enum import Enum

class EventCategory(str, Enum):
    WAR_CONFLICT = "war_conflict"
    TECH_INNOVATION = "tech_innovation"
    SCIENCE_DISCOVERY = "science_discovery"
    POLITICS_STATE = "politics_state"
    CULTURE_ARTS = "culture_arts"
    NATURAL_DISASTER = "natural_disaster"
    EXPLORATION = "exploration"
    RELIGION_PHIL = "religion_phil"

class Translations(BaseModel):
    en: str
    ro: str
    es: str
    de: str
    fr: str

class EventDetail(BaseModel):
    category: EventCategory
    year: int
    event_date: date
    source_url: str
    title_translations: Translations
    narrative_translations: Translations
    impact_score: float = Field(..., ge=0, le=100)
    page_views_30d: int = 0
    gallery: List[str] = []

class DailyPayload(BaseModel):
    date_processed: date
    api_secret: str
    main_event: EventDetail
    secondary_events: List[EventDetail] # Uniformizat!
    metadata: dict = Field(default_factory=dict)