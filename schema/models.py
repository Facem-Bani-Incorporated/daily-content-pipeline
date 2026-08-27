from datetime import date
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field


class EventCategory(str, Enum):
    # ── Existing free categories ──
    WAR_CONFLICT = "war_conflict"
    TECH_INNOVATION = "tech_innovation"
    SCIENCE_DISCOVERY = "science_discovery"
    POLITICS_STATE = "politics_state"
    CULTURE_ARTS = "culture_arts"
    NATURAL_DISASTER = "natural_disaster"
    EXPLORATION = "exploration"
    RELIGION_PHIL = "religion_phil"

    # ── NEW: PRO-only categories ──
    PERSONALITIES = "personalities"
    MEDIA = "media"
    SPORT = "sport"


# Set of categories that are exclusively PRO content
PRO_CATEGORIES = {
    EventCategory.PERSONALITIES.value,
    EventCategory.MEDIA.value,
    EventCategory.SPORT.value,
}


class Translations(BaseModel):
    en: str = "Data pending"
    ro: str = "Data pending"
    es: str = "Data pending"
    de: str = "Data pending"
    fr: str = "Data pending"


def _empty_translations() -> "Translations":
    """Blank per-language strings — used as the notification default so the app's
    client-side fallback kicks in when a language has no generated hook."""
    return Translations(en="", ro="", es="", de="", fr="")


class QuizOption(BaseModel):
    id: str
    text: str


class QuizQuestion(BaseModel):
    id: str
    question: str
    options: List[QuizOption]
    correct_id: str = Field(alias="correctId")
    explanation: str

    class Config:
        populate_by_name = True


class QuizTranslations(BaseModel):
    en: List[QuizQuestion] = []
    ro: List[QuizQuestion] = []
    es: List[QuizQuestion] = []
    de: List[QuizQuestion] = []
    fr: List[QuizQuestion] = []


class DeepDiveChapter(BaseModel):
    title: str
    body: str


class DeepDive(BaseModel):
    """The long-form PRO narrative for one event, in one language.

    Structured rather than a single text blob for two reasons: the app renders the
    chapters, the timeline and the sources as distinct UI, and the *teaser* plus the
    chapter titles can be shipped to free users as the paywall pitch without the
    body text ever leaving the server.
    """
    chapters: List[DeepDiveChapter] = []
    timeline: List[str] = []      # "14:32 — the first signal reaches Lisbon"
    misconception: str = ""       # "what everyone gets wrong about this day"
    aftermath: List[str] = []     # consequences at +10y / +50y / +100y
    sources: List[str] = []       # "Author, Title (Year)" — never invented URLs
    teaser: str = ""              # opening ~70 words; safe to send to free users
    word_count: int = 0


class DeepDiveTranslations(BaseModel):
    en: Optional[DeepDive] = None
    ro: Optional[DeepDive] = None
    es: Optional[DeepDive] = None
    de: Optional[DeepDive] = None
    fr: Optional[DeepDive] = None


class EventDetail(BaseModel):
    category: EventCategory
    year: int
    event_date: date
    source_url: str
    title_translations: Translations
    narrative_translations: Translations
    # Long-form PRO narrative. None when the event predates the feature or the
    # generator failed — the app then shows no teaser at all rather than promising
    # content that does not exist.
    deep_dive: Optional[DeepDiveTranslations] = None
    # Per-language push-notification hook (TikTok-style). Two parallel Translations so the
    # Java backend can reuse its existing translations table for each.
    notification_title_translations: Translations = Field(default_factory=_empty_translations)
    notification_body_translations: Translations = Field(default_factory=_empty_translations)
    impact_score: float
    page_views_30d: int = 0
    gallery: List[str] = []
    quiz: Optional[QuizTranslations] = None

    # ── NEW PRO fields ──
    is_pro: bool = False
    location: Optional[str] = None


class DailyPayload(BaseModel):
    date_processed: date
    events: List[EventDetail]
    metadata: dict = {}