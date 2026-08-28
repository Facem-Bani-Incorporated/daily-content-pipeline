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


class WorldEffects(BaseModel):
    """How one choice moves the four world meters. Deltas, not absolutes, so a run is
    the sum of its decisions. Range roughly -30..+30 each; the generator is told to
    make trade-offs rather than let every meter rise together."""
    stability: int = 0   # order vs chaos
    lives: int = 0       # human cost
    progress: int = 0    # science, industry, knowledge
    freedom: int = 0     # liberty vs control


class Actor(BaseModel):
    """A real faction or person with a stake in the outcome. Their standing moves with
    every choice, which is what turns four abstract meters into a political situation."""
    id: str
    name: str             # "The Bohemian Estates", "Maximilian of Bavaria"
    start: int = 50       # 0-100


class NodeFact(BaseModel):
    """A hard number about the situation, shown as a data strip above the choices.
    This is the antidote to a wall of prose: the player should see the constraints
    they are deciding under, not read about them."""
    label: str            # "Bohemian army"
    value: str            # "15,000, unpaid since spring"


class ChoiceOutcome(BaseModel):
    """The one concrete thing this choice commits, previewed on the card."""
    label: str            # "Troops committed"
    value: str            # "24,000"


class EndingStat(BaseModel):
    """A number from your world set against the real one. The ending's payload."""
    label: str            # "Deaths in the German lands"
    real: str             # "8 million"
    alt: str              # "under 1 million"


class UniverseChoice(BaseModel):
    id: str
    label: str            # short — it sits on a button
    detail: str           # one line of what this actually means
    effects: WorldEffects
    # actor id -> standing delta. Every choice pleases someone and costs someone else.
    actor_effects: dict = {}
    risk: int = 50        # 0-100, how likely this backfires
    outcome: Optional[ChoiceOutcome] = None
    next: str             # id of the node this leads to


class UniverseNode(BaseModel):
    """One beat of the branching story. An empty `choices` list marks an ending."""
    id: str
    year: str             # shown on the branching timeline
    title: str
    text: str             # 45-85 words. Short on purpose — the data carries the rest.
    facts: List[NodeFact] = []
    choices: List[UniverseChoice] = []

    # ── Endings only ──
    verdict: str = ""     # one line: better, worse, or unrecognisable
    epitaph: str = ""     # the last line the player is left with
    rarity: str = ""      # common | uncommon | rare — drives the badge and the collection
    stats: List[EndingStat] = []


class ParallelUniverse(BaseModel):
    """The whole decision tree for one event, in one language.

    Three choices at the opening, two after: 1 + 3 + 6 = 10 decision nodes and 12
    endings, 22 in all. The extra width sits where agency matters most — the first
    decision — without the 40 nodes that three-wide-throughout would cost.
    """
    pivot_year: str       # the moment history forks
    pivot_title: str
    premise: str          # what actually happened, in two sentences, before we change it
    root: str             # id of the opening node
    actors: List[Actor] = []
    nodes: List[UniverseNode] = []


class ParallelUniverseTranslations(BaseModel):
    en: Optional[ParallelUniverse] = None
    ro: Optional[ParallelUniverse] = None
    es: Optional[ParallelUniverse] = None
    de: Optional[ParallelUniverse] = None
    fr: Optional[ParallelUniverse] = None


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
    # Branching what-if game. Generated only for the day's two hero events, so most
    # events legitimately have none.
    parallel: Optional[ParallelUniverseTranslations] = None
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