from typing import Optional
from pydantic_settings import BaseSettings  # <--- Aceasta este linia salvatoare
from pydantic import ConfigDict, field_validator

# Default LLM provider + model. All providers below speak the OpenAI-compatible API,
# so the pipeline talks to one client (core/llm.py) and only these env vars change.
DEFAULT_PROVIDER = "groq"
DEFAULT_MODEL = "openai/gpt-oss-120b"


class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')

    # Core
    WIKI_BASE_URL: str = "https://en.wikipedia.org/api/rest_v1"
    USER_AGENT: str = "DailyHistoryApp/2.0 (contact@yourdomain.com)"

    # ── LLM provider ────────────────────────────────────────────────
    # AI_PROVIDER selects which OpenAI-compatible backend core/llm.py talks to:
    # "gemini" (AI Studio key), "vertex" (Google Cloud, POSTPAID), "openai", or "groq".
    AI_PROVIDER: str = DEFAULT_PROVIDER
    AI_MODEL: str = DEFAULT_MODEL
    AI_BASE_URL: Optional[str] = None  # override the provider's default base URL if ever needed

    # Vertex AI (AI_PROVIDER="vertex") — billed via Google Cloud = pay-at-month-end.
    # Needs a GCP project with Vertex AI API enabled + billing linked, and a service
    # account JSON (paste the whole JSON string into GOOGLE_SERVICE_ACCOUNT_JSON).
    GCP_PROJECT: Optional[str] = None
    GCP_LOCATION: str = "global"
    GOOGLE_SERVICE_ACCOUNT_JSON: Optional[str] = None

    # Legacy "thinking budget" lever, kept for call-site compatibility. It no longer sets
    # a token budget; it only flags which calls are accuracy-critical (budget > 0 →
    # discovery/ranking) so those get reasoning while creative/mechanical calls don't.
    AI_THINKING_BUDGET: int = 2000
    # Reasoning effort for discovery/ranking (budget > 0): "low" | "medium" | "high".
    # These calls must reason about which events truly fall on a date, else the model
    # hallucinates and the Wikipedia validator rejects everything. Creative calls skip
    # reasoning entirely. Ignored by providers/models that don't support it (e.g. gpt-4o-mini).
    # Dropped from "medium" in Sept 2026, for cost. Reasoning tokens bill as OUTPUT
    # ($2.50/M vs $0.30/M for input), and this applies to the discovery and ranking
    # calls — the ones that run over every candidate event, not just the nine that
    # ship. That made it the widest cost surface in the pipeline.
    #
    # "low" still reasons; it just stops paying for long deliberation on a task the
    # Wikipedia validator checks afterwards anyway. Watch the validator's rejection
    # rate after this: if it climbs, the saving is false — rejected events cost a
    # full regeneration — and this goes back to "medium".
    AI_REASONING_EFFORT: str = "low"
    # Hard cap on max output tokens per request. On Gemini/Vertex reasoning tokens count
    # against this, so discovery (medium reasoning + a long list) needs generous room —
    # Vertex limits are high. Lower it only for a tight tokens-per-minute tier (Groq free).
    #
    # Raised from 16384 after the parallel-universe generator spent a week being silently
    # truncated at the old ceiling: a cut-off response is repaired into valid JSON, so it
    # looks like a short answer rather than a clipped one, and every tree was rejected as
    # too small. gemini-2.5-flash allows 65535 out; this leaves headroom without inviting
    # runaway generations. Callers still pass their own max_tokens — this only stops the
    # ones that legitimately need room from being clipped.
    AI_MAX_COMPLETION_TOKENS: int = 40960

    # API Keys — set the one matching AI_PROVIDER
    GEMINI_API_KEY: Optional[str] = None
    OPENAI_API_KEY: Optional[str] = None
    GROQ_API_KEY: Optional[str] = None
    ANTHROPIC_API_KEY: Optional[str] = None  # legacy — no longer used
    CLOUDINARY_CLOUD_NAME: str
    CLOUDINARY_API_KEY: str
    CLOUDINARY_API_SECRET: str

    # Java Bridge
    JAVA_BACKEND_URL: str
    INTERNAL_API_SECRET: str

    # Database
    DATABASE_URL: Optional[str] = None
    MAX_CANDIDATES_FOR_AI: int = 200

    @field_validator("AI_MODEL", mode="before")
    @classmethod
    def force_supported_model(cls, v: Optional[str]) -> str:
        # A stale AI_MODEL from a previous provider would be sent to the current one,
        # which rejects it on every call. Coerce the clearly-foreign names back to the
        # default so a leftover Railway variable cannot silently break a run.
        #
        # This list is the wrong way round if you switch providers and forget it: it
        # used to coerce "openai/gpt-oss*", which meant setting the Groq model in env
        # silently put you back on Gemini. Whatever the current default is, the names
        # coerced here must be the *other* providers'.
        if not v:
            return DEFAULT_MODEL
        low = str(v).lower()
        foreign = ("claude", "haiku", "gemini", "google/", "gpt-4", "gpt-3")
        if any(low.startswith(f) or f in low for f in foreign):
            return DEFAULT_MODEL
        return v

    @field_validator("JAVA_BACKEND_URL")
    @classmethod
    def clean_url(cls, v: str) -> str:
        # Eliminăm slash-ul de la final dacă există
        return v.rstrip('/')

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def fix_postgres_protocol(cls, v: Optional[str]) -> Optional[str]:
        if not v or "asyncpg" in v:
            return v
        # Railway dă postgres://, noi avem nevoie de driverul async
        return v.replace("postgres://", "postgresql+asyncpg://", 1).replace("postgresql://", "postgresql+asyncpg://", 1)

config = Settings()