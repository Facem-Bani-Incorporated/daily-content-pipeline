from typing import Optional
from pydantic_settings import BaseSettings  # <--- Aceasta este linia salvatoare
from pydantic import ConfigDict, field_validator

# Default LLM provider + model. All providers below speak the OpenAI-compatible API,
# so the pipeline talks to one client (core/llm.py) and only these env vars change.
DEFAULT_PROVIDER = "gemini"
DEFAULT_MODEL = "gemini-3.5-flash"


class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')

    # Core
    WIKI_BASE_URL: str = "https://en.wikipedia.org/api/rest_v1"
    USER_AGENT: str = "DailyHistoryApp/2.0 (contact@yourdomain.com)"

    # ── LLM provider ────────────────────────────────────────────────
    # AI_PROVIDER selects which OpenAI-compatible backend core/llm.py talks to:
    # "gemini" (Google), "openai", or "groq". Pick the matching API key below.
    AI_PROVIDER: str = DEFAULT_PROVIDER
    AI_MODEL: str = DEFAULT_MODEL
    AI_BASE_URL: Optional[str] = None  # override the provider's default base URL if ever needed

    # Legacy "thinking budget" lever, kept for call-site compatibility. It no longer sets
    # a token budget; it only flags which calls are accuracy-critical (budget > 0 →
    # discovery/ranking) so those get reasoning while creative/mechanical calls don't.
    AI_THINKING_BUDGET: int = 2000
    # Reasoning effort for discovery/ranking (budget > 0): "low" | "medium" | "high".
    # These calls must reason about which events truly fall on a date, else the model
    # hallucinates and the Wikipedia validator rejects everything. Creative calls skip
    # reasoning entirely. Ignored by providers/models that don't support it (e.g. gpt-4o-mini).
    AI_REASONING_EFFORT: str = "medium"
    # Hard cap on max output tokens per request. Gemini's limits are generous so 8192 is
    # fine; lower it if you ever run on a tight tokens-per-minute tier (e.g. Groq free).
    AI_MAX_COMPLETION_TOKENS: int = 8192

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
        # A stale AI_MODEL env var from a previous provider (e.g. a leftover
        # "claude-haiku-4-5" or "openai/gpt-oss-120b" in Railway) would override the
        # code default and break every call. Coerce any known-foreign model — or an
        # empty value — back to the current default so env can't silently break it.
        if not v:
            return DEFAULT_MODEL
        low = str(v).lower()
        # Foreign providers' models, or Gemini models that Google retired for new users.
        dead = ("gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-2.0", "gemini-1.5")
        if (
            low.startswith("claude")
            or low.startswith("openai/gpt-oss")
            or "haiku" in low
            or low in dead
        ):
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