from typing import Optional
from pydantic_settings import BaseSettings  # <--- Aceasta este linia salvatoare
from pydantic import ConfigDict, field_validator

GROQ_DEFAULT_MODEL = "openai/gpt-oss-120b"


class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')

    # Core
    WIKI_BASE_URL: str = "https://en.wikipedia.org/api/rest_v1"
    USER_AGENT: str = "DailyHistoryApp/2.0 (contact@yourdomain.com)"
    AI_MODEL: str = GROQ_DEFAULT_MODEL
    # Legacy "thinking budget" lever, kept for call-site compatibility. Groq has no
    # extended-thinking billing; for reasoning models (e.g. gpt-oss) this maps onto
    # Groq's `reasoning_effort` (budget > 0 -> "medium", 0 -> "low"). Only discovery/
    # ranking pass a budget (they benefit from reasoning about date accuracy +
    # significance); creative/mechanical calls pass 0 and run at "low" effort.
    AI_THINKING_BUDGET: int = 2000
    # Reasoning effort applied ONLY to discovery/ranking (the accuracy-critical calls,
    # budget > 0): "low" | "medium" | "high". At "low" gpt-oss hallucinates events not
    # on the target date; "high" needs too many tokens to fit the free-tier 8000 TPM
    # cap (single request 413s). "medium" is the sweet spot that fits and still reasons.
    # After upgrading Groq tier, bump to "high" and raise AI_MAX_COMPLETION_TOKENS.
    AI_REASONING_EFFORT: str = "medium"
    # Hard cap on max_completion_tokens PER REQUEST. Groq counts this fully against the
    # tokens-per-minute limit up front, so on the free tier (8000 TPM) a single request
    # must stay well under it or it 413s. Raise this once you're on a paid tier.
    AI_MAX_COMPLETION_TOKENS: int = 6000

    # API Keys
    GROQ_API_KEY: str
    ANTHROPIC_API_KEY: Optional[str] = None  # legacy — kept during transition, no longer used
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
    def force_groq_model(cls, v: Optional[str]) -> str:
        # We migrated off Anthropic. A stale AI_MODEL env var (e.g. a leftover
        # "claude-haiku-4-5" in Railway) would otherwise override the code default
        # and make every call 404 on Groq. Coerce any Anthropic-style model — or an
        # empty value — back to the Groq default so the pipeline can't be broken by env.
        if not v or str(v).lower().startswith("claude"):
            return GROQ_DEFAULT_MODEL
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