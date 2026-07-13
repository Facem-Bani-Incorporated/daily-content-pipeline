from typing import Optional
from pydantic_settings import BaseSettings  # <--- Aceasta este linia salvatoare
from pydantic import ConfigDict, field_validator

class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')

    # Core
    WIKI_BASE_URL: str = "https://en.wikipedia.org/api/rest_v1"
    USER_AGENT: str = "DailyHistoryApp/2.0 (contact@yourdomain.com)"
    AI_MODEL: str = "claude-haiku-4-5"
    # Extended-thinking budget in tokens. Thinking tokens bill as OUTPUT ($5/MTok on
    # Haiku 4.5), so this is the single biggest cost lever. Only discovery/ranking use
    # it (they benefit from reasoning about date accuracy + significance). The high-volume
    # creative/mechanical calls — narratives, quizzes, translations, titles, social —
    # pass thinking_budget=0 explicitly and run WITHOUT thinking. Set to 0 to disable
    # thinking everywhere.
    AI_THINKING_BUDGET: int = 2000

    # API Keys
    ANTHROPIC_API_KEY: str
    GROQ_API_KEY: Optional[str] = None  # legacy — kept during transition, no longer used
    CLOUDINARY_CLOUD_NAME: str
    CLOUDINARY_API_KEY: str
    CLOUDINARY_API_SECRET: str

    # Java Bridge
    JAVA_BACKEND_URL: str
    INTERNAL_API_SECRET: str

    # Database
    DATABASE_URL: Optional[str] = None
    MAX_CANDIDATES_FOR_AI: int = 200

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