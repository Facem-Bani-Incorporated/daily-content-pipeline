from typing import Optional
from pydantic_settings import BaseSettings  # <--- Aceasta este linia salvatoare
from pydantic import ConfigDict, field_validator

class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env", env_file_encoding='utf-8', extra='ignore')

    # Core
    WIKI_BASE_URL: str = "https://en.wikipedia.org/api/rest_v1"
    USER_AGENT: str = "DailyHistoryApp/2.0 (contact@yourdomain.com)"
    AI_MODEL: str = "llama-3.3-70b-versatile"

    # API Keys
    GROQ_API_KEY: str
    CLOUDINARY_CLOUD_NAME: str
    CLOUDINARY_API_KEY: str
    CLOUDINARY_API_SECRET: str

    # Java Bridge
    JAVA_BACKEND_URL: str
    INTERNAL_API_SECRET: str

    # Database
    DATABASE_URL: Optional[str] = None
    MAX_CANDIDATES_FOR_AI: int = 15

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