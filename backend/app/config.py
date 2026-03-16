from pydantic_settings import BaseSettings
from pydantic import field_validator
import json

class Settings(BaseSettings):
    DATABASE_URL: str = "sqlite+aiosqlite:///./data/finance.db"
    CORS_ORIGINS: list[str] = ["http://localhost:5173", "http://localhost:3000"]
    DEBUG: bool = True

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v

    model_config = {"env_file": ".env", "extra": "ignore"}

settings = Settings()
