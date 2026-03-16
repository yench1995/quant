from pydantic_settings import BaseSettings
from pydantic import field_validator
import json

class Settings(BaseSettings):
    DATABASE_PATH: str = "../data/finance.duckdb"
    CORS_ORIGINS: list[str] = ["http://localhost:5173", "http://localhost:3000"]
    DEBUG: bool = True
    # 价格数据源: baostock | akshare | tushare | auto (baostock→tushare→akshare)
    PRICE_SOURCE: str = "auto"
    # Tushare Pro token（PRICE_SOURCE=tushare 或 auto 时需要填写）
    TUSHARE_TOKEN: str = ""

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors(cls, v):
        if isinstance(v, str):
            return json.loads(v)
        return v

    model_config = {"env_file": ".env", "extra": "ignore"}

settings = Settings()
