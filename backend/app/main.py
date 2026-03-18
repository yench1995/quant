from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from .config import settings
from .database import init_db, close_db
from .data.fetcher import baostock_login, baostock_logout
from .strategies.registry import StrategyRegistry
from .api.v1.strategies import router as strategies_router
from .api.v1.backtests import router as backtests_router
from .api.v1.market_data import router as market_data_router
from .api.v1.results import router as results_router
from .api.v1.data_management import router as data_management_router
from .api.v1.seed_full import router as seed_full_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # MotherDuck requires the token in an env var before duckdb.connect() is called
    if settings.MOTHERDUCK_TOKEN:
        import os
        os.environ.setdefault("motherduck_token", settings.MOTHERDUCK_TOKEN)
    init_db(settings.DATABASE_PATH)
    baostock_login()          # 建立持久连接，整个进程生命周期只登录一次
    StrategyRegistry.discover()
    yield
    baostock_logout()         # 应用关闭时退出
    close_db()


app = FastAPI(
    title="A股量化回测平台",
    description="支持多策略插件的A股量化回测系统",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

API_PREFIX = "/api/v1"
app.include_router(strategies_router, prefix=API_PREFIX, tags=["strategies"])
app.include_router(backtests_router, prefix=API_PREFIX, tags=["backtests"])
app.include_router(market_data_router, prefix=API_PREFIX, tags=["market-data"])
app.include_router(results_router, prefix=API_PREFIX, tags=["results"])
app.include_router(data_management_router, prefix=API_PREFIX, tags=["data-management"])
app.include_router(seed_full_router, prefix=f"{API_PREFIX}/seed-full", tags=["seed-full"])


@app.get("/")
async def root():
    return {"message": "A股量化回测平台 API", "docs": "/docs"}
