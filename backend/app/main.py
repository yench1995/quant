from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from .config import settings
from .database import create_tables
from .strategies.registry import StrategyRegistry
from .api.v1.strategies import router as strategies_router
from .api.v1.backtests import router as backtests_router
from .api.v1.market_data import router as market_data_router
from .api.v1.results import router as results_router
from .api.v1.data_management import router as data_management_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create DB tables
    await create_tables()
    # Discover strategies
    StrategyRegistry.discover()
    yield


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


@app.get("/")
async def root():
    return {"message": "A股量化回测平台 API", "docs": "/docs"}
