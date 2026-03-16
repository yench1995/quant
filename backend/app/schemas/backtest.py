from pydantic import BaseModel
from typing import Any
from datetime import datetime

class BacktestCreateRequest(BaseModel):
    strategy_id: str
    parameters: dict[str, Any] = {}
    start_date: str
    end_date: str
    initial_capital: float = 1_000_000.0

class BacktestRunSchema(BaseModel):
    id: str
    strategy_id: str
    status: str
    parameters: dict[str, Any]
    start_date: str
    end_date: str
    initial_capital: float
    error_message: str | None = None
    created_at: datetime
    completed_at: datetime | None = None

    model_config = {"from_attributes": True}

class BacktestResultSchema(BaseModel):
    run_id: str
    total_return: float
    annual_return: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    total_trades: int
    equity_curve: list[dict]
    benchmark_curve: list[dict]
    holding_analysis: dict

    model_config = {"from_attributes": True}

class BacktestDetailSchema(BaseModel):
    run: BacktestRunSchema
    result: BacktestResultSchema | None = None

class TradeSchema(BaseModel):
    id: int
    run_id: str
    symbol: str
    name: str
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    shares: int
    gross_pnl: float
    commission: float
    net_pnl: float
    return_pct: float
    holding_days: int
    signal_net_buy: float

    model_config = {"from_attributes": True}

class PaginatedTrades(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[TradeSchema]
