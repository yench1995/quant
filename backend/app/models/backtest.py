from sqlalchemy import String, JSON, Float, Integer, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from ..database import Base

class BacktestRun(Base):
    __tablename__ = "backtest_runs"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, default="pending")  # pending/running/completed/failed
    parameters: Mapped[dict] = mapped_column(JSON, default=dict)
    start_date: Mapped[str] = mapped_column(String, nullable=False)
    end_date: Mapped[str] = mapped_column(String, nullable=False)
    initial_capital: Mapped[float] = mapped_column(Float, default=1_000_000.0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

class BacktestResult(Base):
    __tablename__ = "backtest_results"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    total_return: Mapped[float] = mapped_column(Float, default=0.0)
    annual_return: Mapped[float] = mapped_column(Float, default=0.0)
    sharpe_ratio: Mapped[float] = mapped_column(Float, default=0.0)
    max_drawdown: Mapped[float] = mapped_column(Float, default=0.0)
    win_rate: Mapped[float] = mapped_column(Float, default=0.0)
    total_trades: Mapped[int] = mapped_column(Integer, default=0)
    equity_curve: Mapped[list] = mapped_column(JSON, default=list)
    benchmark_curve: Mapped[list] = mapped_column(JSON, default=list)
    holding_analysis: Mapped[dict] = mapped_column(JSON, default=dict)

class Trade(Base):
    __tablename__ = "trades"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, default="")
    entry_date: Mapped[str] = mapped_column(String, nullable=False)
    exit_date: Mapped[str] = mapped_column(String, nullable=False)
    entry_price: Mapped[float] = mapped_column(Float, nullable=False)
    exit_price: Mapped[float] = mapped_column(Float, nullable=False)
    shares: Mapped[int] = mapped_column(Integer, nullable=False)
    gross_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    commission: Mapped[float] = mapped_column(Float, default=0.0)
    net_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    return_pct: Mapped[float] = mapped_column(Float, default=0.0)
    holding_days: Mapped[int] = mapped_column(Integer, default=0)
    signal_net_buy: Mapped[float] = mapped_column(Float, default=0.0)
