import numpy as np
from typing import Any

def calculate_metrics(
    equity_curve: list[float],
    trading_days: list[str],
    initial_capital: float,
    trades: list[dict],
) -> dict[str, Any]:
    """Calculate backtest performance metrics."""
    if not equity_curve or len(equity_curve) < 2:
        return {
            "total_return": 0.0,
            "annual_return": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "win_rate": 0.0,
            "total_trades": len(trades),
        }

    arr = np.array(equity_curve, dtype=float)

    # Total return
    total_return = (arr[-1] / arr[0] - 1.0) * 100

    # Annual return (annualized)
    n_days = len(arr)
    years = n_days / 252.0
    annual_return = ((arr[-1] / arr[0]) ** (1.0 / max(years, 1/252)) - 1.0) * 100 if years > 0 else 0.0

    # Daily returns
    daily_returns = np.diff(arr) / arr[:-1]

    # Sharpe ratio (assume risk-free = 3% annual = 3/252 daily)
    rf_daily = 0.03 / 252
    excess = daily_returns - rf_daily
    sharpe = (excess.mean() / excess.std() * np.sqrt(252)) if excess.std() > 0 else 0.0

    # Max drawdown
    peak = np.maximum.accumulate(arr)
    drawdown = (arr - peak) / peak
    max_drawdown = float(drawdown.min()) * 100  # negative %

    # Win rate
    if trades:
        wins = sum(1 for t in trades if t.get("net_pnl", 0) > 0)
        win_rate = wins / len(trades) * 100
    else:
        win_rate = 0.0

    return {
        "total_return": round(float(total_return), 2),
        "annual_return": round(float(annual_return), 2),
        "sharpe_ratio": round(float(sharpe), 3),
        "max_drawdown": round(float(max_drawdown), 2),
        "win_rate": round(float(win_rate), 2),
        "total_trades": len(trades),
    }
