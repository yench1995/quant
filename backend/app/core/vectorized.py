import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Any

# Commission rates
BUY_COMMISSION = 0.0003   # 0.03%
SELL_COMMISSION = 0.0003  # 0.03%
STAMP_DUTY = 0.001        # 0.1% on sell only

@dataclass
class TradeResult:
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

def calculate_trade(
    symbol: str,
    name: str,
    entry_date: str,
    exit_date: str,
    price_df: pd.DataFrame,
    capital: float,
    position_size_pct: float,
    signal_net_buy: float = 0.0,
) -> TradeResult | None:
    """Calculate a single trade result."""
    if price_df is None or price_df.empty:
        return None

    date_col = "date"
    open_col = "open"
    close_col = "close"

    # Get entry (open price on entry_date)
    entry_rows = price_df[price_df[date_col] == entry_date]
    if entry_rows.empty:
        return None
    entry_price = float(entry_rows.iloc[0][open_col])

    # Get exit (close price on exit_date)
    exit_rows = price_df[price_df[date_col] == exit_date]
    if exit_rows.empty:
        # Use last available close if exit date not found
        available = price_df[price_df[date_col] <= exit_date]
        if available.empty:
            return None
        exit_price = float(available.iloc[-1][close_col])
        actual_exit_date = available.iloc[-1][date_col]
    else:
        exit_price = float(exit_rows.iloc[0][close_col])
        actual_exit_date = exit_date

    if entry_price <= 0:
        return None

    # Calculate position
    position_capital = capital * position_size_pct
    shares = int(position_capital / entry_price / 100) * 100  # round to lot
    if shares <= 0:
        shares = 100  # minimum 1 lot

    # Calculate PnL
    gross_pnl = (exit_price - entry_price) * shares
    buy_comm = entry_price * shares * BUY_COMMISSION
    sell_comm = exit_price * shares * (SELL_COMMISSION + STAMP_DUTY)
    commission = buy_comm + sell_comm
    net_pnl = gross_pnl - commission
    return_pct = (exit_price / entry_price - 1) * 100 - (
        (SELL_COMMISSION + STAMP_DUTY + BUY_COMMISSION) * 100
    )

    # Calculate holding days (calendar-based)
    try:
        from datetime import datetime
        d1 = datetime.strptime(entry_date, "%Y-%m-%d")
        d2 = datetime.strptime(actual_exit_date, "%Y-%m-%d")
        holding_days = (d2 - d1).days
    except Exception:
        holding_days = 0

    return TradeResult(
        symbol=symbol,
        name=name,
        entry_date=entry_date,
        exit_date=actual_exit_date,
        entry_price=entry_price,
        exit_price=exit_price,
        shares=shares,
        gross_pnl=gross_pnl,
        commission=commission,
        net_pnl=net_pnl,
        return_pct=return_pct,
        holding_days=holding_days,
        signal_net_buy=signal_net_buy,
    )


def sweep_holding_periods(
    symbol: str,
    entry_date: str,
    price_df: pd.DataFrame,
    trading_days: list[str],
    capital: float,
    position_size_pct: float,
    max_days: int = 30,
) -> dict[int, float]:
    """Calculate returns for holding periods 1 to max_days."""
    results = {}
    if price_df is None or price_df.empty:
        return results

    # Get entry price
    entry_rows = price_df[price_df["date"] == entry_date]
    if entry_rows.empty:
        return results
    entry_price = float(entry_rows.iloc[0]["open"])
    if entry_price <= 0:
        return results

    # Precompute price lookup
    price_map = dict(zip(price_df["date"], price_df["close"]))

    # Find entry index in trading calendar
    try:
        entry_idx = trading_days.index(entry_date)
    except ValueError:
        import bisect
        entry_idx = bisect.bisect_right(trading_days, entry_date)

    for n in range(1, max_days + 1):
        exit_idx = entry_idx + n
        if exit_idx >= len(trading_days):
            break
        exit_date = trading_days[exit_idx]
        exit_price = price_map.get(exit_date)
        if exit_price is None:
            # Find nearest available
            for d in trading_days[exit_idx:]:
                if d in price_map:
                    exit_price = price_map[d]
                    break
        if exit_price is None:
            continue

        ret = (float(exit_price) / entry_price - 1) * 100
        ret -= (BUY_COMMISSION + SELL_COMMISSION + STAMP_DUTY) * 100
        results[n] = round(ret, 3)

    return results
