import asyncio
import json
import uuid
from datetime import datetime, timedelta
from typing import Any
import pandas as pd

from ..database import fetch_all, fetch_one, execute, executemany
from ..strategies.registry import StrategyRegistry
from ..strategies.base import StrategyContext
from ..data.fetcher import AkShareFetcher
from ..data.cache import CacheManager
from ..utils.trading_calendar import TradingCalendar
from ..utils.metrics import calculate_metrics
from .vectorized import calculate_trade, sweep_holding_periods


class BacktestEngine:
    def __init__(self):
        self.fetcher = AkShareFetcher()

    async def run(self, run_id: str):
        """Main backtest execution — runs in background."""
        run = await fetch_one("SELECT * FROM backtest_runs WHERE id = ?", [run_id])
        if not run:
            return

        # Parse JSON parameters
        if run.get("parameters"):
            run["parameters"] = json.loads(run["parameters"])

        await execute(
            "UPDATE backtest_runs SET status = ? WHERE id = ?",
            ["running", run_id],
        )

        try:
            await self._execute(run)
        except Exception as e:
            import traceback
            await execute(
                "UPDATE backtest_runs SET status = ?, error_message = ?, completed_at = ? WHERE id = ?",
                ["failed", f"{str(e)}\n{traceback.format_exc()}", datetime.utcnow(), run_id],
            )

    async def _execute(self, run: dict):
        params = run["parameters"]
        strategy_cls = StrategyRegistry.get(run["strategy_id"])
        if strategy_cls is None:
            raise ValueError(f"Strategy not found: {run['strategy_id']}")

        strategy = strategy_cls()
        holding_days = int(params.get("holding_days", 5))
        position_size_pct = float(params.get("position_size_pct", 0.1))
        run_sweep = bool(params.get("run_holding_sweep", False))
        initial_capital = run["initial_capital"]

        # Get trading calendar
        cache_mgr = CacheManager()
        cal_key = f"calendar_{run['start_date']}_{run['end_date']}"
        cached_cal = await cache_mgr.get(cal_key)
        if cached_cal:
            trading_days = cached_cal
        else:
            trading_days = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.fetcher.get_trading_calendar(run["start_date"], run["end_date"]),
            )
            if trading_days:
                await cache_mgr.set(cal_key, trading_days, ttl_hours=24 * 30)

        if not trading_days:
            raise ValueError("Could not fetch trading calendar")

        calendar = TradingCalendar(trading_days)

        # ── Step 1: LHB data ──────────────────────────────────────────────
        lhb_df_all = await self._get_lhb_data(run["start_date"], run["end_date"], trading_days)

        lhb_by_date: dict[str, pd.DataFrame] = {}
        if lhb_df_all is not None and not lhb_df_all.empty:
            date_col = "date" if "date" in lhb_df_all.columns else lhb_df_all.columns[-1]
            for d, grp in lhb_df_all.groupby(date_col):
                lhb_by_date[str(d)] = grp.reset_index(drop=True)

        # ── Step 2: Generate signals ──────────────────────────────────────
        all_signals = []

        class _PrefetchedFetcher:
            def get_lhb_data(self, date: str) -> pd.DataFrame | None:
                return lhb_by_date.get(date)

        prefetched = _PrefetchedFetcher()

        for trade_date in trading_days:
            ctx = StrategyContext(
                current_date=trade_date,
                trading_days=trading_days,
                parameters=params,
                fetcher=prefetched,
            )
            signals = strategy.generate_signals(ctx)
            for sig in signals:
                entry_date = calendar.offset(trade_date, 1)
                if entry_date is None:
                    continue
                exit_date = calendar.offset(entry_date, holding_days)
                if exit_date is None:
                    continue
                all_signals.append((trade_date, entry_date, exit_date, sig))

        # ── Step 3: Price data ────────────────────────────────────────────
        symbols = list({sig.symbol for _, _, _, sig in all_signals})
        price_cache = await self._get_price_data(symbols, run["start_date"], run["end_date"])

        # Execute trades
        trades_data = []
        current_capital = initial_capital
        equity_by_date: dict[str, float] = {run["start_date"]: initial_capital}

        for signal_date, entry_date, exit_date, sig in all_signals:
            price_df = price_cache.get(sig.symbol)
            if price_df is None:
                continue

            result = calculate_trade(
                symbol=sig.symbol,
                name=sig.name,
                entry_date=entry_date,
                exit_date=exit_date,
                price_df=price_df,
                capital=current_capital,
                position_size_pct=position_size_pct,
                signal_net_buy=sig.net_buy_wan,
            )
            if result is None:
                continue

            trade_dict = {
                "run_id":          run["id"],
                "symbol":          result.symbol,
                "name":            result.name,
                "entry_date":      result.entry_date,
                "exit_date":       result.exit_date,
                "entry_price":     result.entry_price,
                "exit_price":      result.exit_price,
                "shares":          result.shares,
                "gross_pnl":       result.gross_pnl,
                "commission":      result.commission,
                "net_pnl":         result.net_pnl,
                "return_pct":      result.return_pct,
                "holding_days":    result.holding_days,
                "signal_net_buy":  result.signal_net_buy,
            }
            trades_data.append(trade_dict)
            current_capital += result.net_pnl
            equity_by_date[result.exit_date] = current_capital

        # Build equity curve
        equity_curve_values = []
        running_equity = initial_capital
        equity_curve_dates = []
        for td in trading_days:
            if td in equity_by_date:
                running_equity = equity_by_date[td]
            equity_curve_values.append(running_equity)
            equity_curve_dates.append(td)

        equity_curve = [
            {"date": d, "value": round(v, 2)}
            for d, v in zip(equity_curve_dates, equity_curve_values)
        ]

        # Benchmark (CSI 300)
        benchmark_curve = []
        try:
            bm_df = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.fetcher.get_index_history("000300", run["start_date"], run["end_date"]),
            )
            if bm_df is not None and not bm_df.empty:
                bm_start = float(bm_df.iloc[0]["close"])
                benchmark_curve = [
                    {"date": row["date"], "value": round(initial_capital * float(row["close"]) / bm_start, 2)}
                    for _, row in bm_df.iterrows()
                    if "date" in row and "close" in row
                ]
        except Exception:
            pass

        # Holding sweep analysis
        holding_analysis: dict[str, Any] = {}
        if run_sweep:
            sweep_totals: dict[int, list[float]] = {}
            for signal_date, entry_date, _, sig in all_signals:
                price_df = price_cache.get(sig.symbol)
                if price_df is None:
                    continue
                sweep = sweep_holding_periods(
                    symbol=sig.symbol,
                    entry_date=entry_date,
                    price_df=price_df,
                    trading_days=trading_days,
                    capital=initial_capital,
                    position_size_pct=position_size_pct,
                )
                for n, ret in sweep.items():
                    sweep_totals.setdefault(n, []).append(ret)

            holding_analysis = {
                str(n): round(sum(rets) / len(rets), 3)
                for n, rets in sweep_totals.items()
                if rets
            }

        # Calculate metrics
        metrics = calculate_metrics(
            equity_curve=equity_curve_values,
            trading_days=trading_days,
            initial_capital=initial_capital,
            trades=trades_data,
        )

        # Persist results
        await execute(
            """
            INSERT INTO backtest_results
                (run_id, total_return, annual_return, sharpe_ratio, max_drawdown,
                 win_rate, total_trades, equity_curve, benchmark_curve, holding_analysis)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run["id"],
                metrics["total_return"],
                metrics["annual_return"],
                metrics["sharpe_ratio"],
                metrics["max_drawdown"],
                metrics["win_rate"],
                metrics["total_trades"],
                json.dumps(equity_curve, default=str),
                json.dumps(benchmark_curve, default=str),
                json.dumps(holding_analysis, default=str),
            ],
        )

        if trades_data:
            trade_rows = [
                [
                    t["run_id"], t["symbol"], t["name"],
                    t["entry_date"], t["exit_date"],
                    t["entry_price"], t["exit_price"], t["shares"],
                    t["gross_pnl"], t["commission"], t["net_pnl"],
                    t["return_pct"], t["holding_days"], t["signal_net_buy"],
                ]
                for t in trades_data
            ]
            await executemany(
                """
                INSERT INTO trades
                    (run_id, symbol, name, entry_date, exit_date,
                     entry_price, exit_price, shares,
                     gross_pnl, commission, net_pnl,
                     return_pct, holding_days, signal_net_buy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                trade_rows,
            )

        await execute(
            "UPDATE backtest_runs SET status = ?, completed_at = ? WHERE id = ?",
            ["completed", datetime.utcnow(), run["id"]],
        )

    async def _get_lhb_data(
        self, start_date: str, end_date: str, trading_days: list[str]
    ) -> pd.DataFrame | None:
        """Get LHB data from DB first; fetch network only for missing trading days."""
        rows = await fetch_all(
            "SELECT * FROM lhb_daily WHERE date >= ? AND date <= ?",
            [start_date, end_date],
        )

        db_dates = {r["date"] for r in rows}
        missing_dates = set(trading_days) - db_dates

        if missing_dates:
            sorted_missing = sorted(missing_dates)
            fetch_start = sorted_missing[0]
            fetch_end = sorted_missing[-1]
            print(f"[engine] LHB: fetching missing dates {fetch_start}..{fetch_end} from network")
            df = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.fetcher.get_lhb_raw(fetch_start, fetch_end),
            )
            if df is not None and not df.empty:
                date_col = "date" if "date" in df.columns else df.columns[-1]
                insert_rows = [
                    [
                        str(row.get(date_col, ""))[:10],
                        str(row.get("symbol", "")),
                        str(row.get("name", "")),
                        float(row.get("buy_amount", 0) or 0),
                        float(row.get("sell_amount", 0) or 0),
                        float(row.get("net_buy", 0) or 0),
                        int(row.get("buy_inst_count", 0) or 0),
                        int(row.get("sell_inst_count", 0) or 0),
                    ]
                    for _, row in df.iterrows()
                ]
                await executemany(
                    """
                    INSERT INTO lhb_daily
                        (date, symbol, name, buy_amount, sell_amount, net_buy,
                         buy_inst_count, sell_inst_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    insert_rows,
                )
                # Re-query after insert
                rows = await fetch_all(
                    "SELECT * FROM lhb_daily WHERE date >= ? AND date <= ?",
                    [start_date, end_date],
                )

        if not rows:
            return None

        return pd.DataFrame([
            {
                "date":            r["date"],
                "symbol":          r["symbol"],
                "name":            r["name"],
                "buy_amount":      r["buy_amount"],
                "sell_amount":     r["sell_amount"],
                "net_buy":         r["net_buy"],
                "buy_inst_count":  r["buy_inst_count"],
                "sell_inst_count": r["sell_inst_count"],
                "change_pct":      0.0,
            }
            for r in rows
        ])

    async def _get_price_data(
        self, symbols: list[str], start_date: str, end_date: str
    ) -> dict[str, pd.DataFrame | None]:
        """Get price data from DB first; fall back to network for missing symbols."""
        if not symbols:
            return {}

        end_ext = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=60)).strftime("%Y-%m-%d")

        # DuckDB parameterised IN clause: pass list directly
        placeholders = ", ".join(["?" for _ in symbols])
        rows = await fetch_all(
            f"SELECT * FROM stock_price_daily WHERE symbol IN ({placeholders}) AND date >= ? AND date <= ?",
            [*symbols, start_date, end_ext],
        )

        db_symbols = {r["symbol"] for r in rows}
        missing_symbols = [s for s in symbols if s not in db_symbols]

        if missing_symbols:
            print(f"[engine] Price: fetching {len(missing_symbols)} missing symbols from network")
            CONCURRENCY = 5
            loop = asyncio.get_event_loop()
            sem = asyncio.Semaphore(CONCURRENCY)

            async def fetch_one_symbol(symbol: str):
                async with sem:
                    df = await loop.run_in_executor(
                        None,
                        lambda s=symbol: self.fetcher.get_price_history(s, start_date, end_date),
                    )
                    if df is not None and not df.empty:
                        insert_rows = [
                            [
                                str(row.get("date", ""))[:10],
                                symbol,
                                float(row.get("open", 0) or 0),
                                float(row.get("close", 0) or 0),
                                float(row.get("high", 0) or 0),
                                float(row.get("low", 0) or 0),
                                float(row.get("volume", 0) or 0),
                                float(row.get("change_pct", 0) or 0),
                            ]
                            for _, row in df.iterrows()
                        ]
                        if insert_rows:
                            await executemany(
                                """
                                INSERT INTO stock_price_daily
                                    (date, symbol, open, close, high, low, volume, change_pct)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT DO NOTHING
                                """,
                                insert_rows,
                            )

            await asyncio.gather(*[fetch_one_symbol(s) for s in missing_symbols])

            # Re-query
            rows = await fetch_all(
                f"SELECT * FROM stock_price_daily WHERE symbol IN ({placeholders}) AND date >= ? AND date <= ?",
                [*symbols, start_date, end_ext],
            )

        price_cache: dict[str, pd.DataFrame | None] = {s: None for s in symbols}
        symbol_rows: dict[str, list] = {}
        for r in rows:
            symbol_rows.setdefault(r["symbol"], []).append({
                "date":       r["date"],
                "open":       r["open"],
                "close":      r["close"],
                "high":       r["high"],
                "low":        r["low"],
                "volume":     r["volume"],
                "change_pct": r["change_pct"],
            })
        for sym, rec_list in symbol_rows.items():
            price_cache[sym] = pd.DataFrame(rec_list).sort_values("date").reset_index(drop=True)

        return price_cache


engine = BacktestEngine()
