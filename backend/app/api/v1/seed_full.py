"""
seed_full.py — 全量A股历史数据采集 API

端点：
  POST /api/v1/seed-full/start              — 启动（默认 full_check 模式，断点续传）
  POST /api/v1/seed-full/start?mode=incremental&days=3  — 增量模式
  GET  /api/v1/seed-full/status             — 查看当前进度
  GET  /api/v1/seed-full/coverage           — 查看各表覆盖率（不触发采集）

Phase 编排（顺序执行）：
  0  stock_basic   — 股票基本信息
  1  lhb           — 龙虎榜（复用现有逻辑）
  2  price         — 日线价格（复用现有逻辑，单线程保守限速）
  3  indicators    — 技术指标（CPU计算，无需限速）
  4  suspend       — 停复牌记录（按交易日循环）
  5  st            — ST状态（当前快照+日期推导）
  6  northbound    — 北向资金汇总
  7  money_flow    — 主力资金流向（尽力获取，失败跳过）
  8  valuation     — PE/PB/市值日频
  9  index_const   — 指数成分股快照
"""

import asyncio
import random
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from fastapi import APIRouter, BackgroundTasks, Query

from ...database import execute, executemany, fetch_all, fetch_one
from ...data.fetcher import AkShareFetcher
from ...data.fetcher_extended import (
    fetch_index_constituents,
    fetch_money_flow,
    fetch_northbound_flow,
    fetch_st_stocks_current,
    fetch_stock_individual_info,
    fetch_stock_list,
    fetch_stock_suspend_by_date,
    fetch_valuation,
    rate_limiter,
)

router = APIRouter()
fetcher = AkShareFetcher()

SEED_START_DATE = "2018-01-01"
INDEX_CODES = ["000300", "000905", "000852", "000016", "000688"]

# ── 运行状态（内存，跨请求共享）────────────────────────────────────────────────

_status: dict[str, Any] = {
    "running": False,
    "mode": "idle",
    "current_phase": "idle",
    "phases": {},
    "error": None,
    "started_at": None,
    "updated_at": None,
}


# ── DB 辅助：seed_job 断点续传 ─────────────────────────────────────────────────

async def _job_get(phase: str) -> dict | None:
    return await fetch_one("SELECT * FROM seed_job WHERE phase = ?", [phase])


async def _job_upsert(phase: str, **kwargs) -> None:
    existing = await _job_get(phase)
    now = datetime.utcnow().isoformat()
    if existing is None:
        await execute(
            """
            INSERT INTO seed_job (phase, status, total_items, done_items, last_symbol, started_at, updated_at, error_msg)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                phase,
                kwargs.get("status", "pending"),
                kwargs.get("total_items", 0),
                kwargs.get("done_items", 0),
                kwargs.get("last_symbol", ""),
                kwargs.get("started_at", now),
                now,
                kwargs.get("error_msg", ""),
            ],
        )
    else:
        sets = ["updated_at = ?"]
        vals: list[Any] = [now]
        for k, v in kwargs.items():
            sets.append(f"{k} = ?")
            vals.append(v)
        vals.append(phase)
        await execute(
            f"UPDATE seed_job SET {', '.join(sets)} WHERE phase = ?",
            vals,
        )


async def _job_reset(phase: str) -> None:
    await _job_upsert(phase, status="running", done_items=0, last_symbol="",
                      started_at=datetime.utcnow().isoformat(), error_msg="")


# ── 交易日历（用于停牌、覆盖率计算）──────────────────────────────────────────

def _trading_days(start_date: str, end_date: str) -> list[str]:
    return fetcher.get_trading_calendar(start_date, end_date)


# ── 覆盖率检查 ──────────────────────────────────────────────────────────────────

async def _coverage_for_symbol_table(
    table: str, start_date: str, end_date: str
) -> dict:
    """
    返回 {overall_pct, complete_symbols, total_symbols, sample_gaps}
    """
    expected_days = len(_trading_days(start_date, end_date))
    if expected_days == 0:
        return {"overall_pct": 100.0, "complete_symbols": 0, "total_symbols": 0}

    rows = await fetch_all(
        f"""
        SELECT symbol, COUNT(DISTINCT date) AS actual
        FROM {table}
        WHERE date >= ? AND date <= ?
        GROUP BY symbol
        """,
        [start_date, end_date],
    )
    if not rows:
        return {"overall_pct": 0.0, "complete_symbols": 0, "total_symbols": 0}

    threshold = expected_days * 0.95
    complete = sum(1 for r in rows if r["actual"] >= threshold)
    return {
        "overall_pct": round(complete / len(rows) * 100, 1),
        "complete_symbols": complete,
        "total_symbols": len(rows),
        "expected_days": expected_days,
    }


async def _get_gap_symbols(
    table: str, all_symbols: list[str], start_date: str, end_date: str
) -> list[str]:
    """
    返回实际数据天数 < 期望天数 * 95% 的 symbol 列表。
    """
    expected_days = len(_trading_days(start_date, end_date))
    if expected_days == 0:
        return []
    threshold = int(expected_days * 0.95)

    rows = await fetch_all(
        f"""
        SELECT symbol, COUNT(DISTINCT date) AS actual
        FROM {table}
        WHERE date >= ? AND date <= ?
        GROUP BY symbol
        """,
        [start_date, end_date],
    )
    covered = {r["symbol"] for r in rows if r["actual"] >= threshold}
    return [s for s in all_symbols if s not in covered]


# ── Phase 0: 股票基本信息 ───────────────────────────────────────────────────────

async def _phase_stock_basic(mode: str, days: int) -> None:
    phase = "stock_basic"
    job = await _job_get(phase)
    last_symbol = (job or {}).get("last_symbol", "") or ""

    stock_list = await fetch_stock_list()
    if not stock_list:
        await _job_upsert(phase, status="done", error_msg="empty stock list")
        return

    symbols = [s["symbol"] for s in stock_list]
    names = {s["symbol"]: s["name"] for s in stock_list}

    # 先批量写入 name（ON CONFLICT DO NOTHING）
    name_rows = [[sym, names.get(sym, ""), "", "", "", True] for sym in symbols]
    await executemany(
        """
        INSERT INTO stock_basic (symbol, name, exchange, industry, listing_date, is_active)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """,
        name_rows,
    )

    await _job_upsert(phase, status="running", total_items=len(symbols),
                      done_items=0, last_symbol=last_symbol)
    _status["phases"][phase] = {"status": "running", "done": 0, "total": len(symbols)}

    # 断点续传：跳过 last_symbol 之前的
    resume_idx = 0
    if last_symbol:
        try:
            resume_idx = symbols.index(last_symbol) + 1
        except ValueError:
            pass

    for i, symbol in enumerate(symbols[resume_idx:], start=resume_idx):
        info = await fetch_stock_individual_info(symbol)
        if info:
            await execute(
                """
                UPDATE stock_basic
                SET exchange = ?, industry = ?, listing_date = ?, updated_at = now()
                WHERE symbol = ?
                """,
                [info.get("exchange", ""), info.get("industry", ""),
                 info.get("listing_date", ""), symbol],
            )
        done = i + 1
        _status["phases"][phase]["done"] = done
        await _job_upsert(phase, done_items=done, last_symbol=symbol)

    await _job_upsert(phase, status="done", last_symbol="")
    _status["phases"][phase]["status"] = "done"


# ── Phase 1: LHB（复用现有逻辑）────────────────────────────────────────────────

async def _phase_lhb(start_date: str, end_date: str) -> None:
    phase = "lhb"
    _status["phases"][phase] = {"status": "running"}

    # 复用 data_management 的逻辑（季度块，跳过已有）
    from datetime import datetime as _dt
    cur = _dt.strptime(start_date, "%Y-%m-%d")
    end = _dt.strptime(end_date, "%Y-%m-%d")
    chunks = []
    while cur <= end:
        chunk_end = min(cur + timedelta(days=90), end)
        chunks.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cur = chunk_end + timedelta(days=1)

    existing = await fetch_all(
        "SELECT DISTINCT date FROM lhb_daily WHERE date >= ? AND date <= ?",
        [start_date, end_date],
    )
    existing_dates = {r["date"] for r in existing}

    loop = asyncio.get_event_loop()
    done = 0
    for ms, me in chunks:
        has_data = any(ms <= d <= me for d in existing_dates)
        if has_data:
            done += 1
            continue
        await rate_limiter.wait()
        try:
            df = await asyncio.wait_for(
                loop.run_in_executor(None, lambda s=ms, e=me: fetcher.get_lhb_raw(s, e)),
                timeout=35,
            )
            rate_limiter.on_success()
        except (asyncio.TimeoutError, Exception) as exc:
            rate_limiter.on_error()
            print(f"[seed_full] LHB {ms}~{me} error: {exc}")
            done += 1
            continue

        if df is not None and not df.empty:
            date_col = "date" if "date" in df.columns else df.columns[-1]
            rows = [
                [str(row.get(date_col, ""))[:10], str(row.get("symbol", "")),
                 str(row.get("name", "")), float(row.get("buy_amount", 0) or 0),
                 float(row.get("sell_amount", 0) or 0), float(row.get("net_buy", 0) or 0),
                 int(row.get("buy_inst_count", 0) or 0), int(row.get("sell_inst_count", 0) or 0)]
                for _, row in df.iterrows()
            ]
            if rows:
                await executemany(
                    "INSERT INTO lhb_daily (date,symbol,name,buy_amount,sell_amount,net_buy,buy_inst_count,sell_inst_count) "
                    "VALUES (?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    rows,
                )
        done += 1

    _status["phases"][phase] = {"status": "done"}


# ── Phase 2: 日线价格（单线程，保守限速）───────────────────────────────────────

async def _phase_price(start_date: str, end_date: str, all_symbols: list[str]) -> None:
    phase = "price"
    job = await _job_get(phase)
    last_symbol = (job or {}).get("last_symbol", "") or ""

    gap_symbols = await _get_gap_symbols("stock_price_daily", all_symbols, start_date, end_date)
    await _job_upsert(phase, status="running", total_items=len(gap_symbols),
                      done_items=0, last_symbol=last_symbol)
    _status["phases"][phase] = {"status": "running", "done": 0, "total": len(gap_symbols)}

    resume_idx = 0
    if last_symbol and last_symbol in gap_symbols:
        resume_idx = gap_symbols.index(last_symbol) + 1

    loop = asyncio.get_event_loop()
    for i, symbol in enumerate(gap_symbols[resume_idx:], start=resume_idx):
        await rate_limiter.wait()
        try:
            df = await loop.run_in_executor(
                None, lambda s=symbol: fetcher.get_price_history(s, start_date, end_date)
            )
            rate_limiter.on_success()
        except Exception as e:
            rate_limiter.on_error()
            print(f"[seed_full] price {symbol} error: {e}")
            continue

        if df is not None and not df.empty:
            rows = [
                [str(row.get("date", ""))[:10], symbol,
                 float(row.get("open", 0) or 0), float(row.get("close", 0) or 0),
                 float(row.get("high", 0) or 0), float(row.get("low", 0) or 0),
                 float(row.get("volume", 0) or 0), float(row.get("change_pct", 0) or 0)]
                for _, row in df.iterrows()
            ]
            if rows:
                await executemany(
                    "INSERT INTO stock_price_daily (date,symbol,open,close,high,low,volume,change_pct) "
                    "VALUES (?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    rows,
                )
        done = i + 1
        _status["phases"][phase]["done"] = done
        await _job_upsert(phase, done_items=done, last_symbol=symbol)

    await _job_upsert(phase, status="done", last_symbol="")
    _status["phases"][phase]["status"] = "done"


# ── Phase 3: 技术指标（CPU计算）────────────────────────────────────────────────

async def _phase_indicators(start_date: str, end_date: str) -> None:
    phase = "indicators"
    _status["phases"][phase] = {"status": "running"}

    price_rows = await fetch_all(
        "SELECT symbol, date, close FROM stock_price_daily "
        "WHERE date >= ? AND date <= ? ORDER BY symbol, date",
        [start_date, end_date],
    )
    symbol_map: dict[str, list] = {}
    for r in price_rows:
        symbol_map.setdefault(r["symbol"], []).append(r)

    indicator_rows = []
    for sym, records in symbol_map.items():
        df = pd.DataFrame(records).sort_values("date").reset_index(drop=True)
        closes = df["close"].astype(float)
        ma5  = closes.rolling(5).mean()
        ma10 = closes.rolling(10).mean()
        ma20 = closes.rolling(20).mean()
        ma60 = closes.rolling(60).mean()
        ema12 = closes.ewm(span=12, adjust=False).mean()
        ema26 = closes.ewm(span=26, adjust=False).mean()
        macd_line   = ema12 - ema26
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        delta = closes.diff()
        gain = delta.clip(lower=0)
        loss = (-delta).clip(lower=0)
        avg_gain = gain.ewm(com=13, adjust=False).mean()
        avg_loss = loss.ewm(com=13, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, float("nan"))
        rsi14 = 100 - (100 / (1 + rs))

        def _f(series, idx):
            v = series.iloc[idx]
            return None if pd.isna(v) else float(v)

        for i, row in df.iterrows():
            indicator_rows.append([
                row["date"], sym,
                _f(ma5, i), _f(ma10, i), _f(ma20, i), _f(ma60, i),
                _f(macd_line, i), _f(macd_signal, i), _f(rsi14, i),
            ])

    BATCH = 500
    for start in range(0, len(indicator_rows), BATCH):
        await executemany(
            "INSERT INTO stock_indicator_daily (date,symbol,ma5,ma10,ma20,ma60,macd,macd_signal,rsi14) "
            "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
            indicator_rows[start: start + BATCH],
        )

    _status["phases"][phase] = {"status": "done"}


# ── Phase 4: 停复牌记录 ─────────────────────────────────────────────────────────

async def _phase_suspend(start_date: str, end_date: str) -> None:
    phase = "suspend"
    job = await _job_get(phase)
    last_symbol = (job or {}).get("last_symbol", "") or ""  # 这里 last_symbol 存日期

    trading_days = _trading_days(start_date, end_date)
    await _job_upsert(phase, status="running", total_items=len(trading_days), done_items=0)
    _status["phases"][phase] = {"status": "running", "done": 0, "total": len(trading_days)}

    resume_idx = 0
    if last_symbol and last_symbol in trading_days:
        resume_idx = trading_days.index(last_symbol) + 1

    for i, date_str in enumerate(trading_days[resume_idx:], start=resume_idx):
        records = await fetch_stock_suspend_by_date(date_str)
        if records:
            rows = [[r["symbol"], r["suspend_date"], r.get("resume_date", ""), r.get("reason", "")]
                    for r in records]
            await executemany(
                "INSERT INTO stock_suspend (symbol, suspend_date, resume_date, reason) "
                "VALUES (?,?,?,?) ON CONFLICT DO NOTHING",
                rows,
            )
        done = i + 1
        _status["phases"][phase]["done"] = done
        await _job_upsert(phase, done_items=done, last_symbol=date_str)

    await _job_upsert(phase, status="done", last_symbol="")
    _status["phases"][phase]["status"] = "done"


# ── Phase 5: ST 状态 ────────────────────────────────────────────────────────────

async def _phase_st(end_date: str) -> None:
    phase = "st"
    _status["phases"][phase] = {"status": "running"}

    st_stocks = await fetch_st_stocks_current()
    if st_stocks:
        # 将当日快照写入 st_daily（只记录今天）
        today = end_date
        rows = [[today, s["symbol"], s["st_type"]] for s in st_stocks]
        await executemany(
            "INSERT INTO stock_st_daily (date, symbol, st_type) VALUES (?,?,?) ON CONFLICT DO NOTHING",
            rows,
        )

    _status["phases"][phase] = {"status": "done"}


# ── Phase 6: 北向资金 ───────────────────────────────────────────────────────────

async def _phase_northbound(start_date: str, end_date: str) -> None:
    phase = "northbound"
    _status["phases"][phase] = {"status": "running"}

    records = await fetch_northbound_flow(start_date, end_date)
    if records:
        rows = [[r["date"], r["market"], r["net_buy"], r["buy_amount"], r["sell_amount"]]
                for r in records]
        await executemany(
            "INSERT INTO northbound_flow_daily (date, market, net_buy, buy_amount, sell_amount) "
            "VALUES (?,?,?,?,?) ON CONFLICT DO NOTHING",
            rows,
        )

    _status["phases"][phase] = {"status": "done"}


# ── Phase 7: 主力资金流向 ────────────────────────────────────────────────────────

async def _phase_money_flow(start_date: str, all_symbols: list[str]) -> None:
    phase = "money_flow"
    job = await _job_get(phase)
    last_symbol = (job or {}).get("last_symbol", "") or ""

    # 只取 2022 年以后的（AkShare 覆盖约 2-3 年）
    mf_start = max(start_date, "2022-01-01")

    await _job_upsert(phase, status="running", total_items=len(all_symbols),
                      done_items=0, last_symbol=last_symbol)
    _status["phases"][phase] = {"status": "running", "done": 0, "total": len(all_symbols)}

    resume_idx = 0
    if last_symbol and last_symbol in all_symbols:
        resume_idx = all_symbols.index(last_symbol) + 1

    for i, symbol in enumerate(all_symbols[resume_idx:], start=resume_idx):
        try:
            records = await fetch_money_flow(symbol)
        except Exception as e:
            print(f"[seed_full] money_flow {symbol} skipped: {e}")
            records = []

        if records:
            rows = [
                [r["date"], r["symbol"], r.get("main_net_inflow", 0),
                 r.get("main_net_inflow_pct", 0), r.get("super_large_net", 0),
                 r.get("large_net", 0), r.get("medium_net", 0), r.get("small_net", 0)]
                for r in records if r["date"] >= mf_start
            ]
            if rows:
                await executemany(
                    "INSERT INTO stock_money_flow_daily "
                    "(date,symbol,main_net_inflow,main_net_inflow_pct,super_large_net,large_net,medium_net,small_net) "
                    "VALUES (?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    rows,
                )
        done = i + 1
        _status["phases"][phase]["done"] = done
        await _job_upsert(phase, done_items=done, last_symbol=symbol)

    await _job_upsert(phase, status="done", last_symbol="")
    _status["phases"][phase]["status"] = "done"


# ── Phase 8: 估值（PE/PB/市值）──────────────────────────────────────────────────

async def _phase_valuation(start_date: str, end_date: str, all_symbols: list[str]) -> None:
    phase = "valuation"
    job = await _job_get(phase)
    last_symbol = (job or {}).get("last_symbol", "") or ""

    gap_symbols = await _get_gap_symbols("stock_valuation_daily", all_symbols, start_date, end_date)
    await _job_upsert(phase, status="running", total_items=len(gap_symbols),
                      done_items=0, last_symbol=last_symbol)
    _status["phases"][phase] = {"status": "running", "done": 0, "total": len(gap_symbols)}

    resume_idx = 0
    if last_symbol and last_symbol in gap_symbols:
        resume_idx = gap_symbols.index(last_symbol) + 1

    for i, symbol in enumerate(gap_symbols[resume_idx:], start=resume_idx):
        records = await fetch_valuation(symbol)
        if records:
            rows = [
                [r["date"], r["symbol"], r.get("pe_ttm"), r.get("pb"),
                 r.get("ps_ttm"), r.get("total_mv"), r.get("circ_mv")]
                for r in records if start_date <= r["date"] <= end_date
            ]
            if rows:
                await executemany(
                    "INSERT INTO stock_valuation_daily (date,symbol,pe_ttm,pb,ps_ttm,total_mv,circ_mv) "
                    "VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING",
                    rows,
                )
        done = i + 1
        _status["phases"][phase]["done"] = done
        await _job_upsert(phase, done_items=done, last_symbol=symbol)

    await _job_upsert(phase, status="done", last_symbol="")
    _status["phases"][phase]["status"] = "done"


# ── Phase 9: 指数成分股 ─────────────────────────────────────────────────────────

async def _phase_index_const() -> None:
    phase = "index_const"
    _status["phases"][phase] = {"status": "running"}

    for index_code in INDEX_CODES:
        records = await fetch_index_constituents(index_code)
        if records:
            rows = [[r["index_code"], r["symbol"], r["in_date"], r.get("out_date")]
                    for r in records]
            await executemany(
                "INSERT INTO index_constituent (index_code, symbol, in_date, out_date) "
                "VALUES (?,?,?,?) ON CONFLICT DO NOTHING",
                rows,
            )

    _status["phases"][phase] = {"status": "done"}


# ── 增量模式辅助 ────────────────────────────────────────────────────────────────

def _incremental_dates(days: int) -> tuple[str, str]:
    today = datetime.today()
    start = today - timedelta(days=days)
    return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


# ── 主任务协程 ──────────────────────────────────────────────────────────────────

async def _run_full_seed(mode: str, days: int) -> None:
    global _status
    _status["running"] = True
    _status["error"] = None
    _status["started_at"] = datetime.utcnow().isoformat()
    rate_limiter.reset()

    try:
        today = datetime.today().strftime("%Y-%m-%d")
        if mode == "incremental":
            start_date, end_date = _incremental_dates(days)
        else:
            start_date, end_date = SEED_START_DATE, today

        # ── 获取全量股票列表（后续各 phase 共用）──────────────────────────────
        all_symbol_rows = await fetch_all("SELECT symbol FROM stock_basic")
        if not all_symbol_rows:
            # stock_basic 为空，先从网络拉一次列表（不拉详情）
            stock_list = await fetch_stock_list()
            all_symbols = [s["symbol"] for s in stock_list]
        else:
            all_symbols = [r["symbol"] for r in all_symbol_rows]

        # ── Phase 0: stock_basic ───────────────────────────────────────────
        _status["current_phase"] = "stock_basic"
        if mode != "incremental":
            await _phase_stock_basic(mode, days)
            # 刷新列表
            rows = await fetch_all("SELECT symbol FROM stock_basic")
            if rows:
                all_symbols = [r["symbol"] for r in rows]

        # ── Phase 1: LHB ──────────────────────────────────────────────────
        _status["current_phase"] = "lhb"
        await _phase_lhb(start_date, end_date)

        # ── Phase 2: price ────────────────────────────────────────────────
        _status["current_phase"] = "price"
        await _phase_price(start_date, end_date, all_symbols)

        # ── Phase 3: indicators ───────────────────────────────────────────
        _status["current_phase"] = "indicators"
        await _phase_indicators(start_date, end_date)

        # ── Phase 4: suspend ──────────────────────────────────────────────
        _status["current_phase"] = "suspend"
        await _phase_suspend(start_date, end_date)

        # ── Phase 5: st ───────────────────────────────────────────────────
        _status["current_phase"] = "st"
        await _phase_st(end_date)

        # ── Phase 6: northbound ───────────────────────────────────────────
        _status["current_phase"] = "northbound"
        await _phase_northbound(start_date, end_date)

        # ── Phase 7: money_flow ───────────────────────────────────────────
        _status["current_phase"] = "money_flow"
        await _phase_money_flow(start_date, all_symbols)

        # ── Phase 8: valuation ────────────────────────────────────────────
        _status["current_phase"] = "valuation"
        await _phase_valuation(start_date, end_date, all_symbols)

        # ── Phase 9: index_const ──────────────────────────────────────────
        _status["current_phase"] = "index_const"
        if mode != "incremental":
            await _phase_index_const()

        _status["current_phase"] = "done"

    except Exception as e:
        import traceback
        _status["error"] = f"{str(e)}\n{traceback.format_exc()}"
        _status["current_phase"] = "error"
    finally:
        _status["running"] = False
        _status["updated_at"] = datetime.utcnow().isoformat()


# ── API 端点 ────────────────────────────────────────────────────────────────────

@router.post("/start")
async def start_seed(
    background_tasks: BackgroundTasks,
    mode: str = Query(default="full_check", description="full_check | incremental"),
    days: int = Query(default=3, description="增量模式回溯天数"),
):
    global _status
    if _status["running"]:
        return {"message": "Already running", "status": _status}

    _status = {
        "running": True,
        "mode": mode,
        "current_phase": "starting",
        "phases": {},
        "error": None,
        "started_at": datetime.utcnow().isoformat(),
        "updated_at": None,
    }
    background_tasks.add_task(_run_full_seed, mode, days)
    return {"message": "Seed started", "mode": mode, "days": days if mode == "incremental" else None}


@router.get("/status")
async def get_status():
    # 附带各 phase 的 DB 持久化状态
    jobs = await fetch_all("SELECT * FROM seed_job ORDER BY phase")
    return {**_status, "db_jobs": jobs}


@router.get("/coverage")
async def get_coverage():
    today = datetime.today().strftime("%Y-%m-%d")
    start = SEED_START_DATE

    tables = {
        "stock_price_daily": await _coverage_for_symbol_table("stock_price_daily", start, today),
        "stock_indicator_daily": await _coverage_for_symbol_table("stock_indicator_daily", start, today),
        "stock_valuation_daily": await _coverage_for_symbol_table("stock_valuation_daily", start, today),
        "stock_money_flow_daily": await _coverage_for_symbol_table("stock_money_flow_daily", "2022-01-01", today),
    }

    # 单值统计的表
    basic = await fetch_one("SELECT COUNT(*) AS cnt FROM stock_basic")
    suspend = await fetch_one("SELECT COUNT(*) AS cnt FROM stock_suspend")
    st_daily = await fetch_one("SELECT COUNT(*) AS cnt FROM stock_st_daily")
    northbound = await fetch_one("SELECT COUNT(*) AS cnt, MIN(date) AS earliest, MAX(date) AS latest FROM northbound_flow_daily")
    index_const = await fetch_one("SELECT COUNT(*) AS cnt FROM index_constituent")

    return {
        "symbol_tables": tables,
        "stock_basic": {"total": (basic or {}).get("cnt", 0)},
        "stock_suspend": {"total": (suspend or {}).get("cnt", 0)},
        "stock_st_daily": {"total": (st_daily or {}).get("cnt", 0)},
        "northbound_flow_daily": northbound or {},
        "index_constituent": {"total": (index_const or {}).get("cnt", 0)},
    }
