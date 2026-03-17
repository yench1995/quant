import asyncio
import random
from datetime import datetime, timedelta
from typing import Any
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
import pandas as pd

from ...database import fetch_all, fetch_one, executemany
from ...data.fetcher import AkShareFetcher

router = APIRouter()
fetcher = AkShareFetcher()

# ── In-memory progress state (single-process) ──────────────────────────────
_seed_status: dict[str, Any] = {
    "running": False,
    "phase": "idle",
    "lhb": {"status": "idle", "months_done": 0, "total_months": 0},
    "price": {"status": "idle", "symbols_done": 0, "total_symbols": 0},
    "indicator": {"status": "idle", "symbols_done": 0, "total_symbols": 0},
    "error": None,
}


class SeedRequest(BaseModel):
    start_date: str = ""
    end_date: str = ""


def _default_dates() -> tuple[str, str]:
    today = datetime.today()
    three_years_ago = today - timedelta(days=3 * 365)
    return three_years_ago.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


def _month_ranges(start_date: str, end_date: str) -> list[tuple[str, str]]:
    """Split a date range into quarterly chunks to reduce API call count."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    chunks = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=90), end)
        chunks.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cur = chunk_end + timedelta(days=1)
    return chunks


async def _run_seed(start_date: str, end_date: str):
    global _seed_status
    _seed_status["running"] = True
    _seed_status["error"] = None

    try:
        loop = asyncio.get_event_loop()

        # ── Phase 1: LHB data ──────────────────────────────────────────────
        _seed_status["phase"] = "lhb"
        _seed_status["lhb"]["status"] = "running"
        months = _month_ranges(start_date, end_date)

        # 预查已有数据的季度块，跳过已存在的
        existing_lhb = await fetch_all(
            "SELECT DISTINCT date FROM lhb_daily WHERE date >= ? AND date <= ?",
            [start_date, end_date],
        )
        existing_lhb_dates = {r["date"] for r in existing_lhb}

        # 标记哪些块需要跳过（该季度已有任意记录视为已完成）
        chunks_needed = []
        for ms, me in months:
            has_data = any(ms <= d <= me for d in existing_lhb_dates)
            chunks_needed.append((ms, me, not has_data))

        need_count = sum(1 for _, _, needed in chunks_needed if needed)
        skip_count = len(months) - need_count
        print(f"[seed] LHB: {len(months)} 块，已有 {skip_count} 块跳过，需抓取 {need_count} 块")

        _seed_status["lhb"]["total_months"] = len(months)
        _seed_status["lhb"]["months_done"] = skip_count  # 已跳过的直接计入进度

        for i, (ms, me, needed) in enumerate(chunks_needed):
            if not needed:
                continue  # 已有数据，跳过

            try:
                df = await asyncio.wait_for(
                    loop.run_in_executor(None, lambda s=ms, e=me: fetcher.get_lhb_raw(s, e)),
                    timeout=35,
                )
            except asyncio.TimeoutError:
                print(f"[seed] LHB timeout for {ms}~{me}, skipping")
                _seed_status["lhb"]["months_done"] += 1
                await asyncio.sleep(2)
                continue

            if df is not None and not df.empty:
                date_col = "date" if "date" in df.columns else df.columns[-1]
                rows = [
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
                if rows:
                    await executemany(
                        """
                        INSERT INTO lhb_daily
                            (date, symbol, name, buy_amount, sell_amount, net_buy,
                             buy_inst_count, sell_inst_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT DO NOTHING
                        """,
                        rows,
                    )

            _seed_status["lhb"]["months_done"] += 1
            await asyncio.sleep(1.5 + random.uniform(0, 1))

        _seed_status["lhb"]["status"] = "done"

        # ── Phase 2: Stock price data ──────────────────────────────────────
        _seed_status["phase"] = "price"
        _seed_status["price"]["status"] = "running"

        # 所有 LHB 出现过的股票
        all_symbol_rows = await fetch_all("SELECT DISTINCT symbol FROM lhb_daily")
        all_symbols = [r["symbol"] for r in all_symbol_rows]

        # 查哪些 symbol 在请求的日期范围内已有价格数据（有数据则跳过）
        end_check = end_date  # 检查是否覆盖到 end_date 附近
        covered_rows = await fetch_all(
            "SELECT DISTINCT symbol FROM stock_price_daily WHERE date >= ? AND date <= ?",
            [start_date, end_check],
        )
        covered_symbols = {r["symbol"] for r in covered_rows}
        symbols_to_fetch = [s for s in all_symbols if s not in covered_symbols]

        print(
            f"[seed] Price: 共 {len(all_symbols)} 个股票，"
            f"已有 {len(covered_symbols)} 个跳过，需抓取 {len(symbols_to_fetch)} 个"
        )

        _seed_status["price"]["total_symbols"] = len(all_symbols)
        _seed_status["price"]["symbols_done"] = len(covered_symbols)  # 已跳过的计入进度

        CONCURRENCY = 3
        sem = asyncio.Semaphore(CONCURRENCY)

        async def fetch_and_store(symbol: str):
            async with sem:
                df = await loop.run_in_executor(
                    None,
                    lambda s=symbol: fetcher.get_price_history(s, start_date, end_date),
                )
                if df is not None and not df.empty:
                    rows = [
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
                    if rows:
                        await executemany(
                            """
                            INSERT INTO stock_price_daily
                                (date, symbol, open, close, high, low, volume, change_pct)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT DO NOTHING
                            """,
                            rows,
                        )
                _seed_status["price"]["symbols_done"] += 1

        await asyncio.gather(*[fetch_and_store(s) for s in symbols_to_fetch])
        _seed_status["price"]["status"] = "done"

        # ── Phase 3: Calculate and store technical indicators ──────────────
        _seed_status["phase"] = "indicator"
        _seed_status["indicator"]["status"] = "running"
        _seed_status["indicator"]["total_symbols"] = len(all_symbols)

        price_rows = await fetch_all(
            "SELECT symbol, date, close FROM stock_price_daily ORDER BY symbol, date"
        )

        # Group by symbol
        symbol_price_map: dict[str, list[dict]] = {}
        for r in price_rows:
            symbol_price_map.setdefault(r["symbol"], []).append(r)

        indicator_rows = []
        for sym, records in symbol_price_map.items():
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

            for i, row in df.iterrows():
                def _f(series, idx):
                    v = series.iloc[idx]
                    return None if pd.isna(v) else float(v)

                indicator_rows.append([
                    row["date"],
                    sym,
                    _f(ma5, i),
                    _f(ma10, i),
                    _f(ma20, i),
                    _f(ma60, i),
                    _f(macd_line, i),
                    _f(macd_signal, i),
                    _f(rsi14, i),
                ])

            _seed_status["indicator"]["symbols_done"] += 1

        if indicator_rows:
            BATCH = 500
            for start in range(0, len(indicator_rows), BATCH):
                await executemany(
                    """
                    INSERT INTO stock_indicator_daily
                        (date, symbol, ma5, ma10, ma20, ma60, macd, macd_signal, rsi14)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    indicator_rows[start : start + BATCH],
                )

        _seed_status["indicator"]["status"] = "done"
        _seed_status["phase"] = "done"

    except Exception as e:
        import traceback
        _seed_status["error"] = f"{str(e)}\n{traceback.format_exc()}"
        _seed_status["phase"] = "error"
    finally:
        _seed_status["running"] = False


@router.post("/data/seed-all")
async def seed_all(req: SeedRequest, background_tasks: BackgroundTasks):
    global _seed_status
    if _seed_status["running"]:
        return {"message": "Already running", "status": _seed_status}

    start_date = req.start_date or _default_dates()[0]
    end_date = req.end_date or _default_dates()[1]

    months = _month_ranges(start_date, end_date)
    _seed_status = {
        "running": True,
        "phase": "lhb",
        "lhb": {"status": "pending", "months_done": 0, "total_months": len(months)},
        "price": {"status": "pending", "symbols_done": 0, "total_symbols": 0},
        "indicator": {"status": "pending", "symbols_done": 0, "total_symbols": 0},
        "error": None,
    }

    background_tasks.add_task(_run_seed, start_date, end_date)
    return {"message": "Seed started", "start_date": start_date, "end_date": end_date}


@router.get("/data/seed-status")
async def get_seed_status():
    return _seed_status


@router.get("/data/coverage")
async def get_coverage():
    lhb = await fetch_one("""
        SELECT
            COUNT(*)          AS total_records,
            COUNT(DISTINCT date)   AS total_dates,
            MIN(date)         AS earliest,
            MAX(date)         AS latest
        FROM lhb_daily
    """)
    price = await fetch_one("""
        SELECT
            COUNT(*)            AS total_records,
            COUNT(DISTINCT symbol) AS total_symbols,
            MIN(date)           AS earliest,
            MAX(date)           AS latest
        FROM stock_price_daily
    """)
    return {
        "lhb": {
            "total_dates":   lhb["total_dates"]   or 0,
            "earliest":      lhb["earliest"]      or "",
            "latest":        lhb["latest"]        or "",
            "total_records": lhb["total_records"] or 0,
        },
        "price": {
            "total_symbols": price["total_symbols"] or 0,
            "total_records": price["total_records"] or 0,
            "earliest":      price["earliest"]      or "",
            "latest":        price["latest"]        or "",
        },
    }
