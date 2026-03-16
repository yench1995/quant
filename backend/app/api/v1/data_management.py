import asyncio
from datetime import datetime, timedelta
from typing import Any
from fastapi import APIRouter, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import select, func, text

from ...database import AsyncSessionLocal
from ...models.lhb import LHBDaily
from ...models.price import StockPriceDaily
from ...data.fetcher import AkShareFetcher

router = APIRouter()
fetcher = AkShareFetcher()

# ── In-memory progress state (single-process) ──────────────────────────────
_seed_status: dict[str, Any] = {
    "running": False,
    "phase": "idle",
    "lhb": {"status": "idle", "months_done": 0, "total_months": 0},
    "price": {"status": "idle", "symbols_done": 0, "total_symbols": 0},
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
    """Split a date range into monthly chunks."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    months = []
    cur = start.replace(day=1)
    while cur <= end:
        month_start = cur
        # last day of month
        next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = next_month - timedelta(days=1)
        # clamp to requested range
        actual_start = max(month_start, start)
        actual_end = min(month_end, end)
        months.append((actual_start.strftime("%Y-%m-%d"), actual_end.strftime("%Y-%m-%d")))
        cur = next_month
    return months


async def _run_seed(start_date: str, end_date: str):
    global _seed_status
    _seed_status["running"] = True
    _seed_status["error"] = None

    try:
        # ── Phase 1: LHB data ──────────────────────────────────────────────
        _seed_status["phase"] = "lhb"
        _seed_status["lhb"]["status"] = "running"
        months = _month_ranges(start_date, end_date)
        _seed_status["lhb"]["total_months"] = len(months)
        _seed_status["lhb"]["months_done"] = 0

        loop = asyncio.get_event_loop()

        for i, (ms, me) in enumerate(months):
            df = await loop.run_in_executor(None, lambda s=ms, e=me: fetcher.get_lhb_raw(s, e))
            if df is not None and not df.empty:
                # normalize date column
                date_col = "date" if "date" in df.columns else df.columns[-1]
                records = []
                for _, row in df.iterrows():
                    records.append(
                        LHBDaily(
                            date=str(row.get(date_col, ""))[:10],
                            symbol=str(row.get("symbol", "")),
                            name=str(row.get("name", "")),
                            buy_amount=float(row.get("buy_amount", 0) or 0),
                            sell_amount=float(row.get("sell_amount", 0) or 0),
                            net_buy=float(row.get("net_buy", 0) or 0),
                            buy_inst_count=int(row.get("buy_inst_count", 0) or 0),
                            sell_inst_count=int(row.get("sell_inst_count", 0) or 0),
                        )
                    )
                if records:
                    async with AsyncSessionLocal() as session:
                        for rec in records:
                            await session.execute(
                                text(
                                    "INSERT OR IGNORE INTO lhb_daily "
                                    "(date, symbol, name, buy_amount, sell_amount, net_buy, "
                                    "buy_inst_count, sell_inst_count) "
                                    "VALUES (:date, :symbol, :name, :buy_amount, :sell_amount, "
                                    ":net_buy, :buy_inst_count, :sell_inst_count)"
                                ),
                                {
                                    "date": rec.date,
                                    "symbol": rec.symbol,
                                    "name": rec.name,
                                    "buy_amount": rec.buy_amount,
                                    "sell_amount": rec.sell_amount,
                                    "net_buy": rec.net_buy,
                                    "buy_inst_count": rec.buy_inst_count,
                                    "sell_inst_count": rec.sell_inst_count,
                                },
                            )
                        await session.commit()

            _seed_status["lhb"]["months_done"] = i + 1

        _seed_status["lhb"]["status"] = "done"

        # ── Phase 2: Stock price data ──────────────────────────────────────
        _seed_status["phase"] = "price"
        _seed_status["price"]["status"] = "running"

        # Collect unique symbols from lhb_daily
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(LHBDaily.symbol).distinct()
            )
            symbols = [row[0] for row in result.fetchall()]

        _seed_status["price"]["total_symbols"] = len(symbols)
        _seed_status["price"]["symbols_done"] = 0

        CONCURRENCY = 5
        sem = asyncio.Semaphore(CONCURRENCY)
        done_count = 0

        async def fetch_and_store(symbol: str):
            nonlocal done_count
            async with sem:
                df = await loop.run_in_executor(
                    None,
                    lambda s=symbol: fetcher.get_price_history(s, start_date, end_date),
                )
                if df is not None and not df.empty:
                    async with AsyncSessionLocal() as session:
                        for _, row in df.iterrows():
                            await session.execute(
                                text(
                                    "INSERT OR IGNORE INTO stock_price_daily "
                                    "(date, symbol, open, close, high, low, volume, change_pct) "
                                    "VALUES (:date, :symbol, :open, :close, :high, :low, :volume, :change_pct)"
                                ),
                                {
                                    "date": str(row.get("date", ""))[:10],
                                    "symbol": symbol,
                                    "open": float(row.get("open", 0) or 0),
                                    "close": float(row.get("close", 0) or 0),
                                    "high": float(row.get("high", 0) or 0),
                                    "low": float(row.get("low", 0) or 0),
                                    "volume": float(row.get("volume", 0) or 0),
                                    "change_pct": float(row.get("change_pct", 0) or 0),
                                },
                            )
                        await session.commit()
                done_count += 1
                _seed_status["price"]["symbols_done"] = done_count

        await asyncio.gather(*[fetch_and_store(s) for s in symbols])
        _seed_status["price"]["status"] = "done"
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

    # Reset status
    months = _month_ranges(start_date, end_date)
    _seed_status = {
        "running": True,
        "phase": "lhb",
        "lhb": {"status": "pending", "months_done": 0, "total_months": len(months)},
        "price": {"status": "pending", "symbols_done": 0, "total_symbols": 0},
        "error": None,
    }

    background_tasks.add_task(_run_seed, start_date, end_date)
    return {"message": "Seed started", "start_date": start_date, "end_date": end_date}


@router.get("/data/seed-status")
async def get_seed_status():
    return _seed_status


@router.get("/data/coverage")
async def get_coverage():
    async with AsyncSessionLocal() as session:
        # LHB stats
        lhb_count = await session.scalar(select(func.count()).select_from(LHBDaily))
        lhb_dates = await session.scalar(
            select(func.count(LHBDaily.date.distinct()))
        )
        lhb_earliest = await session.scalar(select(func.min(LHBDaily.date)))
        lhb_latest = await session.scalar(select(func.max(LHBDaily.date)))

        # Price stats
        price_count = await session.scalar(select(func.count()).select_from(StockPriceDaily))
        price_symbols = await session.scalar(
            select(func.count(StockPriceDaily.symbol.distinct()))
        )
        price_earliest = await session.scalar(select(func.min(StockPriceDaily.date)))
        price_latest = await session.scalar(select(func.max(StockPriceDaily.date)))

    return {
        "lhb": {
            "total_dates": lhb_dates or 0,
            "earliest": lhb_earliest or "",
            "latest": lhb_latest or "",
            "total_records": lhb_count or 0,
        },
        "price": {
            "total_symbols": price_symbols or 0,
            "total_records": price_count or 0,
            "earliest": price_earliest or "",
            "latest": price_latest or "",
        },
    }
