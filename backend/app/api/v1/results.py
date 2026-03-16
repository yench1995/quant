import json
from fastapi import APIRouter, HTTPException, Query

from ...database import fetch_all, fetch_one
from ...schemas.backtest import PaginatedTrades, TradeSchema

router = APIRouter()


@router.get("/results/{run_id}/trades", response_model=PaginatedTrades)
async def get_trades(
    run_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    offset = (page - 1) * page_size
    count_row = await fetch_one(
        "SELECT COUNT(*) AS cnt FROM trades WHERE run_id = ?", [run_id]
    )
    total = int(count_row["cnt"]) if count_row else 0

    trades = await fetch_all(
        "SELECT * FROM trades WHERE run_id = ? ORDER BY entry_date OFFSET ? LIMIT ?",
        [run_id, offset, page_size],
    )
    return PaginatedTrades(total=total, page=page, page_size=page_size, items=trades)


@router.get("/results/{run_id}/equity-curve")
async def get_equity_curve(run_id: str):
    row = await fetch_one(
        "SELECT equity_curve, benchmark_curve FROM backtest_results WHERE run_id = ?",
        [run_id],
    )
    if not row:
        raise HTTPException(status_code=404, detail="Result not found")
    return {
        "equity_curve":    json.loads(row["equity_curve"] or "[]"),
        "benchmark_curve": json.loads(row["benchmark_curve"] or "[]"),
    }


@router.get("/results/{run_id}/holding-analysis")
async def get_holding_analysis(run_id: str):
    row = await fetch_one(
        "SELECT holding_analysis FROM backtest_results WHERE run_id = ?", [run_id]
    )
    if not row:
        raise HTTPException(status_code=404, detail="Result not found")
    return {"holding_analysis": json.loads(row["holding_analysis"] or "{}")}
