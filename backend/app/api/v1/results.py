from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from ...database import get_db
from ...models.backtest import BacktestResult, Trade
from ...schemas.backtest import PaginatedTrades, TradeSchema

router = APIRouter()

@router.get("/results/{run_id}/trades", response_model=PaginatedTrades)
async def get_trades(
    run_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    offset = (page - 1) * page_size
    total_result = await db.execute(
        select(func.count()).select_from(Trade).where(Trade.run_id == run_id)
    )
    total = total_result.scalar_one()

    trades_result = await db.execute(
        select(Trade)
        .where(Trade.run_id == run_id)
        .order_by(Trade.entry_date)
        .offset(offset)
        .limit(page_size)
    )
    trades = trades_result.scalars().all()

    return PaginatedTrades(total=total, page=page, page_size=page_size, items=trades)

@router.get("/results/{run_id}/equity-curve")
async def get_equity_curve(run_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(BacktestResult).where(BacktestResult.run_id == run_id)
    )
    row = result.scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Result not found")
    return {
        "equity_curve": row.equity_curve,
        "benchmark_curve": row.benchmark_curve,
    }

@router.get("/results/{run_id}/holding-analysis")
async def get_holding_analysis(run_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(BacktestResult).where(BacktestResult.run_id == run_id)
    )
    row = result.scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Result not found")
    return {"holding_analysis": row.holding_analysis}
