import uuid
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from ...database import get_db
from ...models.backtest import BacktestRun
from ...schemas.backtest import BacktestCreateRequest, BacktestRunSchema, BacktestDetailSchema
from ...core.engine import engine as backtest_engine

router = APIRouter()

@router.post("/backtests", response_model=BacktestRunSchema, status_code=201)
async def create_backtest(
    req: BacktestCreateRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    run_id = str(uuid.uuid4())
    run = BacktestRun(
        id=run_id,
        strategy_id=req.strategy_id,
        status="pending",
        parameters=req.parameters,
        start_date=req.start_date,
        end_date=req.end_date,
        initial_capital=req.initial_capital,
    )
    db.add(run)
    await db.commit()
    await db.refresh(run)
    background_tasks.add_task(backtest_engine.run, run_id)
    return run

@router.get("/backtests", response_model=list[BacktestRunSchema])
async def list_backtests(
    skip: int = 0,
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(BacktestRun).order_by(desc(BacktestRun.created_at)).offset(skip).limit(limit)
    )
    return result.scalars().all()

@router.get("/backtests/{run_id}", response_model=BacktestDetailSchema)
async def get_backtest(run_id: str, db: AsyncSession = Depends(get_db)):
    run = await db.get(BacktestRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Backtest not found")

    from sqlalchemy import select as sa_select
    from ...models.backtest import BacktestResult
    result_row = await db.execute(
        sa_select(BacktestResult).where(BacktestResult.run_id == run_id)
    )
    result = result_row.scalar_one_or_none()

    return BacktestDetailSchema(
        run=BacktestRunSchema.model_validate(run),
        result=result,
    )
