import json
import uuid
from datetime import datetime
from fastapi import APIRouter, BackgroundTasks, HTTPException

from ...database import fetch_all, fetch_one, execute
from ...schemas.backtest import (
    BacktestCreateRequest,
    BacktestRunSchema,
    BacktestDetailSchema,
    BacktestResultSchema,
)
from ...core.engine import engine as backtest_engine

router = APIRouter()


def _parse_run(row: dict) -> dict:
    """Deserialise JSON fields in a backtest_runs row."""
    if row and row.get("parameters"):
        row["parameters"] = json.loads(row["parameters"])
    return row


def _parse_result(row: dict) -> dict:
    """Deserialise JSON fields in a backtest_results row."""
    if row:
        for field in ("equity_curve", "benchmark_curve", "holding_analysis"):
            if row.get(field):
                row[field] = json.loads(row[field])
    return row


@router.post("/backtests", response_model=BacktestRunSchema, status_code=201)
async def create_backtest(
    req: BacktestCreateRequest,
    background_tasks: BackgroundTasks,
):
    run_id = str(uuid.uuid4())
    now = datetime.utcnow()
    await execute(
        """
        INSERT INTO backtest_runs
            (id, strategy_id, status, parameters, start_date, end_date,
             initial_capital, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            req.strategy_id,
            "pending",
            json.dumps(req.parameters),
            req.start_date,
            req.end_date,
            req.initial_capital,
            now,
        ],
    )
    background_tasks.add_task(backtest_engine.run, run_id)
    return {
        "id":              run_id,
        "strategy_id":     req.strategy_id,
        "status":          "pending",
        "parameters":      req.parameters,
        "start_date":      req.start_date,
        "end_date":        req.end_date,
        "initial_capital": req.initial_capital,
        "error_message":   None,
        "created_at":      now,
        "completed_at":    None,
    }


@router.get("/backtests", response_model=list[BacktestRunSchema])
async def list_backtests(skip: int = 0, limit: int = 50):
    rows = await fetch_all(
        "SELECT * FROM backtest_runs ORDER BY created_at DESC OFFSET ? LIMIT ?",
        [skip, limit],
    )
    return [_parse_run(r) for r in rows]


@router.delete("/backtests/{run_id}", status_code=204)
async def delete_backtest(run_id: str):
    run = await fetch_one("SELECT id FROM backtest_runs WHERE id = ?", [run_id])
    if not run:
        raise HTTPException(status_code=404, detail="Backtest not found")
    await execute("DELETE FROM trades WHERE run_id = ?", [run_id])
    await execute("DELETE FROM backtest_results WHERE run_id = ?", [run_id])
    await execute("DELETE FROM backtest_runs WHERE id = ?", [run_id])


@router.get("/backtests/{run_id}", response_model=BacktestDetailSchema)
async def get_backtest(run_id: str):
    run = await fetch_one("SELECT * FROM backtest_runs WHERE id = ?", [run_id])
    if not run:
        raise HTTPException(status_code=404, detail="Backtest not found")
    _parse_run(run)

    result_row = await fetch_one(
        "SELECT * FROM backtest_results WHERE run_id = ?", [run_id]
    )
    result = _parse_result(result_row) if result_row else None

    return BacktestDetailSchema(
        run=BacktestRunSchema.model_validate(run),
        result=BacktestResultSchema.model_validate(result) if result else None,
    )
