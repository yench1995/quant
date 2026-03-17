"""
DuckDB connection layer.

Architecture:
  - Single persistent DuckDB connection opened at startup
  - asyncio.Lock serialises all DB access (DuckDB connections are not thread-safe)
  - run_in_executor wraps every synchronous DuckDB call so the event loop is never blocked
"""
import asyncio
from pathlib import Path
from typing import Any

import duckdb

_conn: duckdb.DuckDBPyConnection | None = None
_write_lock: asyncio.Lock | None = None


def _get_lock() -> asyncio.Lock:
    global _write_lock
    if _write_lock is None:
        _write_lock = asyncio.Lock()
    return _write_lock


def init_db(path: str) -> None:
    """Open (or create) the DuckDB database and create all tables.

    Supports both local file paths and MotherDuck cloud connections:
      - Local:       init_db("../data/finance.duckdb")
      - MotherDuck:  init_db("md:finance")  (requires MOTHERDUCK_TOKEN env var)
    """
    global _conn
    if path.startswith("md:"):
        _conn = duckdb.connect(path)
    else:
        db_path = Path(path).resolve()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        _conn = duckdb.connect(str(db_path))
    _create_tables(_conn)


def close_db() -> None:
    global _conn
    if _conn:
        _conn.close()
        _conn = None


def _get_conn() -> duckdb.DuckDBPyConnection:
    if _conn is None:
        raise RuntimeError("Database not initialised. Call init_db() first.")
    return _conn


def _create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    stmts = [
        # ── Market data ──────────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS lhb_daily (
            date            VARCHAR NOT NULL,
            symbol          VARCHAR NOT NULL,
            name            VARCHAR DEFAULT '',
            buy_amount      DOUBLE  DEFAULT 0.0,
            sell_amount     DOUBLE  DEFAULT 0.0,
            net_buy         DOUBLE  DEFAULT 0.0,
            buy_inst_count  INTEGER DEFAULT 0,
            sell_inst_count INTEGER DEFAULT 0,
            PRIMARY KEY (date, symbol)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS stock_price_daily (
            date        VARCHAR NOT NULL,
            symbol      VARCHAR NOT NULL,
            open        DOUBLE  DEFAULT 0.0,
            close       DOUBLE  DEFAULT 0.0,
            high        DOUBLE  DEFAULT 0.0,
            low         DOUBLE  DEFAULT 0.0,
            volume      DOUBLE  DEFAULT 0.0,
            change_pct  DOUBLE  DEFAULT 0.0,
            PRIMARY KEY (date, symbol)
        )
        """,
        # ── Backtests ─────────────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS backtest_runs (
            id              VARCHAR PRIMARY KEY,
            strategy_id     VARCHAR NOT NULL,
            status          VARCHAR DEFAULT 'pending',
            parameters      VARCHAR DEFAULT '{}',
            start_date      VARCHAR NOT NULL,
            end_date        VARCHAR NOT NULL,
            initial_capital DOUBLE  DEFAULT 1000000.0,
            error_message   VARCHAR,
            created_at      TIMESTAMP DEFAULT now(),
            completed_at    TIMESTAMP
        )
        """,
        "CREATE SEQUENCE IF NOT EXISTS backtest_results_id_seq START 1",
        """
        CREATE TABLE IF NOT EXISTS backtest_results (
            id               BIGINT DEFAULT nextval('backtest_results_id_seq') PRIMARY KEY,
            run_id           VARCHAR NOT NULL,
            total_return     DOUBLE  DEFAULT 0.0,
            annual_return    DOUBLE  DEFAULT 0.0,
            sharpe_ratio     DOUBLE  DEFAULT 0.0,
            max_drawdown     DOUBLE  DEFAULT 0.0,
            win_rate         DOUBLE  DEFAULT 0.0,
            total_trades     INTEGER DEFAULT 0,
            equity_curve     VARCHAR DEFAULT '[]',
            benchmark_curve  VARCHAR DEFAULT '[]',
            holding_analysis VARCHAR DEFAULT '{}'
        )
        """,
        "CREATE SEQUENCE IF NOT EXISTS trades_id_seq START 1",
        """
        CREATE TABLE IF NOT EXISTS trades (
            id              BIGINT DEFAULT nextval('trades_id_seq') PRIMARY KEY,
            run_id          VARCHAR NOT NULL,
            symbol          VARCHAR NOT NULL,
            name            VARCHAR DEFAULT '',
            entry_date      VARCHAR NOT NULL,
            exit_date       VARCHAR NOT NULL,
            entry_price     DOUBLE  NOT NULL,
            exit_price      DOUBLE  NOT NULL,
            shares          INTEGER NOT NULL,
            gross_pnl       DOUBLE  DEFAULT 0.0,
            commission      DOUBLE  DEFAULT 0.0,
            net_pnl         DOUBLE  DEFAULT 0.0,
            return_pct      DOUBLE  DEFAULT 0.0,
            holding_days    INTEGER DEFAULT 0,
            signal_net_buy  DOUBLE  DEFAULT 0.0
        )
        """,
        # ── Technical indicators ──────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS stock_indicator_daily (
            date        VARCHAR NOT NULL,
            symbol      VARCHAR NOT NULL,
            ma5         DOUBLE,
            ma10        DOUBLE,
            ma20        DOUBLE,
            ma60        DOUBLE,
            macd        DOUBLE,
            macd_signal DOUBLE,
            rsi14       DOUBLE,
            PRIMARY KEY (date, symbol)
        )
        """,
        # ── Cache ─────────────────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS data_cache (
            key        VARCHAR PRIMARY KEY,
            payload    VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT now(),
            expires_at TIMESTAMP
        )
        """,
    ]
    for stmt in stmts:
        conn.execute(stmt)


# ── Public async helpers ───────────────────────────────────────────────────────

async def fetch_all(sql: str, params: list | None = None) -> list[dict[str, Any]]:
    loop = asyncio.get_event_loop()
    conn = _get_conn()
    lock = _get_lock()

    async with lock:
        def _run() -> list[dict[str, Any]]:
            rel = conn.execute(sql, params or [])
            cols = [d[0] for d in rel.description]
            return [dict(zip(cols, row)) for row in rel.fetchall()]

        return await loop.run_in_executor(None, _run)


async def fetch_one(sql: str, params: list | None = None) -> dict[str, Any] | None:
    rows = await fetch_all(sql, params)
    return rows[0] if rows else None


async def execute(sql: str, params: list | None = None) -> None:
    loop = asyncio.get_event_loop()
    conn = _get_conn()
    lock = _get_lock()

    async with lock:
        def _run() -> None:
            conn.execute(sql, params or [])

        await loop.run_in_executor(None, _run)


async def executemany(sql: str, rows: list[list | tuple]) -> None:
    if not rows:
        return
    loop = asyncio.get_event_loop()
    conn = _get_conn()
    lock = _get_lock()

    async with lock:
        def _run() -> None:
            conn.executemany(sql, rows)

        await loop.run_in_executor(None, _run)
