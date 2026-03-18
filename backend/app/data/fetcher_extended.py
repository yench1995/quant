"""
fetcher_extended.py — 扩展数据采集器

覆盖的数据类型：
  - 股票基本信息      (stock_basic)
  - 停复牌记录        (stock_suspend)
  - ST 状态当前快照   (stock_st_daily)
  - 北向资金汇总      (northbound_flow_daily)
  - 主力资金流向      (stock_money_flow_daily)
  - PE/PB/市值日频    (stock_valuation_daily)
  - 指数成分股快照    (index_constituent)

每个函数在调用 AkShare API 前后调用 rate_limiter.wait() / on_success() / on_error()。
所有函数均为 async，内部用 run_in_executor 包裹同步 API。
"""

import asyncio
import traceback
from typing import Any

import akshare as ak
import pandas as pd

from .rate_limiter import ConservativeRateLimiter

# 全局限速器（seed_full.py 中也可直接引用）
rate_limiter = ConservativeRateLimiter(base_delay=3.0, jitter=1.5, max_backoff=300.0)

_loop_ref: asyncio.AbstractEventLoop | None = None


def _get_loop() -> asyncio.AbstractEventLoop:
    return asyncio.get_event_loop()


async def _run_sync(fn, *args, **kwargs):
    """在线程池中运行同步函数，避免阻塞事件循环。"""
    loop = _get_loop()
    return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))


# ── 股票基本信息 ────────────────────────────────────────────────────────────────

async def fetch_stock_list() -> list[dict]:
    """
    获取全量 A 股代码+名称列表。
    返回: [{"symbol": "000001", "name": "平安银行"}, ...]
    """
    await rate_limiter.wait()
    try:
        df = await _run_sync(ak.stock_info_a_code_name)
        rate_limiter.on_success()
        if df is None or df.empty:
            return []
        # 列名适配
        col_map = {}
        for c in df.columns:
            if "code" in c.lower() or "股票代码" in c:
                col_map[c] = "symbol"
            elif "name" in c.lower() or "股票名称" in c:
                col_map[c] = "name"
        df = df.rename(columns=col_map)
        if "symbol" not in df.columns:
            df.columns = ["symbol", "name"] + list(df.columns[2:])
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)
        return df[["symbol", "name"]].to_dict(orient="records")
    except Exception as e:
        rate_limiter.on_error()
        print(f"[fetcher_ext] fetch_stock_list error: {e}")
        return []


async def fetch_stock_individual_info(symbol: str) -> dict | None:
    """
    获取单只股票的基本信息：行业、上市日期、交易所等。
    返回: {"symbol": ..., "industry": ..., "listing_date": ..., "exchange": ..., "is_active": True}
    """
    await rate_limiter.wait()
    try:
        df = await _run_sync(ak.stock_individual_info_em, symbol=symbol)
        rate_limiter.on_success()
        if df is None or df.empty:
            return None
        # df 通常是两列: item / value
        info: dict[str, str] = {}
        for _, row in df.iterrows():
            vals = row.tolist()
            if len(vals) >= 2:
                info[str(vals[0])] = str(vals[1])

        def _get(*keys: str) -> str:
            for k in keys:
                if k in info:
                    return info[k]
            return ""

        listing_raw = _get("上市时间", "上市日期", "ipo_date")
        listing_date = ""
        if listing_raw and listing_raw not in ("-", "--", "None"):
            try:
                listing_date = pd.to_datetime(listing_raw).strftime("%Y-%m-%d")
            except Exception:
                listing_date = listing_raw[:10] if len(listing_raw) >= 10 else listing_raw

        exchange_raw = _get("交易所", "exchange", "市场")
        if "上海" in exchange_raw or symbol.startswith(("6", "5")):
            exchange = "SH"
        elif "深圳" in exchange_raw or symbol.startswith(("0", "1", "2", "3")):
            exchange = "SZ"
        elif "北京" in exchange_raw or symbol.startswith(("4", "8", "9")):
            exchange = "BJ"
        else:
            exchange = ""

        return {
            "symbol": symbol,
            "industry": _get("行业", "所属行业", "industry"),
            "listing_date": listing_date,
            "exchange": exchange,
            "is_active": True,
        }
    except Exception as e:
        rate_limiter.on_error()
        print(f"[fetcher_ext] fetch_stock_individual_info {symbol} error: {e}")
        return None


# ── 停复牌记录 ──────────────────────────────────────────────────────────────────

async def fetch_stock_suspend_by_date(date_str: str) -> list[dict]:
    """
    获取指定交易日的停牌股票列表。
    date_str: 'YYYY-MM-DD'
    返回: [{"symbol": ..., "suspend_date": ..., "resume_date": ..., "reason": ...}, ...]
    """
    await rate_limiter.wait()
    try:
        date_compact = date_str.replace("-", "")
        df = await _run_sync(ak.stock_tfp_em, date=date_compact)
        rate_limiter.on_success()
        if df is None or df.empty:
            return []

        results = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            # 字段适配
            symbol = str(row_dict.get("股票代码", row_dict.get("code", ""))).zfill(6)
            resume_raw = str(row_dict.get("预计复牌日期", row_dict.get("resume_date", "")))
            resume_date = ""
            if resume_raw and resume_raw not in ("-", "--", "None", "nan"):
                try:
                    resume_date = pd.to_datetime(resume_raw).strftime("%Y-%m-%d")
                except Exception:
                    resume_date = resume_raw[:10]

            reason = str(row_dict.get("停牌原因", row_dict.get("reason", "")))
            if not symbol or symbol == "000000":
                continue
            results.append({
                "symbol": symbol,
                "suspend_date": date_str,
                "resume_date": resume_date,
                "reason": reason[:200] if reason else "",
            })
        return results
    except Exception as e:
        if "429" in str(e) or "too many" in str(e).lower():
            rate_limiter.on_rate_limited()
        else:
            rate_limiter.on_error()
        print(f"[fetcher_ext] fetch_stock_suspend_by_date {date_str} error: {e}")
        return []


# ── ST 状态（当前快照）──────────────────────────────────────────────────────────

async def fetch_st_stocks_current() -> list[dict]:
    """
    获取当前所有 ST / *ST 股票列表（快照）。
    返回: [{"symbol": ..., "name": ..., "st_type": "ST"/"*ST"}, ...]
    """
    await rate_limiter.wait()
    try:
        df = await _run_sync(ak.stock_zh_a_st_em)
        rate_limiter.on_success()
        if df is None or df.empty:
            return []

        results = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            symbol = str(row_dict.get("代码", row_dict.get("code", ""))).zfill(6)
            name = str(row_dict.get("名称", row_dict.get("name", "")))
            st_type = "*ST" if "*ST" in name else "ST"
            if not symbol or symbol == "000000":
                continue
            results.append({"symbol": symbol, "name": name, "st_type": st_type})
        return results
    except Exception as e:
        rate_limiter.on_error()
        print(f"[fetcher_ext] fetch_st_stocks_current error: {e}")
        return []


# ── 北向资金汇总 ────────────────────────────────────────────────────────────────

async def fetch_northbound_flow(start_date: str, end_date: str) -> list[dict]:
    """
    获取北向资金（沪深港通北向）汇总流向。
    返回: [{"date": ..., "market": "北向资金", "net_buy": ..., "buy_amount": ..., "sell_amount": ...}, ...]
    """
    await rate_limiter.wait()
    try:
        df = await _run_sync(
            ak.stock_hsgt_fund_flow_summary_em,
            indicator="北向资金",
        )
        rate_limiter.on_success()
        if df is None or df.empty:
            return []

        results = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            # 字段名适配
            date_raw = str(row_dict.get("日期", row_dict.get("date", "")))
            try:
                date_str = pd.to_datetime(date_raw).strftime("%Y-%m-%d")
            except Exception:
                continue
            if not (start_date <= date_str <= end_date):
                continue

            def _float(key: str) -> float:
                for k in [key] + list(row_dict.keys()):
                    if key.lower() in k.lower():
                        try:
                            return float(str(row_dict[k]).replace(",", "") or 0)
                        except Exception:
                            pass
                return 0.0

            net_buy = 0.0
            buy_amount = 0.0
            sell_amount = 0.0
            for k, v in row_dict.items():
                k_lower = k.lower()
                try:
                    val = float(str(v).replace(",", "") or 0)
                except Exception:
                    val = 0.0
                if "净" in k or "net" in k_lower:
                    net_buy = val
                elif "买" in k and "卖" not in k or "buy" in k_lower:
                    buy_amount = val
                elif "卖" in k or "sell" in k_lower:
                    sell_amount = val

            results.append({
                "date": date_str,
                "market": "北向资金",
                "net_buy": net_buy,
                "buy_amount": buy_amount,
                "sell_amount": sell_amount,
            })
        return results
    except Exception as e:
        rate_limiter.on_error()
        print(f"[fetcher_ext] fetch_northbound_flow error: {e}")
        return []


# ── 主力资金流向 ────────────────────────────────────────────────────────────────

def _symbol_to_market(symbol: str) -> str:
    """推断市场: sh / sz"""
    if symbol.startswith(("6", "5")):
        return "sh"
    return "sz"


async def fetch_money_flow(symbol: str) -> list[dict]:
    """
    获取单只股票的主力资金流向历史。
    返回: [{"date": ..., "symbol": ..., "main_net_inflow": ..., ...}, ...]
    """
    await rate_limiter.wait()
    try:
        market = _symbol_to_market(symbol)
        df = await _run_sync(
            ak.stock_individual_fund_flow,
            stock=symbol,
            market=market,
        )
        rate_limiter.on_success()
        if df is None or df.empty:
            return []

        results = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            date_raw = str(row_dict.get("日期", row_dict.get("date", "")))
            try:
                date_str = pd.to_datetime(date_raw).strftime("%Y-%m-%d")
            except Exception:
                continue

            def _fval(keys: list[str]) -> float:
                for k in row_dict:
                    for key in keys:
                        if key in k:
                            try:
                                return float(str(row_dict[k]).replace(",", "") or 0)
                            except Exception:
                                return 0.0
                return 0.0

            results.append({
                "date": date_str,
                "symbol": symbol,
                "main_net_inflow": _fval(["主力净流入", "main_net"]),
                "main_net_inflow_pct": _fval(["主力净流入占比", "main_net_pct", "主力净占比"]),
                "super_large_net": _fval(["超大单净流入", "super_large"]),
                "large_net": _fval(["大单净流入", "large_net"]),
                "medium_net": _fval(["中单净流入", "medium_net"]),
                "small_net": _fval(["小单净流入", "small_net"]),
            })
        return results
    except Exception as e:
        if "429" in str(e) or "too many" in str(e).lower():
            rate_limiter.on_rate_limited()
        else:
            rate_limiter.on_error()
        print(f"[fetcher_ext] fetch_money_flow {symbol} error: {e}")
        return []


# ── PE/PB/市值日频 ──────────────────────────────────────────────────────────────

async def fetch_valuation(symbol: str) -> list[dict]:
    """
    获取单只股票的历史 PE/PB/市值日频数据。
    返回: [{"date": ..., "symbol": ..., "pe_ttm": ..., "pb": ..., "ps_ttm": ..., "total_mv": ..., "circ_mv": ...}, ...]
    """
    await rate_limiter.wait()
    try:
        df = await _run_sync(ak.stock_a_lg_indicator, symbol=symbol)
        rate_limiter.on_success()
        if df is None or df.empty:
            return []

        results = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            date_raw = str(row_dict.get("trade_date", row_dict.get("日期", row_dict.get("date", ""))))
            try:
                date_str = pd.to_datetime(date_raw).strftime("%Y-%m-%d")
            except Exception:
                continue

            def _fv(keys: list[str]) -> float | None:
                for k in row_dict:
                    for key in keys:
                        if key.lower() == k.lower() or key in k:
                            try:
                                v = float(str(row_dict[k]).replace(",", "") or "nan")
                                return None if pd.isna(v) else v
                            except Exception:
                                return None
                return None

            results.append({
                "date": date_str,
                "symbol": symbol,
                "pe_ttm": _fv(["pe_ttm", "市盈率TTM", "pe"]),
                "pb": _fv(["pb", "市净率", "pb_mrq"]),
                "ps_ttm": _fv(["ps_ttm", "市销率TTM", "ps"]),
                "total_mv": _fv(["total_mv", "总市值"]),
                "circ_mv": _fv(["circ_mv", "流通市值"]),
            })
        return results
    except Exception as e:
        if "429" in str(e) or "too many" in str(e).lower():
            rate_limiter.on_rate_limited()
        else:
            rate_limiter.on_error()
        print(f"[fetcher_ext] fetch_valuation {symbol} error: {e}")
        return []


# ── 指数成分股 ──────────────────────────────────────────────────────────────────

async def fetch_index_constituents(index_code: str) -> list[dict]:
    """
    获取指数成分股当前快照。
    index_code: '000300' / '000905' / '000852' / '000016' / '000688'
    返回: [{"index_code": ..., "symbol": ..., "in_date": "current"}, ...]
    """
    await rate_limiter.wait()
    try:
        df = await _run_sync(ak.index_stock_cons, symbol=index_code)
        rate_limiter.on_success()
        if df is None or df.empty:
            return []

        results = []
        for _, row in df.iterrows():
            row_dict = row.to_dict()
            symbol = str(row_dict.get("品种代码", row_dict.get("code", row_dict.get("symbol", "")))).zfill(6)
            if not symbol or symbol == "000000":
                continue
            results.append({
                "index_code": index_code,
                "symbol": symbol,
                "in_date": "current",
                "out_date": None,
            })
        return results
    except Exception as e:
        rate_limiter.on_error()
        print(f"[fetcher_ext] fetch_index_constituents {index_code} error: {e}")
        return []
