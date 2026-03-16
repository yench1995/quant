"""
AkShareFetcher — 支持多数据源的行情抓取器

价格数据源（通过 settings.PRICE_SOURCE 配置）：

  baostock  — 宝股，独立服务器，不爬东财，免费无需注册，稳定
  tushare   — Tushare Pro，需要 token（免费注册可得），数据最全
  akshare   — 东方财富接口，容易被限流/断连，但无需 token
  auto      — 优先 baostock → tushare（有 token 时）→ akshare（默认）

龙虎榜：仅 akshare（目前唯一免费接口）

BaoStock symbol:  sh.600000 / sz.000001 / bj.830000
Tushare symbol:   000001.SZ / 600000.SH / 000300.SH
"""

import socket
import threading
import time
import random

import pandas as pd
from datetime import datetime, timedelta

# ── pandas 2.0 兼容补丁 ────────────────────────────────────────────────────────
# tushare 内部仍调用 fillna(method='ffill')，pandas >= 2.0 已移除该参数。
# 在加载任何依赖 tushare 的代码前打补丁。
def _patch_pandas_fillna() -> None:
    from pandas.core.generic import NDFrame
    _orig = NDFrame.fillna

    def _patched(self, value=None, method=None, axis=None,
                 inplace=False, limit=None, **kwargs):
        if method is not None:
            if method in ("ffill", "pad"):
                return self.ffill(axis=axis, inplace=inplace, limit=limit)
            if method in ("bfill", "backfill"):
                return self.bfill(axis=axis, inplace=inplace, limit=limit)
        return _orig(self, value=value, axis=axis,
                     inplace=inplace, limit=limit, **kwargs)

    NDFrame.fillna = _patched  # type: ignore[method-assign]

_patch_pandas_fillna()
# ──────────────────────────────────────────────────────────────────────────────

import akshare as ak

from .transforms import normalize_lhb_df, normalize_price_df
from ..config import settings

socket.setdefaulttimeout(25)

# BaoStock 持久连接管理
# 用锁串行所有 bs 调用（bs 非线程安全），login/logout 只在启动/关闭时各一次
_baostock_lock = threading.Lock()
_baostock_logged_in = False


def baostock_login() -> bool:
    """在应用启动时调用一次，建立持久连接。"""
    global _baostock_logged_in
    try:
        import baostock as bs
        with _baostock_lock:
            if not _baostock_logged_in:
                lg = bs.login()
                _baostock_logged_in = lg.error_code == "0"
                if not _baostock_logged_in:
                    print(f"[baostock] login failed: {lg.error_msg}")
        return _baostock_logged_in
    except ImportError:
        return False


def baostock_logout() -> None:
    """在应用关闭时调用一次。"""
    global _baostock_logged_in
    try:
        import baostock as bs
        with _baostock_lock:
            if _baostock_logged_in:
                bs.logout()
                _baostock_logged_in = False
    except Exception:
        pass


# ── 符号格式转换 ───────────────────────────────────────────────────────────────

def _to_baostock_symbol(symbol: str) -> str:
    """000001 → sz.000001 / 600000 → sh.600000 / 830000 → bj.830000"""
    if symbol.startswith(("6", "5")):
        return f"sh.{symbol}"
    if symbol.startswith(("0", "1", "2", "3")):
        return f"sz.{symbol}"
    if symbol.startswith(("4", "8", "9")):
        return f"bj.{symbol}"
    return f"sz.{symbol}"


def _to_tushare_symbol(symbol: str) -> str:
    """000001 → 000001.SZ / 600000 → 600000.SH / 830000 → 830000.BJ"""
    if symbol.startswith(("6", "5")):
        return f"{symbol}.SH"
    if symbol.startswith(("0", "1", "2", "3")):
        return f"{symbol}.SZ"
    if symbol.startswith(("4", "8", "9")):
        return f"{symbol}.BJ"
    return f"{symbol}.SZ"


# ── BaoStock 实现 ──────────────────────────────────────────────────────────────

def _fetch_price_baostock(
    symbol: str, start_date: str, end_date: str
) -> pd.DataFrame | None:
    """BaoStock 前复权日线。复用全局持久连接，不在此处 login/logout。"""
    if not _baostock_logged_in:
        if not baostock_login():
            return None
    try:
        import baostock as bs
    except ImportError:
        return None

    with _baostock_lock:
        try:
            rs = bs.query_history_k_data_plus(
                _to_baostock_symbol(symbol),
                "date,open,high,low,close,volume,pctChg",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3",  # 前复权
            )
            data = []
            while rs.error_code == "0" and rs.next():
                data.append(rs.get_row_data())
        except Exception as e:
            print(f"[baostock] query error for {symbol}: {e}")
            return None

    if not data:
        return None

    df = pd.DataFrame(data, columns=["date", "open", "high", "low", "close", "volume", "change_pct"])
    for col in ("open", "high", "low", "close", "volume", "change_pct"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df = df[df["close"] > 0].reset_index(drop=True)  # 过滤停牌日
    return df if not df.empty else None


# ── Tushare 实现 ───────────────────────────────────────────────────────────────

_tushare_pro = None
_tushare_lock = threading.Lock()


def _get_tushare_pro():
    """懒加载 Tushare Pro API 实例（单例）。"""
    global _tushare_pro
    if _tushare_pro is not None:
        return _tushare_pro
    token = settings.TUSHARE_TOKEN
    if not token:
        return None
    try:
        import tushare as ts
        with _tushare_lock:
            if _tushare_pro is None:
                ts.set_token(token)   # 必须先 set_token
                _tushare_pro = ts.pro_api()
    except ImportError:
        print("[tushare] not installed, pip install tushare")
    return _tushare_pro


def _fetch_price_tushare(
    symbol: str, start_date: str, end_date: str
) -> pd.DataFrame | None:
    """
    Tushare Pro 前复权日线。
    用 pro.daily() + pro.adj_factor() 手动复权，避免 pro_bar 的 pandas 2.0 兼容问题。
    """
    pro = _get_tushare_pro()
    if pro is None:
        return None
    try:
        ts_code = _to_tushare_symbol(symbol)
        end_ext = (
            datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=60)
        ).strftime("%Y-%m-%d")
        sd = start_date.replace("-", "")
        ed = end_ext.replace("-", "")

        time.sleep(random.uniform(0.2, 0.5))

        # 不复权日线
        df = pro.daily(ts_code=ts_code, start_date=sd, end_date=ed)
        if df is None or df.empty:
            return None

        # 前复权因子
        adj = pro.adj_factor(ts_code=ts_code, start_date=sd, end_date=ed)

        df = df.rename(columns={"trade_date": "date", "vol": "volume", "pct_chg": "change_pct"})
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        if adj is not None and not adj.empty:
            adj = adj.rename(columns={"trade_date": "date"})
            adj["date"] = pd.to_datetime(adj["date"]).dt.strftime("%Y-%m-%d")
            df = df.merge(adj[["date", "adj_factor"]], on="date", how="left")
            df["adj_factor"] = df["adj_factor"].ffill().fillna(1.0)
            # 计算前复权：用最新复权因子归一化
            latest_factor = df["adj_factor"].iloc[-1] if not df.empty else 1.0
            ratio = df["adj_factor"] / latest_factor
            for col in ("open", "high", "low", "close"):
                df[col] = pd.to_numeric(df[col], errors="coerce") * ratio
        else:
            for col in ("open", "high", "low", "close"):
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df["volume"]     = pd.to_numeric(df["volume"],     errors="coerce").fillna(0.0)
        df["change_pct"] = pd.to_numeric(df["change_pct"], errors="coerce").fillna(0.0)

        df = (
            df[["date", "open", "high", "low", "close", "volume", "change_pct"]]
            .sort_values("date")
            .reset_index(drop=True)
        )
        return df if not df.empty else None
    except Exception as e:
        print(f"[tushare] error fetching price for {symbol}: {e}")
        return None


# ── AkShare 实现 ───────────────────────────────────────────────────────────────

def _fetch_price_akshare(
    symbol: str, start_date: str, end_date: str, retries: int = 3
) -> pd.DataFrame | None:
    """AkShare 东方财富前复权日线，带指数退避重试。"""
    end_ext = (
        datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=60)
    ).strftime("%Y-%m-%d")
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(0.3, 0.8))
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date.replace("-", ""),
                end_date=end_ext.replace("-", ""),
                adjust="qfq",
            )
            if df is not None and not df.empty:
                return normalize_price_df(df)
            return None
        except Exception as e:
            wait = 2 ** attempt + random.uniform(0, 1)
            if attempt < retries - 1:
                print(f"[akshare] retry {attempt + 1}/{retries} for {symbol} in {wait:.1f}s: {e}")
                time.sleep(wait)
            else:
                print(f"[akshare] error fetching price for {symbol}: {e}")
    return None


# ── 主 Fetcher 类 ──────────────────────────────────────────────────────────────

class AkShareFetcher:
    """
    统一行情抓取器。

    价格数据源通过 .env 中的 PRICE_SOURCE 切换：
      PRICE_SOURCE=baostock   只用宝股（推荐，稳定）
      PRICE_SOURCE=tushare    只用 Tushare Pro（需要 TUSHARE_TOKEN）
      PRICE_SOURCE=akshare    只用东方财富（容易被封）
      PRICE_SOURCE=auto       baostock → tushare → akshare（默认）
    """

    # ── 价格数据 ──────────────────────────────────────────────────────────────

    def get_price_history(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        source = settings.PRICE_SOURCE.lower()

        if source == "baostock":
            return _fetch_price_baostock(symbol, start_date, end_date)

        if source == "tushare":
            return _fetch_price_tushare(symbol, start_date, end_date)

        if source == "akshare":
            return _fetch_price_akshare(symbol, start_date, end_date)

        # auto：baostock → tushare（有token） → akshare
        df = _fetch_price_baostock(symbol, start_date, end_date)
        if df is not None:
            return df

        if settings.TUSHARE_TOKEN:
            print(f"[auto] baostock None for {symbol}, trying tushare")
            df = _fetch_price_tushare(symbol, start_date, end_date)
            if df is not None:
                return df

        print(f"[auto] falling back to akshare for {symbol}")
        return _fetch_price_akshare(symbol, start_date, end_date)

    # ── 交易日历 ──────────────────────────────────────────────────────────────

    def get_trading_calendar(self, start_date: str, end_date: str) -> list[str]:
        # 1. BaoStock
        try:
            import baostock as bs
            if not _baostock_logged_in:
                baostock_login()
            with _baostock_lock:
                rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
                dates = []
                while rs.error_code == "0" and rs.next():
                    row = rs.get_row_data()
                    if len(row) >= 2 and row[1] == "1":
                        dates.append(row[0])
            if dates:
                return sorted(dates)
        except Exception as e:
            print(f"[baostock] calendar error: {e}")

        # 2. Tushare
        if settings.TUSHARE_TOKEN:
            try:
                pro = _get_tushare_pro()
                if pro:
                    df = pro.trade_cal(
                        exchange="SSE",
                        start_date=start_date.replace("-", ""),
                        end_date=end_date.replace("-", ""),
                    )
                    dates = df[df["is_open"] == 1]["cal_date"].tolist()
                    dates = [f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in dates]
                    if dates:
                        return sorted(dates)
            except Exception as e:
                print(f"[tushare] calendar error: {e}")

        # 3. AkShare
        try:
            df = ak.tool_trade_date_hist_sina()
            if df is not None and not df.empty:
                col = df.columns[0]
                dates = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d").tolist()
                return sorted(d for d in dates if start_date <= d <= end_date)
        except Exception as e:
            print(f"[akshare] calendar error: {e}")

        return self._fallback_calendar(start_date, end_date)

    # ── 指数行情 ──────────────────────────────────────────────────────────────

    def get_index_history(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        # 1. BaoStock
        try:
            import baostock as bs
            if not _baostock_logged_in:
                baostock_login()
            with _baostock_lock:
                rs = bs.query_history_k_data_plus(
                    f"sh.{symbol}", "date,close",
                    start_date=start_date, end_date=end_date,
                    frequency="d",
                )
                data = []
                while rs.error_code == "0" and rs.next():
                    data.append(rs.get_row_data())
            if data:
                df = pd.DataFrame(data, columns=["date", "close"])
                df["close"] = pd.to_numeric(df["close"], errors="coerce")
                df = df[df["close"] > 0].reset_index(drop=True)
                if not df.empty:
                    return df
        except Exception as e:
            print(f"[baostock] index {symbol} error: {e}")

        # 2. Tushare
        if settings.TUSHARE_TOKEN:
            try:
                pro = _get_tushare_pro()
                if pro:
                    df = pro.index_daily(
                        ts_code=f"{symbol}.SH",
                        start_date=start_date.replace("-", ""),
                        end_date=end_date.replace("-", ""),
                    )
                    if df is not None and not df.empty:
                        df = df.rename(columns={"trade_date": "date"})
                        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                        df["close"] = pd.to_numeric(df["close"], errors="coerce")
                        return df[["date", "close"]].sort_values("date").reset_index(drop=True)
            except Exception as e:
                print(f"[tushare] index {symbol} error: {e}")

        # 3. AkShare
        try:
            df = ak.stock_zh_index_daily(symbol=f"sh{symbol}")
            if df is None or df.empty:
                return None
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
            return df
        except Exception as e:
            print(f"[akshare] index {symbol} error: {e}")
            return None

    # ── 龙虎榜 — 仅 AkShare ───────────────────────────────────────────────────

    def get_lhb_data(self, date: str) -> pd.DataFrame | None:
        try:
            date_fmt = date.replace("-", "")
            df = ak.stock_lhb_jgmmtj_em(start_date=date_fmt, end_date=date_fmt)
            if df is None or df.empty:
                return None
            df = normalize_lhb_df(df)
            for col in ("buy_amount", "sell_amount", "net_buy"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            return df
        except Exception as e:
            print(f"[akshare] LHB data {date}: {e}")
            return None

    def get_lhb_raw(self, start_date: str, end_date: str) -> pd.DataFrame | None:
        try:
            df = ak.stock_lhb_jgmmtj_em(
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
            )
            if df is not None and not df.empty:
                return normalize_lhb_df(df)
            return None
        except Exception as e:
            print(f"[akshare] LHB raw: {e}")
            return None

    def get_lhb_seat_detail(self, symbol: str, date: str) -> dict:
        date_fmt = date.replace("-", "")
        result = {"buy": [], "sell": []}
        for flag, key in [("买入", "buy"), ("卖出", "sell")]:
            try:
                df = ak.stock_lhb_stock_detail_em(symbol=symbol, date=date_fmt, flag=flag)
                if df is None or df.empty:
                    continue
                df = df.rename(columns={
                    "交易营业部名称": "seat_name",
                    "买入金额": "buy_amount",
                    "买入金额-占总成交比例": "buy_ratio",
                    "卖出金额": "sell_amount",
                    "卖出金额-占总成交比例": "sell_ratio",
                    "净额": "net_amount",
                    "类型": "reason",
                })
                for col in ("buy_amount", "sell_amount", "net_amount"):
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
                df = df.drop_duplicates(subset=["seat_name", "buy_amount", "sell_amount"])
                name_count: dict[str, int] = {}
                seats = []
                for _, row in df.iterrows():
                    raw = str(row.get("seat_name", ""))
                    name_count[raw] = name_count.get(raw, 0) + 1
                    seats.append({
                        "_raw": raw,
                        "buy_amount":  float(row["buy_amount"]),
                        "sell_amount": float(row["sell_amount"]),
                        "net_amount":  float(row["net_amount"]),
                    })
                dup = {n for n, c in name_count.items() if c > 1}
                counters: dict[str, int] = {}
                for seat in seats:
                    n = seat.pop("_raw")
                    if n in dup:
                        counters[n] = counters.get(n, 0) + 1
                        display = f"{n} #{counters[n]}"
                    else:
                        display = n
                    seat["seat_name"]        = display
                    seat["buy_amount_wan"]   = round(seat.pop("buy_amount") / 10000, 2)
                    seat["sell_amount_wan"]  = round(seat.pop("sell_amount") / 10000, 2)
                    seat["net_amount_wan"]   = round(seat.pop("net_amount") / 10000, 2)
                    seat["is_institution"]   = "机构" in display
                result[key] = seats
            except Exception as e:
                print(f"[akshare] seat detail {symbol} {date} {flag}: {e}")
        return result

    # ── 兜底日历 ──────────────────────────────────────────────────────────────

    def _fallback_calendar(self, start_date: str, end_date: str) -> list[str]:
        dates, cur = [], datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        while cur <= end:
            if cur.weekday() < 5:
                dates.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        return dates


fetcher = AkShareFetcher()
