import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from .transforms import normalize_lhb_df, normalize_price_df

class AkShareFetcher:
    """Wrapper around AkShare with standardized column names."""

    def get_lhb_data(self, date: str) -> pd.DataFrame | None:
        """
        Get institution buy/sell statistics for a specific date using stock_lhb_jgmmtj_em.
        Returns DataFrame with columns: symbol, name, buy_amount, sell_amount, net_buy (元)
        """
        try:
            date_fmt = date.replace("-", "")
            df = ak.stock_lhb_jgmmtj_em(start_date=date_fmt, end_date=date_fmt)
            if df is None or df.empty:
                return None
            df = normalize_lhb_df(df)
            # Ensure numeric columns
            for col in ["buy_amount", "sell_amount", "net_buy"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            return df
        except Exception as e:
            print(f"Error fetching LHB data for {date}: {e}")
            return None

    def get_lhb_raw(self, start_date: str, end_date: str) -> pd.DataFrame | None:
        """Get raw institution LHB data for date range."""
        try:
            sd = start_date.replace("-", "")
            ed = end_date.replace("-", "")
            df = ak.stock_lhb_jgmmtj_em(start_date=sd, end_date=ed)
            if df is not None and not df.empty:
                return normalize_lhb_df(df)
            return None
        except Exception as e:
            print(f"Error fetching LHB raw: {e}")
            return None

    def get_price_history(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame | None:
        """Get daily OHLCV data for a stock."""
        try:
            # Extend end date by 60 days to cover exit trades
            end_ext = (
                datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=60)
            ).strftime("%Y-%m-%d")
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
            print(f"Error fetching price for {symbol}: {e}")
            return None

    def get_trading_calendar(self, start_date: str, end_date: str) -> list[str]:
        """Get list of A-share trading days."""
        try:
            df = ak.tool_trade_date_hist_sina()
            if df is None or df.empty:
                return self._fallback_calendar(start_date, end_date)

            # The column might be 'trade_date' or unnamed
            col = df.columns[0]
            dates = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d").tolist()
            dates = [d for d in dates if start_date <= d <= end_date]
            return sorted(dates)
        except Exception as e:
            print(f"Error fetching trading calendar: {e}")
            return self._fallback_calendar(start_date, end_date)

    def get_index_history(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame | None:
        """Get index history (e.g., 000300 for CSI 300)."""
        try:
            df = ak.stock_zh_index_daily(symbol=f"sh{symbol}")
            if df is None or df.empty:
                return None
            df = df.rename(columns={"date": "date", "open": "open", "close": "close"})
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
            return df
        except Exception as e:
            print(f"Error fetching index {symbol}: {e}")
            return None

    def _fallback_calendar(self, start_date: str, end_date: str) -> list[str]:
        """Simple fallback: weekdays only."""
        dates = []
        cur = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        while cur <= end:
            if cur.weekday() < 5:
                dates.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        return dates

    def get_lhb_seat_detail(self, symbol: str, date: str) -> dict:
        """
        Get buy/sell seat details for a specific stock on a specific date.
        Returns {'buy': [...], 'sell': [...]}
        """
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
                for col in ["buy_amount", "sell_amount", "net_amount"]:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
                # Deduplicate: same seat + same amounts = same row appearing in multiple listing reasons
                # Keep distinct rows (different amounts = genuinely different anonymous institution seats)
                df = df.drop_duplicates(subset=["seat_name", "buy_amount", "sell_amount"])
                # Number duplicate seat names (e.g. "机构专用 #1", "机构专用 #2")
                name_count: dict[str, int] = {}
                seats = []
                for _, row in df.iterrows():
                    raw_name = str(row.get("seat_name", ""))
                    name_count[raw_name] = name_count.get(raw_name, 0) + 1
                    seats.append({
                        "_raw_name": raw_name,
                        "buy_amount": float(row["buy_amount"]),
                        "sell_amount": float(row["sell_amount"]),
                        "net_amount": float(row["net_amount"]),
                    })
                # Second pass: assign display names with counter if duplicates exist
                dup_names = {n for n, c in name_count.items() if c > 1}
                counters: dict[str, int] = {}
                for seat in seats:
                    n = seat.pop("_raw_name")
                    if n in dup_names:
                        counters[n] = counters.get(n, 0) + 1
                        display_name = f"{n} #{counters[n]}"
                    else:
                        display_name = n
                    seat["seat_name"] = display_name
                    seat["buy_amount_wan"] = round(seat.pop("buy_amount") / 10000, 2)
                    seat["sell_amount_wan"] = round(seat.pop("sell_amount") / 10000, 2)
                    seat["net_amount_wan"] = round(seat.pop("net_amount") / 10000, 2)
                    seat["is_institution"] = "机构" in display_name
                result[key] = seats
            except Exception as e:
                print(f"Error fetching seat detail {symbol} {date} {flag}: {e}")

        return result


fetcher = AkShareFetcher()
