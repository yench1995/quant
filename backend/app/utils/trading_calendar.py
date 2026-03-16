from bisect import bisect_right, bisect_left

class TradingCalendar:
    def __init__(self, trading_days: list[str]):
        self.trading_days = sorted(trading_days)

    def next_trading_day(self, date: str, n: int = 1) -> str | None:
        idx = bisect_right(self.trading_days, date)
        target = idx + n - 1
        if target < len(self.trading_days):
            return self.trading_days[target]
        return None

    def offset(self, date: str, n: int) -> str | None:
        """Return the date n trading days after date (n can be negative)."""
        try:
            idx = self.trading_days.index(date)
        except ValueError:
            idx = bisect_right(self.trading_days, date)
        target = idx + n
        if 0 <= target < len(self.trading_days):
            return self.trading_days[target]
        return None

    def days_between(self, start: str, end: str) -> int:
        si = bisect_left(self.trading_days, start)
        ei = bisect_left(self.trading_days, end)
        return ei - si
