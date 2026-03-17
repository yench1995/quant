from typing import ClassVar

from .base import AbstractStrategy, ParameterSpec, SignalRecord, StrategyContext


class MA5BreakoutStrategy(AbstractStrategy):
    """当日收盘价突破前5日均线（MA5），发出买入信号。

    需要 StrategyContext.price_cache 已预填充（回测引擎在信号循环前完成）。
    """

    STRATEGY_ID: ClassVar[str] = "ma5_breakout"
    STRATEGY_NAME: ClassVar[str] = "MA5突破策略"
    STRATEGY_DESCRIPTION: ClassVar[str] = (
        "收盘价上穿5日均线时买入，持有N个交易日后收盘卖出。"
        "仅对曾出现在龙虎榜的股票生效（依赖 price_cache 预加载范围）。"
    )
    PARAMETERS: ClassVar[list[ParameterSpec]] = [
        ParameterSpec("holding_days", "int", 5, 1, 30, "持仓交易日天数"),
        ParameterSpec("max_positions", "int", 10, 1, 50, "最大同时持仓数"),
        ParameterSpec("position_size_pct", "float", 0.1, 0.01, 1.0, "单股仓位比例"),
        ParameterSpec("min_history_days", "int", 6, 6, 60, "最少历史交易日数（用于计算MA5需≥6）"),
    ]

    def generate_signals(self, ctx: StrategyContext) -> list[SignalRecord]:
        if not ctx.price_cache:
            return []

        min_history = int(ctx.parameters.get("min_history_days", 6))
        max_positions = int(ctx.parameters.get("max_positions", 10))
        signals: list[SignalRecord] = []

        for symbol, df in ctx.price_cache.items():
            if df is None or df.empty:
                continue

            past = df[df["date"] < ctx.current_date].tail(min_history)
            if len(past) < min_history:
                continue

            # MA5 of the 5 days before current_date
            ma5 = past["close"].iloc[-5:].mean()
            prev_close = past["close"].iloc[-2] if len(past) >= 2 else None
            today_close = past["close"].iloc[-1]

            # Crossover: previous close was below MA5, current close is above
            if prev_close is not None and prev_close < ma5 and today_close > ma5:
                signals.append(SignalRecord(
                    symbol=symbol,
                    name="",
                    signal_date=ctx.current_date,
                    net_buy_wan=0.0,
                ))

        # Rank by distance above MA5 (descending) and cap at max_positions
        def _score(sig: SignalRecord) -> float:
            df = ctx.price_cache.get(sig.symbol)
            if df is None or df.empty:
                return 0.0
            past = df[df["date"] < ctx.current_date].tail(6)
            if len(past) < 6:
                return 0.0
            ma5 = past["close"].iloc[-5:].mean()
            close = past["close"].iloc[-1]
            return (close - ma5) / ma5 if ma5 else 0.0

        signals.sort(key=_score, reverse=True)
        return signals[:max_positions]
