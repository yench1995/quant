from .base import AbstractStrategy, ParameterSpec, SignalRecord, StrategyContext
from typing import ClassVar

class LHBInstitutionStrategy(AbstractStrategy):
    STRATEGY_ID: ClassVar[str] = "lhb_institution"
    STRATEGY_NAME: ClassVar[str] = "龙虎榜机构净买入"
    STRATEGY_DESCRIPTION: ClassVar[str] = (
        "筛选龙虎榜中机构净买入股票，次日开盘买入，持有N个交易日后收盘卖出"
    )
    PARAMETERS: ClassVar[list[ParameterSpec]] = [
        ParameterSpec("holding_days", "int", 5, 1, 30, "持仓交易日天数"),
        ParameterSpec("min_net_buy_wan", "float", 500.0, 0.0, None, "机构净买入下限（万元）"),
        ParameterSpec("include_quant", "bool", True, None, None, "是否纳入量化机构"),
        ParameterSpec("max_positions", "int", 10, 1, 50, "最大同时持仓数"),
        ParameterSpec("position_size_pct", "float", 0.1, 0.01, 1.0, "单股仓位比例"),
        ParameterSpec("run_holding_sweep", "bool", False, None, None, "扫描1-30天所有持仓周期"),
    ]

    def generate_signals(self, ctx: StrategyContext) -> list[SignalRecord]:
        min_net_buy = ctx.parameters.get("min_net_buy_wan", 500.0)
        max_positions = ctx.parameters.get("max_positions", 10)

        try:
            df = ctx.fetcher.get_lhb_data(ctx.current_date)
        except Exception:
            return []

        if df is None or df.empty:
            return []

        # stock_lhb_jgmmtj_em already aggregates institution buy/sell per stock.
        # net_buy column is 机构买入净额 (元)
        if "net_buy" not in df.columns:
            # Fallback: compute from buy/sell columns if available
            if "buy_amount" in df.columns and "sell_amount" in df.columns:
                df = df.copy()
                df["net_buy"] = df["buy_amount"] - df["sell_amount"]
            else:
                return []

        min_net_buy_yuan = min_net_buy * 10000  # convert 万元 to 元
        filtered = df[df["net_buy"] >= min_net_buy_yuan].copy()

        if filtered.empty:
            return []

        # Deduplicate: same stock may appear for multiple listing reasons — take max net_buy row
        filtered = (
            filtered.sort_values("net_buy", ascending=False)
            .drop_duplicates(subset=["symbol"])
            .head(max_positions)
        )

        signals = []
        for _, row in filtered.iterrows():
            signals.append(SignalRecord(
                symbol=str(row["symbol"]),
                name=str(row.get("name", "")),
                signal_date=ctx.current_date,
                net_buy_wan=float(row["net_buy"]) / 10000,
            ))

        return signals
