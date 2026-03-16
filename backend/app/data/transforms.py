import pandas as pd

# Column name mappings for LHB data from AkShare
LHB_COLUMN_MAP = {
    # akshare stock_lhb_jgmmtj_em columns (institution buy/sell stats)
    "代码": "symbol",
    "名称": "name",
    "上榜日期": "date",
    "上榜日": "date",
    "机构买入总额": "buy_amount",
    "机构卖出总额": "sell_amount",
    "机构买入净额": "net_buy",
    "买方机构数": "buy_inst_count",
    "卖方机构数": "sell_inst_count",
    # akshare stock_lhb_detail_em columns (kept for compatibility)
    "解读": "interpretation",
    "收盘价": "close",
    "涨跌幅": "change_pct",
    "龙虎榜净买额": "net_amount",
    "龙虎榜买入额": "total_buy",
    "龙虎榜卖出额": "total_sell",
    "龙虎榜成交额": "total_amount",
    "市场总成交额": "market_total",
    "净买额占总成交比": "net_ratio",
    "成交额占总成交比": "total_ratio",
    "换手率": "turnover",
    "流通市值": "float_cap",
}

def normalize_lhb_df(df: pd.DataFrame) -> pd.DataFrame:
    """Rename AkShare LHB columns to standard names."""
    rename = {k: v for k, v in LHB_COLUMN_MAP.items() if k in df.columns}
    return df.rename(columns=rename)

def normalize_price_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize price DataFrame columns."""
    col_map = {
        "日期": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "振幅": "amplitude",
        "涨跌幅": "change_pct",
        "涨跌额": "change",
        "换手率": "turnover",
    }
    rename = {k: v for k, v in col_map.items() if k in df.columns}
    df = df.rename(columns=rename)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df
