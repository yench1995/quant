import pandas as pd
from fastapi import APIRouter, Query
from ...data.fetcher import AkShareFetcher
import asyncio

router = APIRouter()
fetcher = AkShareFetcher()


@router.get("/market-data/lhb")
async def get_lhb(date: str = Query(..., description="日期 YYYY-MM-DD")):
    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, lambda: fetcher.get_lhb_data(date))
    if df is None or df.empty:
        return {"date": date, "data": []}
    return {"date": date, "data": df.to_dict("records")}


@router.get("/market-data/lhb-institutions")
async def get_lhb_institutions(
    date: str = Query(..., description="日期 YYYY-MM-DD"),
    min_net_buy_wan: float = Query(0.0, description="机构净买入下限（万元），默认0即净买入>卖出"),
    only_down: bool = Query(False, description="仅显示当天股价下跌的个股"),
):
    """查询指定日期龙虎榜中机构净买入大于卖出的个股，按净买入金额降序排列。"""
    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, lambda: fetcher.get_lhb_data(date))

    if df is None or df.empty:
        return {"date": date, "total": 0, "data": []}

    min_yuan = min_net_buy_wan * 10000
    filtered = df[df["net_buy"] > min_yuan].copy()

    if only_down and "change_pct" in filtered.columns:
        filtered["change_pct"] = pd.to_numeric(filtered["change_pct"], errors="coerce")
        filtered = filtered[filtered["change_pct"] < 0]

    filtered = (
        filtered.sort_values("net_buy", ascending=False)
        .drop_duplicates(subset=["symbol"])
        .reset_index(drop=True)
    )

    records = []
    for _, row in filtered.iterrows():
        records.append({
            "symbol":           str(row.get("symbol", "")),
            "name":             str(row.get("name", "")),
            "change_pct":       round(float(row.get("change_pct", 0)), 2),
            "buy_amount_wan":   round(float(row.get("buy_amount", 0)) / 10000, 2),
            "sell_amount_wan":  round(float(row.get("sell_amount", 0)) / 10000, 2),
            "net_buy_wan":      round(float(row.get("net_buy", 0)) / 10000, 2),
            "buy_inst_count":   int(row.get("buy_inst_count", 0)),
            "sell_inst_count":  int(row.get("sell_inst_count", 0)),
        })

    return {"date": date, "total": len(records), "data": records}


@router.get("/market-data/lhb-seat-detail")
async def get_lhb_seat_detail(
    symbol: str = Query(..., description="股票代码"),
    date: str = Query(..., description="日期 YYYY-MM-DD"),
):
    """查询某只股票在指定日期的龙虎榜买入/卖出席位明细。"""
    loop = asyncio.get_event_loop()
    detail = await loop.run_in_executor(
        None, lambda: fetcher.get_lhb_seat_detail(symbol, date)
    )
    return {"symbol": symbol, "date": date, **detail}
