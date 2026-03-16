from sqlalchemy import Column, Integer, String, Float, UniqueConstraint, Index
from ..database import Base


class StockPriceDaily(Base):
    __tablename__ = "stock_price_daily"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String(10), nullable=False)
    symbol = Column(String(10), nullable=False)
    open = Column(Float, default=0.0)
    close = Column(Float, default=0.0)
    high = Column(Float, default=0.0)
    low = Column(Float, default=0.0)
    volume = Column(Float, default=0.0)
    change_pct = Column(Float, default=0.0)

    __table_args__ = (
        UniqueConstraint("date", "symbol", name="uq_price_date_symbol"),
        Index("ix_price_date", "date"),
        Index("ix_price_symbol", "symbol"),
    )
