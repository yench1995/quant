from sqlalchemy import Column, Integer, String, Float, UniqueConstraint, Index
from ..database import Base


class LHBDaily(Base):
    __tablename__ = "lhb_daily"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(String(10), nullable=False)
    symbol = Column(String(10), nullable=False)
    name = Column(String(50), nullable=False, default="")
    buy_amount = Column(Float, default=0.0)
    sell_amount = Column(Float, default=0.0)
    net_buy = Column(Float, default=0.0)
    buy_inst_count = Column(Integer, default=0)
    sell_inst_count = Column(Integer, default=0)

    __table_args__ = (
        UniqueConstraint("date", "symbol", name="uq_lhb_date_symbol"),
        Index("ix_lhb_date", "date"),
        Index("ix_lhb_symbol", "symbol"),
    )
