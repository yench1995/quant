from sqlalchemy import String, JSON, Float, Integer, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship
from ..database import Base

class Strategy(Base):
    __tablename__ = "strategies"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, default="")
    module_path: Mapped[str] = mapped_column(String, nullable=False)
    class_name: Mapped[str] = mapped_column(String, nullable=False)
    parameters: Mapped[list] = mapped_column(JSON, default=list)

class StrategyParameter(Base):
    __tablename__ = "strategy_parameters"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    strategy_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    default = mapped_column(JSON)
    min_val = mapped_column(JSON, nullable=True)
    max_val = mapped_column(JSON, nullable=True)
    description: Mapped[str] = mapped_column(String, default="")
