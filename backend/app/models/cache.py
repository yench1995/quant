from sqlalchemy import String, Text, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from ..database import Base

class DataCache(Base):
    __tablename__ = "data_cache"
    key: Mapped[str] = mapped_column(String, primary_key=True)
    payload: Mapped[str] = mapped_column(Text, nullable=False)  # JSON string
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
