import json
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from ..models.cache import DataCache

class CacheManager:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get(self, key: str) -> dict | list | None:
        result = await self.session.execute(
            select(DataCache).where(DataCache.key == key)
        )
        row = result.scalar_one_or_none()
        if row is None:
            return None
        if row.expires_at and row.expires_at < datetime.utcnow():
            await self.session.delete(row)
            await self.session.commit()
            return None
        return json.loads(row.payload)

    async def set(self, key: str, value: dict | list, ttl_hours: int = 24 * 7):
        expires_at = datetime.utcnow() + timedelta(hours=ttl_hours)
        existing = await self.session.execute(
            select(DataCache).where(DataCache.key == key)
        )
        row = existing.scalar_one_or_none()
        payload = json.dumps(value, ensure_ascii=False, default=str)
        if row:
            row.payload = payload
            row.expires_at = expires_at
        else:
            self.session.add(DataCache(key=key, payload=payload, expires_at=expires_at))
        await self.session.commit()
