import json
from datetime import datetime, timedelta

from ..database import fetch_one, execute


class CacheManager:
    async def get(self, key: str) -> dict | list | None:
        row = await fetch_one(
            "SELECT payload, expires_at FROM data_cache WHERE key = ?", [key]
        )
        if row is None:
            return None
        expires_at = row.get("expires_at")
        if expires_at and expires_at < datetime.utcnow():
            await execute("DELETE FROM data_cache WHERE key = ?", [key])
            return None
        return json.loads(row["payload"])

    async def set(self, key: str, value: dict | list, ttl_hours: int = 24 * 7) -> None:
        expires_at = datetime.utcnow() + timedelta(hours=ttl_hours)
        payload = json.dumps(value, ensure_ascii=False, default=str)
        await execute(
            """
            INSERT INTO data_cache (key, payload, expires_at) VALUES (?, ?, ?)
            ON CONFLICT (key) DO UPDATE SET
                payload    = EXCLUDED.payload,
                expires_at = EXCLUDED.expires_at
            """,
            [key, payload, expires_at],
        )
