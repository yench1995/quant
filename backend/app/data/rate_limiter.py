"""
ConservativeRateLimiter — 自适应限速器

行为：
  - 正常:     3 ± 1.5s 随机间隔
  - 连续出错: 指数退避，最大 300s
  - 成功后:   重置退避计数，恢复正常间隔
  - 429/ban:  立即跳到高退避区间
"""

import asyncio
import random
import time


class ConservativeRateLimiter:
    def __init__(
        self,
        base_delay: float = 3.0,
        jitter: float = 1.5,
        max_backoff: float = 300.0,
    ) -> None:
        self.base_delay = base_delay
        self.jitter = jitter
        self.max_backoff = max_backoff

        self._consecutive_errors = 0
        self._last_call_time: float = 0.0

    async def wait(self) -> None:
        """每次 API 调用前调用，阻塞直到可以发起下一次请求。"""
        if self._consecutive_errors == 0:
            delay = self.base_delay + random.uniform(-self.jitter, self.jitter)
            delay = max(delay, 0.5)
        else:
            backoff = min(self.base_delay * (2 ** self._consecutive_errors), self.max_backoff)
            delay = backoff + random.uniform(0, backoff * 0.2)

        elapsed = time.monotonic() - self._last_call_time
        remaining = delay - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)

        self._last_call_time = time.monotonic()

    def on_success(self) -> None:
        """成功后调用，重置退避。"""
        self._consecutive_errors = 0

    def on_error(self) -> None:
        """失败后调用，增加退避计数。"""
        self._consecutive_errors = min(self._consecutive_errors + 1, 10)

    def on_rate_limited(self) -> None:
        """收到 429 或被封时调用，大幅增加退避。"""
        self._consecutive_errors = min(self._consecutive_errors + 3, 10)

    def reset(self) -> None:
        """手动重置状态（用于新 Phase 开始时）。"""
        self._consecutive_errors = 0
        self._last_call_time = 0.0
