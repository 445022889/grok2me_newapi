"""Token 刷新调度器"""

import asyncio
import time
from typing import Optional

from app.core.config import get_config
from app.core.logger import logger
from app.core.storage import get_storage, StorageError, RedisStorage
from app.services.token.manager import (
    DEFAULT_SUPER_PERIODIC_RESET_INTERVAL_MINUTES,
    DEFAULT_SUPER_PERIODIC_RESET_QUOTA,
    SUPER_POOL_NAME,
    get_token_manager,
)


class TokenRefreshScheduler:
    """Token 自动刷新调度器"""

    def __init__(self, interval_hours: int = 8):
        self.interval_hours = interval_hours
        self.interval_seconds = interval_hours * 3600
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def _get_loop_interval_seconds(self) -> int:
        interval_seconds = int(self.interval_seconds)
        reset_enabled = get_config("token.super_periodic_reset_enabled", True)
        if not reset_enabled:
            return max(60, interval_seconds)

        reset_minutes = get_config(
            "token.super_periodic_reset_interval_minutes",
            DEFAULT_SUPER_PERIODIC_RESET_INTERVAL_MINUTES,
        )
        try:
            reset_minutes = int(reset_minutes)
        except (TypeError, ValueError):
            reset_minutes = DEFAULT_SUPER_PERIODIC_RESET_INTERVAL_MINUTES
        reset_seconds = max(60, reset_minutes * 60)
        return max(60, min(interval_seconds, reset_seconds))

    async def _maybe_reset_super_pool_quota(self, storage) -> None:
        reset_enabled = get_config("token.super_periodic_reset_enabled", True)
        if not reset_enabled:
            return

        reset_minutes = get_config(
            "token.super_periodic_reset_interval_minutes",
            DEFAULT_SUPER_PERIODIC_RESET_INTERVAL_MINUTES,
        )
        reset_quota = get_config(
            "token.super_periodic_reset_quota",
            DEFAULT_SUPER_PERIODIC_RESET_QUOTA,
        )
        try:
            reset_minutes = int(reset_minutes)
        except (TypeError, ValueError):
            reset_minutes = DEFAULT_SUPER_PERIODIC_RESET_INTERVAL_MINUTES
        try:
            reset_quota = int(reset_quota)
        except (TypeError, ValueError):
            reset_quota = DEFAULT_SUPER_PERIODIC_RESET_QUOTA

        reset_seconds = max(60, reset_minutes * 60)
        slot = int(time.time() // reset_seconds)
        lock_name = f"token_super_periodic_reset:{slot}"

        try:
            async with storage.acquire_lock(lock_name, timeout=reset_seconds + 60):
                manager = await get_token_manager()
                result = await manager.reset_pool_quota(
                    SUPER_POOL_NAME,
                    reset_quota,
                )
                logger.info(
                    f"Scheduler: periodic {SUPER_POOL_NAME} quota reset completed - "
                    f"updated={result['updated']}, "
                    f"skipped={result['skipped']}, "
                    f"total={result['total']}, "
                    f"quota={reset_quota}, "
                    f"slot={slot}"
                )
        except StorageError:
            logger.info(
                f"Scheduler: periodic {SUPER_POOL_NAME} quota reset skipped "
                f"(slot={slot}, lock not acquired)"
            )

    async def _refresh_loop(self):
        """刷新循环"""
        logger.info(
            f"Scheduler: started (refresh interval: {self.interval_hours}h, "
            f"loop interval: {self._get_loop_interval_seconds()}s)"
        )

        while self._running:
            try:
                storage = get_storage()
                lock_acquired = False
                lock = None

                if isinstance(storage, RedisStorage):
                    # Redis: non-blocking lock to avoid multi-worker duplication
                    lock_key = "grok2api:lock:token_refresh"
                    lock = storage.redis.lock(
                        lock_key, timeout=self.interval_seconds + 60, blocking_timeout=0
                    )
                    lock_acquired = await lock.acquire(blocking=False)
                else:
                    try:
                        async with storage.acquire_lock("token_refresh", timeout=1):
                            lock_acquired = True
                    except StorageError:
                        lock_acquired = False

                if not lock_acquired:
                    logger.info("Scheduler: skipped (lock not acquired)")
                    await asyncio.sleep(self._get_loop_interval_seconds())
                    continue

                try:
                    logger.info("Scheduler: starting token refresh...")
                    manager = await get_token_manager()
                    result = await manager.refresh_cooling_tokens()

                    logger.info(
                        f"Scheduler: refresh completed - "
                        f"checked={result['checked']}, "
                        f"refreshed={result['refreshed']}, "
                        f"recovered={result['recovered']}, "
                        f"expired={result['expired']}"
                    )
                    await self._maybe_reset_super_pool_quota(storage)
                finally:
                    if lock is not None and lock_acquired:
                        try:
                            await lock.release()
                        except Exception:
                            pass

                await asyncio.sleep(self._get_loop_interval_seconds())

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scheduler: refresh error - {e}")
                await asyncio.sleep(self._get_loop_interval_seconds())

    def start(self):
        """启动调度器"""
        if self._running:
            logger.warning("Scheduler: already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._refresh_loop())
        logger.info("Scheduler: enabled")

    def stop(self):
        """停止调度器"""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("Scheduler: stopped")


# 全局单例
_scheduler: Optional[TokenRefreshScheduler] = None


def get_scheduler(interval_hours: int = 8) -> TokenRefreshScheduler:
    """获取调度器单例"""
    global _scheduler
    if _scheduler is None:
        _scheduler = TokenRefreshScheduler(interval_hours)
    return _scheduler


__all__ = ["TokenRefreshScheduler", "get_scheduler"]
