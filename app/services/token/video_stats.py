"""Video success metrics for tokens."""

from __future__ import annotations

import os
import time
from typing import Dict, Iterable, Optional

from app.core.config import get_config
from app.core.logger import logger

try:
    from redis import asyncio as aioredis
except ImportError:  # pragma: no cover - runtime dependency guard
    aioredis = None


VIDEO_STATS_WINDOW_SECONDS = 24 * 3600
VIDEO_STATS_KEY_PREFIX = "grok2api:video_success"


class VideoStatsService:
    """Tracks per-token successful video generations in the last 24 hours."""

    _client = None
    _client_url: Optional[str] = None
    _warned_missing_driver = False

    @classmethod
    def _resolve_redis_url(cls) -> str:
        env_url = os.getenv("TOKEN_VIDEO_STATS_REDIS_URL", "").strip()
        if env_url:
            return env_url
        return str(get_config("token.video_stats_redis_url", "") or "").strip()

    @classmethod
    async def _get_client(cls):
        redis_url = cls._resolve_redis_url()
        if not redis_url:
            return None
        if aioredis is None:
            if not cls._warned_missing_driver:
                cls._warned_missing_driver = True
                logger.warning(
                    "VideoStatsService disabled: redis package is not installed"
                )
            return None
        if cls._client is not None and cls._client_url == redis_url:
            return cls._client
        cls._client = aioredis.from_url(
            redis_url,
            decode_responses=True,
            health_check_interval=30,
        )
        cls._client_url = redis_url
        return cls._client

    @staticmethod
    def _normalize_token(token: str) -> str:
        raw = str(token or "").strip()
        if raw.startswith("sso="):
            raw = raw[4:]
        return raw

    @classmethod
    def _key_for_token(cls, token: str) -> str:
        return f"{VIDEO_STATS_KEY_PREFIX}:{cls._normalize_token(token)}"

    @classmethod
    async def record_success(cls, token: str) -> None:
        raw_token = cls._normalize_token(token)
        if not raw_token:
            return
        client = await cls._get_client()
        if client is None:
            return

        now = int(time.time())
        key = cls._key_for_token(raw_token)
        cutoff = now - VIDEO_STATS_WINDOW_SECONDS
        member = f"{now}:{time.time_ns()}"

        try:
            async with client.pipeline(transaction=True) as pipe:
                pipe.zadd(key, {member: now})
                pipe.zremrangebyscore(key, 0, cutoff)
                pipe.expire(key, VIDEO_STATS_WINDOW_SECONDS * 2)
                await pipe.execute()
        except Exception as e:
            logger.warning(f"VideoStatsService record failed: {e}")

    @classmethod
    async def get_success_counts(
        cls, tokens: Iterable[str]
    ) -> Dict[str, int]:
        normalized = []
        seen = set()
        for token in tokens:
            raw = cls._normalize_token(token)
            if raw and raw not in seen:
                seen.add(raw)
                normalized.append(raw)

        if not normalized:
            return {}

        client = await cls._get_client()
        if client is None:
            return {token: 0 for token in normalized}

        now = int(time.time())
        cutoff = now - VIDEO_STATS_WINDOW_SECONDS

        try:
            async with client.pipeline(transaction=False) as pipe:
                for token in normalized:
                    pipe.zcount(cls._key_for_token(token), cutoff, "+inf")
                counts = await pipe.execute()
            result: Dict[str, int] = {}
            for token, count in zip(normalized, counts):
                try:
                    result[token] = int(count or 0)
                except (TypeError, ValueError):
                    result[token] = 0
            return result
        except Exception as e:
            logger.warning(f"VideoStatsService query failed: {e}")
            return {token: 0 for token in normalized}

    @classmethod
    async def close(cls):
        client = cls._client
        cls._client = None
        cls._client_url = None
        if client is not None:
            try:
                close_method = getattr(client, "aclose", None) or getattr(
                    client, "close", None
                )
                if close_method is not None:
                    result = close_method()
                    if result is not None:
                        await result
            except Exception:
                pass
