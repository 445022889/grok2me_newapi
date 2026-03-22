import asyncio
from copy import deepcopy

import orjson
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.core.auth import get_app_key, verify_app_key
from app.core.batch import create_task, expire_task, get_task
from app.core.config import config, get_config
from app.core.logger import logger
from app.core.storage import get_storage
from app.services.grok.batch_services.usage import UsageService
from app.services.grok.batch_services.nsfw import NSFWService
from app.services.token.manager import (
    DEFAULT_SUPER_PERIODIC_RESET_INTERVAL_MINUTES,
    DEFAULT_SUPER_PERIODIC_RESET_QUOTA,
    SUPER_POOL_NAME,
    get_token_manager,
)
from app.services.token.video_stats import VideoStatsService

router = APIRouter()


def _normalize_token_value(value) -> str:
    token = str(value or "").strip()
    if token.startswith("sso="):
        token = token[4:]
    return token


def _iter_token_items(tokens_data: dict):
    for pool_name, tokens in (tokens_data or {}).items():
        if not isinstance(tokens, list):
            continue
        for item in tokens:
            if isinstance(item, str):
                token_data = {"token": _normalize_token_value(item)}
            elif isinstance(item, dict):
                token_data = dict(item)
                token_data["token"] = _normalize_token_value(token_data.get("token"))
            else:
                continue
            if token_data.get("token"):
                yield pool_name, token_data


def _augment_tokens_with_video_stats(tokens_data: dict, counts: dict[str, int]) -> dict:
    for pool_name, tokens in (tokens_data or {}).items():
        if not isinstance(tokens, list):
            continue
        for index, item in enumerate(tokens):
            if isinstance(item, str):
                token = _normalize_token_value(item)
                tokens[index] = {
                    "token": token,
                    "status": "active",
                    "quota": 0,
                    "note": "",
                    "use_count": 0,
                    "tags": [],
                    "video_success_24h": counts.get(token, 0),
                }
            elif isinstance(item, dict):
                token = _normalize_token_value(item.get("token"))
                item["token"] = token
                item["video_success_24h"] = counts.get(token, 0)
    return tokens_data


def _build_existing_map(tokens_data: dict) -> dict[str, dict[str, dict]]:
    existing_map: dict[str, dict[str, dict]] = {}
    for pool_name, token_data in _iter_token_items(tokens_data):
        existing_map.setdefault(pool_name, {})[token_data["token"]] = token_data
    return existing_map


def _token_settings_payload() -> dict:
    return {
        "super_periodic_reset_enabled": bool(
            get_config("token.super_periodic_reset_enabled", True)
        ),
        "super_periodic_reset_interval_minutes": int(
            get_config(
                "token.super_periodic_reset_interval_minutes",
                DEFAULT_SUPER_PERIODIC_RESET_INTERVAL_MINUTES,
            )
            or DEFAULT_SUPER_PERIODIC_RESET_INTERVAL_MINUTES
        ),
        "super_periodic_reset_quota": int(
            get_config(
                "token.super_periodic_reset_quota",
                DEFAULT_SUPER_PERIODIC_RESET_QUOTA,
            )
            or DEFAULT_SUPER_PERIODIC_RESET_QUOTA
        ),
        "video_stats_redis_url": str(
            get_config("token.video_stats_redis_url", "") or ""
        ),
    }


@router.get("/tokens", dependencies=[Depends(verify_app_key)])
async def get_tokens():
    """获取所有 Token"""
    storage = get_storage()
    tokens = await storage.load_tokens()
    tokens = deepcopy(tokens or {})

    raw_tokens = []
    for _, token_data in _iter_token_items(tokens):
        raw_tokens.append(token_data["token"])

    counts = await VideoStatsService.get_success_counts(raw_tokens)
    return _augment_tokens_with_video_stats(tokens, counts)


@router.get("/tokens/settings", dependencies=[Depends(verify_app_key)])
async def get_token_settings():
    """获取 token 管理页相关设置。"""
    return {"status": "success", "settings": _token_settings_payload()}


@router.post("/tokens/settings", dependencies=[Depends(verify_app_key)])
async def update_token_settings(data: dict):
    """更新 token 管理页相关设置。"""
    try:
        enabled = bool(data.get("super_periodic_reset_enabled", True))
        interval = int(
            data.get(
                "super_periodic_reset_interval_minutes",
                DEFAULT_SUPER_PERIODIC_RESET_INTERVAL_MINUTES,
            )
        )
        quota = int(
            data.get(
                "super_periodic_reset_quota",
                DEFAULT_SUPER_PERIODIC_RESET_QUOTA,
            )
        )
        redis_url = str(data.get("video_stats_redis_url", "") or "").strip()

        if interval < 1:
            raise HTTPException(status_code=400, detail="Interval must be >= 1 minute")
        if quota < 0:
            raise HTTPException(status_code=400, detail="Quota must be >= 0")

        await config.update(
            {
                "token": {
                    "super_periodic_reset_enabled": enabled,
                    "super_periodic_reset_interval_minutes": interval,
                    "super_periodic_reset_quota": quota,
                    "video_stats_redis_url": redis_url,
                }
            }
        )
        await VideoStatsService.close()
        return {"status": "success", "settings": _token_settings_payload()}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tokens/super-reset", dependencies=[Depends(verify_app_key)])
async def reset_super_quota_now():
    """立即重置 ssoSuper 池额度。"""
    try:
        mgr = await get_token_manager()
        quota = int(
            get_config(
                "token.super_periodic_reset_quota",
                DEFAULT_SUPER_PERIODIC_RESET_QUOTA,
            )
            or DEFAULT_SUPER_PERIODIC_RESET_QUOTA
        )
        result = await mgr.reset_pool_quota(SUPER_POOL_NAME, quota)
        return {"status": "success", "quota": quota, "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tokens", dependencies=[Depends(verify_app_key)])
async def update_tokens(data: dict):
    """更新 Token 信息"""
    storage = get_storage()
    try:
        from app.services.token.models import TokenInfo

        async with storage.acquire_lock("tokens_save", timeout=10):
            existing = await storage.load_tokens() or {}
            normalized = {}
            allowed_fields = set(TokenInfo.model_fields.keys())
            existing_map = _build_existing_map(existing)
            for pool_name, tokens in (data or {}).items():
                if not isinstance(tokens, list):
                    continue
                pool_list = []
                for item in tokens:
                    if isinstance(item, str):
                        token_data = {"token": _normalize_token_value(item)}
                    elif isinstance(item, dict):
                        token_data = dict(item)
                        token_data["token"] = _normalize_token_value(
                            token_data.get("token")
                        )
                    else:
                        continue

                    base = existing_map.get(pool_name, {}).get(
                        token_data.get("token"), {}
                    )
                    merged = dict(base)
                    merged.update(token_data)
                    if merged.get("tags") is None:
                        merged["tags"] = []

                    filtered = {k: v for k, v in merged.items() if k in allowed_fields}
                    try:
                        info = TokenInfo(**filtered)
                        pool_list.append(info.model_dump())
                    except Exception as e:
                        logger.warning(f"Skip invalid token in pool '{pool_name}': {e}")
                        continue
                normalized[pool_name] = pool_list

            await storage.save_tokens(normalized)
            mgr = await get_token_manager()
            await mgr.reload()
        return {"status": "success", "message": "Token 已更新"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tokens/import", dependencies=[Depends(verify_app_key)])
async def import_tokens(data: dict):
    """批量导入 token，避免整表回写造成页面卡顿。"""
    storage = get_storage()
    try:
        from app.services.token.models import TokenInfo

        pool_name = str(data.get("pool_name") or "ssoBasic").strip() or "ssoBasic"
        tokens = data.get("tokens")
        if not isinstance(tokens, list):
            raise HTTPException(status_code=400, detail="tokens must be a list")

        quota = data.get("quota")
        quota = int(quota) if quota is not None else None
        note = str(data.get("note") or "").strip()[:50]

        normalized_inputs = []
        seen = set()
        for value in tokens:
            token = _normalize_token_value(value)
            if token and token not in seen:
                seen.add(token)
                normalized_inputs.append(token)

        if not normalized_inputs:
            raise HTTPException(status_code=400, detail="No valid tokens provided")

        allowed_fields = set(TokenInfo.model_fields.keys())

        async with storage.acquire_lock("tokens_save", timeout=20):
            existing = await storage.load_tokens() or {}
            existing_map = _build_existing_map(existing)
            global_existing = {
                token
                for pool_tokens in existing_map.values()
                for token in pool_tokens.keys()
            }

            pool_tokens = list(existing.get(pool_name) or [])
            imported = 0
            skipped = 0

            for token in normalized_inputs:
                if token in global_existing:
                    skipped += 1
                    continue
                payload = {"token": token}
                if quota is not None:
                    payload["quota"] = quota
                if note:
                    payload["note"] = note
                payload = {k: v for k, v in payload.items() if k in allowed_fields}
                info = TokenInfo(**payload)
                pool_tokens.append(info.model_dump())
                global_existing.add(token)
                imported += 1

            existing[pool_name] = pool_tokens
            await storage.save_tokens(existing)
            mgr = await get_token_manager()
            await mgr.reload()

        return {
            "status": "success",
            "summary": {
                "requested": len(normalized_inputs),
                "imported": imported,
                "skipped": skipped,
                "pool_name": pool_name,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tokens/refresh", dependencies=[Depends(verify_app_key)])
async def refresh_tokens(data: dict):
    """刷新 Token 状态"""
    try:
        mgr = await get_token_manager()
        tokens = []
        if isinstance(data.get("token"), str) and data["token"].strip():
            tokens.append(data["token"].strip())
        if isinstance(data.get("tokens"), list):
            tokens.extend([str(t).strip() for t in data["tokens"] if str(t).strip()])

        if not tokens:
            raise HTTPException(status_code=400, detail="No tokens provided")

        unique_tokens = list(dict.fromkeys(tokens))

        raw_results = await UsageService.batch(
            unique_tokens,
            mgr,
        )

        results = {}
        for token, res in raw_results.items():
            if res.get("ok"):
                results[token] = res.get("data", False)
            else:
                results[token] = False

        response = {"status": "success", "results": results}
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tokens/refresh/async", dependencies=[Depends(verify_app_key)])
async def refresh_tokens_async(data: dict):
    """刷新 Token 状态（异步批量 + SSE 进度）"""
    mgr = await get_token_manager()
    tokens = []
    if isinstance(data.get("token"), str) and data["token"].strip():
        tokens.append(data["token"].strip())
    if isinstance(data.get("tokens"), list):
        tokens.extend([str(t).strip() for t in data["tokens"] if str(t).strip()])

    if not tokens:
        raise HTTPException(status_code=400, detail="No tokens provided")

    unique_tokens = list(dict.fromkeys(tokens))

    task = create_task(len(unique_tokens))

    async def _run():
        try:

            async def _on_item(item: str, res: dict):
                task.record(bool(res.get("ok")))

            raw_results = await UsageService.batch(
                unique_tokens,
                mgr,
                on_item=_on_item,
                should_cancel=lambda: task.cancelled,
            )

            if task.cancelled:
                task.finish_cancelled()
                return

            results: dict[str, bool] = {}
            ok_count = 0
            fail_count = 0
            for token, res in raw_results.items():
                if res.get("ok") and res.get("data") is True:
                    ok_count += 1
                    results[token] = True
                else:
                    fail_count += 1
                    results[token] = False

            await mgr._save(force=True)

            result = {
                "status": "success",
                "summary": {
                    "total": len(unique_tokens),
                    "ok": ok_count,
                    "fail": fail_count,
                },
                "results": results,
            }
            task.finish(result)
        except Exception as e:
            task.fail_task(str(e))
        finally:
            import asyncio
            asyncio.create_task(expire_task(task.id, 300))

    import asyncio
    asyncio.create_task(_run())

    return {
        "status": "success",
        "task_id": task.id,
        "total": len(unique_tokens),
    }


@router.get("/batch/{task_id}/stream")
async def batch_stream(task_id: str, request: Request):
    app_key = get_app_key()
    if app_key:
        key = request.query_params.get("app_key")
        if key != app_key:
            raise HTTPException(status_code=401, detail="Invalid authentication token")
    task = get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    async def event_stream():
        queue = task.attach()
        try:
            yield f"data: {orjson.dumps({'type': 'snapshot', **task.snapshot()}).decode()}\n\n"

            final = task.final_event()
            if final:
                yield f"data: {orjson.dumps(final).decode()}\n\n"
                return

            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
                    final = task.final_event()
                    if final:
                        yield f"data: {orjson.dumps(final).decode()}\n\n"
                        return
                    continue

                yield f"data: {orjson.dumps(event).decode()}\n\n"
                if event.get("type") in ("done", "error", "cancelled"):
                    return
        finally:
            task.detach(queue)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/batch/{task_id}/cancel", dependencies=[Depends(verify_app_key)])
async def batch_cancel(task_id: str):
    task = get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    task.cancel()
    return {"status": "success"}


@router.post("/tokens/nsfw/enable", dependencies=[Depends(verify_app_key)])
async def enable_nsfw(data: dict):
    """批量开启 NSFW (Unhinged) 模式"""
    try:
        mgr = await get_token_manager()

        tokens = []
        if isinstance(data.get("token"), str) and data["token"].strip():
            tokens.append(data["token"].strip())
        if isinstance(data.get("tokens"), list):
            tokens.extend([str(t).strip() for t in data["tokens"] if str(t).strip()])

        if not tokens:
            for pool_name, pool in mgr.pools.items():
                for info in pool.list():
                    raw = (
                        info.token[4:] if info.token.startswith("sso=") else info.token
                    )
                    tokens.append(raw)

        if not tokens:
            raise HTTPException(status_code=400, detail="No tokens available")

        unique_tokens = list(dict.fromkeys(tokens))

        raw_results = await NSFWService.batch(
            unique_tokens,
            mgr,
        )

        results = {}
        ok_count = 0
        fail_count = 0

        for token, res in raw_results.items():
            masked = f"{token[:8]}...{token[-8:]}" if len(token) > 20 else token
            if res.get("ok") and res.get("data", {}).get("success"):
                ok_count += 1
                results[masked] = res.get("data", {})
            else:
                fail_count += 1
                results[masked] = res.get("data") or {"error": res.get("error")}

        response = {
            "status": "success",
            "summary": {
                "total": len(unique_tokens),
                "ok": ok_count,
                "fail": fail_count,
            },
            "results": results,
        }

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Enable NSFW failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tokens/nsfw/enable/async", dependencies=[Depends(verify_app_key)])
async def enable_nsfw_async(data: dict):
    """批量开启 NSFW (Unhinged) 模式（异步批量 + SSE 进度）"""
    mgr = await get_token_manager()

    tokens = []
    if isinstance(data.get("token"), str) and data["token"].strip():
        tokens.append(data["token"].strip())
    if isinstance(data.get("tokens"), list):
        tokens.extend([str(t).strip() for t in data["tokens"] if str(t).strip()])

    if not tokens:
        for pool_name, pool in mgr.pools.items():
            for info in pool.list():
                raw = info.token[4:] if info.token.startswith("sso=") else info.token
                tokens.append(raw)

    if not tokens:
        raise HTTPException(status_code=400, detail="No tokens available")

    unique_tokens = list(dict.fromkeys(tokens))

    task = create_task(len(unique_tokens))

    async def _run():
        try:

            async def _on_item(item: str, res: dict):
                ok = bool(res.get("ok") and res.get("data", {}).get("success"))
                task.record(ok)

            raw_results = await NSFWService.batch(
                unique_tokens,
                mgr,
                on_item=_on_item,
                should_cancel=lambda: task.cancelled,
            )

            if task.cancelled:
                task.finish_cancelled()
                return

            results = {}
            ok_count = 0
            fail_count = 0
            for token, res in raw_results.items():
                masked = f"{token[:8]}...{token[-8:]}" if len(token) > 20 else token
                if res.get("ok") and res.get("data", {}).get("success"):
                    ok_count += 1
                    results[masked] = res.get("data", {})
                else:
                    fail_count += 1
                    results[masked] = res.get("data") or {"error": res.get("error")}

            await mgr._save(force=True)

            result = {
                "status": "success",
                "summary": {
                    "total": len(unique_tokens),
                    "ok": ok_count,
                    "fail": fail_count,
                },
                "results": results,
            }
            task.finish(result)
        except Exception as e:
            task.fail_task(str(e))
        finally:
            import asyncio
            asyncio.create_task(expire_task(task.id, 300))

    import asyncio
    asyncio.create_task(_run())

    return {
        "status": "success",
        "task_id": task.id,
        "total": len(unique_tokens),
    }
