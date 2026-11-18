"""模拟上游服务：可产生慢请求、可被手动置为故障模式、可调整配置版本。

管理接口（仅用于测试/演练，不经过网关）：
  GET  /control/state            查看当前模式、在途请求数与配置版本
  POST /control/mode?mode=...    切换模式: healthy | slow | error | hang
  POST /control/config?version=N[&omit_version=bool]
                                 调整配置版本（可升可降，模拟发布进度不同
                                 与版本回退）；omit_version=true 时响应不再
                                 携带 X-Config-Version 头（模拟缺版本）

业务接口:
  GET  /config?delay=N            配置响应：JSON 体 + X-Config-Version 头
  GET  /slow?seconds=N            延迟 N 秒后返回（慢请求）
  GET  /echo                      回显请求信息
  GET  /flaky                     按当前模式返回 200 或 500
  GET  /                          健康信息
"""
from __future__ import annotations

import asyncio
import time

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

# 节点运行模式（演示用的全局状态）
_mode = "healthy"
_inflight = 0
_started = time.time()
_node_id = "unknown"
# 配置版本（模拟各副本发布进度）：可升可降；omit 时响应不带版本头
_config_version = 1
_config_omit_version = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    import os

    global _node_id
    _node_id = os.environ.get("NODE_ID", "unknown")
    yield


app = FastAPI(title="mock-upstream", version="1.0.0", lifespan=lifespan)


@app.get("/")
async def root() -> JSONResponse:
    """健康端点：如实反映节点模式。

    error -> 503（探测据此判定节点不健康）；hang -> 延迟（探测超时判定）；
    其余 -> 200。注意控制面状态请查 /control/state，该接口不受模式影响。
    """
    if _mode == "error":
        return JSONResponse(
            status_code=503,
            content={"node": _node_id, "mode": _mode, "inflight": _inflight,
                     "error": "upstream in error mode"},
        )
    if _mode == "hang":
        await asyncio.sleep(120)
    return JSONResponse(
        status_code=200,
        content={
            "node": _node_id,
            "mode": _mode,
            "inflight": _inflight,
            "uptime": round(time.time() - _started, 2),
        },
    )


@app.get("/echo")
async def echo(request: Request) -> dict:
    return {
        "node": _node_id,
        "path": "/echo",
        "query_params": dict(request.query_params),
        "headers": {k: v for k, v in request.headers.items()},
    }


@app.get("/slow")
async def slow(seconds: float = Query(2.0, ge=0, le=60)) -> JSONResponse:
    """慢请求：用于验证排空期间在途请求不被截断。"""
    global _inflight
    _inflight += 1
    started = time.time()
    try:
        await asyncio.sleep(seconds)
        if _mode == "error":
            return JSONResponse(
                status_code=500,
                content={"node": _node_id, "error": "upstream in error mode"},
            )
        return JSONResponse(
            status_code=200,
            content={
                "node": _node_id,
                "path": "/slow",
                "slept": round(time.time() - started, 3),
                "requested": seconds,
            },
        )
    finally:
        _inflight -= 1


@app.get("/flaky")
async def flaky() -> JSONResponse:
    """按模式返回：error -> 500；hang -> 挂起（由网关超时兜底）；其余 -> 200。"""
    if _mode == "error":
        return JSONResponse(
            status_code=500,
            content={"node": _node_id, "error": "upstream in error mode"},
        )
    if _mode == "hang":
        # 挂足够久，让网关的请求超时先触发
        await asyncio.sleep(120)
    return JSONResponse(status_code=200, content={"node": _node_id, "ok": True})


@app.get("/config")
async def config(delay: float = Query(0.0, ge=0, le=60)) -> JSONResponse:
    """配置响应：JSON 体 + X-Config-Version 整数头（可用 /control/config 调整）。

    delay 用于模拟慢配置响应（验证在途版本请求计入排空等待）；
    error -> 500；hang -> 挂起（由网关超时兜底）。
    """
    global _inflight
    if _mode == "hang":
        await asyncio.sleep(120)
    _inflight += 1
    started = time.time()
    try:
        if delay > 0:
            await asyncio.sleep(delay)
        if _mode == "error":
            return JSONResponse(
                status_code=500,
                content={"node": _node_id, "error": "upstream in error mode"},
            )
        headers = {}
        if not _config_omit_version:
            headers["X-Config-Version"] = str(_config_version)
        return JSONResponse(
            status_code=200,
            headers=headers,
            content={
                "node": _node_id,
                "path": "/config",
                "version": None if _config_omit_version else _config_version,
                "config": {
                    "feature_flags": {"monotonic_read": True},
                    "config_version": _config_version,
                },
                "delay": delay,
                "elapsed": round(time.time() - started, 3),
            },
        )
    finally:
        _inflight -= 1


@app.get("/control/state")
async def control_state() -> dict:
    return {
        "node": _node_id,
        "mode": _mode,
        "inflight": _inflight,
        "config_version": _config_version,
        "config_omit_version": _config_omit_version,
    }


@app.post("/control/mode")
async def set_mode(mode: str = Query(..., pattern="^(healthy|slow|error|hang)$")) -> dict:
    global _mode
    old = _mode
    _mode = mode
    return {"node": _node_id, "previous_mode": old, "mode": _mode}


@app.post("/control/config")
async def set_config(
    version: int = Query(..., ge=0),
    omit_version: Optional[bool] = Query(None),
) -> dict:
    """调整配置版本（允许回退到更低版本，模拟发布进度不一致）。

    omit_version 为 None 时保持现状；为 true 时 /config 响应不再携带
    X-Config-Version 头，用于演练网关对缺版本响应的拒绝逻辑。
    """
    global _config_version, _config_omit_version
    old = _config_version
    _config_version = version
    if omit_version is not None:
        _config_omit_version = omit_version
    return {
        "node": _node_id,
        "previous_version": old,
        "config_version": _config_version,
        "config_omit_version": _config_omit_version,
    }
