"""HTTP 入口网关（FastAPI + httpx）。

职责：
  * 仅转发 GET 请求到上游，按轮询（round-robin）选择「健康且未排空」的节点；
  * 周期性主动探测上游，探测结论只影响健康标志，绝不改动人工排空标志；
  * 提供排空 / 恢复 / 状态管理 API；所有节点都不可接流时返回 503；
  * 排空期间新请求不再进入该节点，已在途的请求继续等待原响应返回；
  * 排空 API 阻塞到在途请求全部结束后才报告 drained=true；
  * 超时 / 连接错误 / 5xx 不虚报成功，并同步刷新健康状态，在途计数靠 try/finally 保证准确。

配置单调读（GET /proxy/config 可选）：
  * 上游配置响应带整数 X-Config-Version；终端下次 GET 可带 X-Min-Version；
  * 网关通过周期版本探测 + 实际响应观察跟踪各副本的配置版本，
    只让「已确认达到门槛且健康、未排空」的节点接流；
  * 无合格节点时返回 503 config_min_version_unavailable（暂不可读），绝不交付旧配置；
  * 节点版本可能在选路后回退：实际响应低于门槛或缺版本头同样拒绝交付，
    并立即把该节点的版本记录更新为实际观测值，作为后续选路依据；
  * 版本落后不算节点故障（不降级健康位），副本赶上后经周期版本探测自动恢复候选；
  * 版本探测绝不向排空节点发新请求；已接流的版本请求仍计入排空等待；
  * 不带 X-Min-Version 的 GET 保持原行为（仅被动记录观测到的版本）。
"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

import httpx
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse, Response

# ----------------------------- 配置 -----------------------------

UPSTREAMS_RAW = os.environ.get(
    "UPSTREAMS",
    "node1=http://upstream1:8000,node2=http://upstream2:8000",
)
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "2.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "1.5"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "15.0"))
DRAIN_DEFAULT_TIMEOUT = float(os.environ.get("DRAIN_DEFAULT_TIMEOUT", "30.0"))

# 透传响应时需要剥离的逐跳（hop-by-hop）头
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}


def _parse_upstreams(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, url = item.partition("=")
        if not _:
            raise ValueError(f"非法 UPSTREAMS 条目: {item!r}，应为 name=url")
        out.append((name.strip(), url.strip().rstrip("/")))
    if not out:
        raise ValueError("至少需要配置一个上游节点")
    return out


def _parse_config_version(raw: Optional[str]) -> Optional[int]:
    """解析 X-Config-Version 头；缺失或不是整数时返回 None（视为版本未知）。"""
    if raw is None:
        return None
    try:
        return int(raw.strip())
    except (ValueError, AttributeError):
        return None


# ----------------------------- 节点状态 -----------------------------

@dataclass
class NodeState:
    name: str
    base_url: str
    # 健康标志：只由探测任务（以及请求失败时的降级）写入
    healthy: bool = False
    # 排空标志：只由人工 drain/resume 写入，探测永不触碰
    draining: bool = False
    inflight: int = 0
    rr_hits: int = 0
    success_count: int = 0
    failure_count: int = 0
    # 最近一次确认的配置版本（来自版本探测或实际响应观测）；None 表示未知
    config_version: Optional[int] = None
    config_version_at: Optional[float] = None
    # 选路后因版本不足/缺版本被拒绝交付的次数（落后不算故障，单独计数）
    stale_count: int = 0
    last_probe_at: Optional[float] = None
    last_probe_ok: Optional[bool] = None
    last_probe_detail: str = "尚未探测"
    last_change_at: float = field(default_factory=time.time)

    def can_serve(self) -> bool:
        return self.healthy and not self.draining

    def version_qualified(self, min_version: Optional[int]) -> bool:
        """是否已确认达到版本门槛。不带门槛的请求不做版本限制。"""
        if min_version is None:
            return True
        return self.config_version is not None and self.config_version >= min_version

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "base_url": self.base_url,
            "healthy": self.healthy,
            "draining": self.draining,
            "eligible": self.can_serve(),
            "inflight": self.inflight,
            "rr_hits": self.rr_hits,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "config_version": self.config_version,
            "config_version_at": self.config_version_at,
            "stale_count": self.stale_count,
            "last_probe_at": self.last_probe_at,
            "last_probe_ok": self.last_probe_ok,
            "last_probe_detail": self.last_probe_detail,
        }


class Gateway:
    def __init__(self, nodes: list[NodeState]) -> None:
        self.nodes = nodes
        self._by_name = {n.name: n for n in nodes}
        # 一把条件变量保护所有节点状态；请求结束 / 排空等待都在其上通知
        self.cond = asyncio.Condition()
        self._rr = 0
        self.client: Optional[httpx.AsyncClient] = None
        self._probe_task: Optional[asyncio.Task] = None

    def get(self, name: str) -> Optional[NodeState]:
        return self._by_name.get(name)

    async def pick_node(
        self,
        min_version: Optional[int] = None,
        exclude: frozenset = frozenset(),
    ) -> Optional[NodeState]:
        """轮询选取可接流的节点，并立即登记在途计数。

        min_version 不为 None 时，只考虑已确认达到该配置版本的节点；
        exclude 中的节点（本次请求已试过并被拒的）直接跳过。
        """
        async with self.cond:
            n = len(self.nodes)
            for offset in range(n):
                idx = (self._rr + offset) % n
                node = self.nodes[idx]
                if node.name in exclude or not node.can_serve():
                    continue
                if not node.version_qualified(min_version):
                    continue
                self._rr = (idx + 1) % n
                node.inflight += 1
                node.rr_hits += 1
                return node
            return None

    async def release_node(self, node: NodeState, ok: bool) -> None:
        async with self.cond:
            node.inflight -= 1
            if ok:
                node.success_count += 1
            else:
                node.failure_count += 1
            # 在途归零可能让 drain 等待者满足条件
            self.cond.notify_all()

    async def mark_unhealthy(self, node: NodeState, detail: str) -> None:
        """请求失败时的即时降级；只改健康标志，不动 draining。"""
        async with self.cond:
            node.healthy = False
            node.last_probe_ok = False
            node.last_probe_detail = f"请求失败降级: {detail}"
            node.last_probe_at = time.time()
            node.last_change_at = time.time()

    async def note_config_version(
        self, node: NodeState, version: Optional[int], stale_reject: bool = False
    ) -> None:
        """把实际观测到的配置版本记为后续选路依据。

        版本可能回退，因此记录始终跟随最新观测值（可升可降）；
        stale_reject=True 表示本次响应因版本不足/缺版本被拒绝交付，
        只累计 stale_count，绝不动健康标志（落后不是故障）。
        """
        async with self.cond:
            node.config_version = version
            node.config_version_at = time.time()
            if stale_reject:
                node.stale_count += 1
            node.last_change_at = time.time()

    async def probe_config_version(self, node: NodeState) -> None:
        """版本探测：只打未排空节点；结果只影响版本记录，不动健康/排空标志。"""
        assert self.client is not None
        async with self.cond:
            if node.draining:
                # 版本检查绝不向排空节点发新请求
                return
        try:
            resp = await self.client.get(
                f"{node.base_url}/config", timeout=PROBE_TIMEOUT
            )
        except httpx.RequestError:
            # 连接/超时失败由健康探测定性，版本记录保持不变
            return
        if resp.status_code >= 500:
            return
        version = _parse_config_version(resp.headers.get("x-config-version"))
        async with self.cond:
            node.config_version = version
            node.config_version_at = time.time()

    async def probe_one(self, node: NodeState) -> None:
        """对单个节点做主动探测。成功只置 healthy=True，绝不解除 draining。"""
        assert self.client is not None
        try:
            resp = await self.client.get(
                f"{node.base_url}/", timeout=PROBE_TIMEOUT
            )
            ok = resp.status_code < 500
            detail = f"HTTP {resp.status_code}"
        except httpx.TimeoutException as exc:
            ok, detail = False, f"探测超时: {type(exc).__name__}"
        except httpx.RequestError as exc:
            ok, detail = False, f"探测连接错误: {type(exc).__name__}: {str(exc)[:100]}"

        async with self.cond:
            node.healthy = ok
            # 注意：这里刻意不修改 node.draining
            node.last_probe_at = time.time()
            node.last_probe_ok = ok
            node.last_probe_detail = detail
            node.last_change_at = time.time()
            draining = node.draining

        # 健康且未排空的节点才做版本探测（排空节点不发版本检查请求）
        if ok and not draining:
            await self.probe_config_version(node)

    async def probe_all(self) -> None:
        await asyncio.gather(*(self.probe_one(n) for n in self.nodes))

    async def _probe_loop(self) -> None:
        while True:
            try:
                await self.probe_all()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 探测循环本身不能崩
                print(f"[probe-loop] unexpected error: {exc!r}")
            await asyncio.sleep(PROBE_INTERVAL)

    async def drain(
        self, node: NodeState, timeout: Optional[float]
    ) -> tuple[bool, int]:
        """置排空标志并等待在途清零。返回 (是否排空完成, 剩余在途数)。"""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + max(timeout, 0.0)
        async with self.cond:
            node.draining = True
            node.last_change_at = time.time()
            while node.inflight > 0:
                if deadline is None:
                    await self.cond.wait()
                    continue
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False, node.inflight
                # asyncio.Condition.wait 不支持 timeout，用 wait_for 限时等待
                try:
                    await asyncio.wait_for(self.cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass  # 回到循环重新计算剩余时间 / 在途数
            return True, 0

    async def resume(self, node: NodeState) -> None:
        """恢复接流：先清排空并保守置为不健康，再立即探测，由探测决定能否接流。"""
        async with self.cond:
            node.draining = False
            # 恢复不绕过故障探测：探测结论出来前一律不接流，杜绝状态窗口
            node.healthy = False
            node.last_probe_ok = None
            node.last_probe_detail = "恢复接流，等待首次探测确认"
            node.last_change_at = time.time()
        await self.probe_one(node)


# ----------------------------- 应用生命周期 -----------------------------

gateway = Gateway([NodeState(name, url) for name, url in _parse_upstreams(UPSTREAMS_RAW)])


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as client:
        gateway.client = client
        # 启动先做一轮探测，避免把流量打到未知状态的节点
        await gateway.probe_all()
        gateway._probe_task = asyncio.create_task(gateway._probe_loop())
        try:
            yield
        finally:
            if gateway._probe_task:
                gateway._probe_task.cancel()
                try:
                    await gateway._probe_task
                except asyncio.CancelledError:
                    pass


app = FastAPI(
    title="http-entry-gateway",
    version="1.1.0",
    description=(
        "仅转发 GET 的入口网关：轮询健康且未排空的上游，支持排空/恢复/状态管理，"
        "无可接流节点时返回 503。GET /proxy/config 支持可选单调读："
        "携带 X-Min-Version 时只交付已确认达到该版本的配置，"
        "无合格节点返回 503 config_min_version_unavailable，绝不交付旧版本。"
    ),
    lifespan=lifespan,
)


# ----------------------------- 转发接口 -----------------------------

@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": "http-entry-gateway",
        "proxy": "GET /proxy/{path} 仅支持 GET；/proxy/config 可带 X-Min-Version 做单调读",
        "admin": [
            "GET  /admin/status",
            "POST /admin/nodes/{name}/drain?timeout=",
            "POST /admin/nodes/{name}/resume",
            "POST /admin/probe",
        ],
    }


def _passthrough(upstream_resp: httpx.Response, node: NodeState) -> Response:
    """如实透传上游响应（剥离逐跳头），附加网关标记头。"""
    headers = {
        k: v
        for k, v in upstream_resp.headers.items()
        if k.lower() not in _HOP_BY_HOP
    }
    headers["X-Gateway"] = "http-entry-gateway"
    headers["X-Upstream-Node"] = node.name
    return Response(
        content=upstream_resp.content,
        status_code=upstream_resp.status_code,
        headers=headers,
        media_type=upstream_resp.headers.get("content-type"),
    )


async def _min_version_unavailable(min_version: int) -> JSONResponse:
    """无已确认达到门槛的节点：明确告知暂不可读，绝不交付旧配置。"""
    async with gateway.cond:
        nodes = {
            n.name: {
                "healthy": n.healthy,
                "draining": n.draining,
                "config_version": n.config_version,
            }
            for n in gateway.nodes
        }
    return JSONResponse(
        status_code=503,
        headers={"Retry-After": "1"},
        content={
            "error": "config_min_version_unavailable",
            "detail": (
                f"暂无已确认达到 X-Min-Version={min_version} 且健康、未排空的节点，"
                "配置暂不可读；未交付任何旧版本配置，请稍后重试"
            ),
            "min_version": min_version,
            "nodes": nodes,
        },
    )


@app.get("/proxy/{path:path}", tags=["proxy"])
async def proxy(
    path: str,
    request: Request,
    x_min_version: Optional[int] = Header(
        default=None,
        description=(
            "仅对 /proxy/config 生效的单调读门槛：只交付已确认达到该整数版本的配置；"
            "不满足时返回 503 config_min_version_unavailable 而非旧配置"
        ),
    ),
) -> Response:
    if path == "config" and x_min_version is not None:
        return await proxy_config_monotonic(request, x_min_version)
    return await proxy_simple(path, request)


async def proxy_simple(path: str, request: Request) -> Response:
    """常规转发：轮询健康且未排空的节点。不带版本门槛的 GET 保持原行为。"""
    node = await gateway.pick_node()
    if node is None:
        return JSONResponse(
            status_code=503,
            content={
                "error": "no_eligible_upstream",
                "detail": "当前没有健康且未排空的上游节点可接流",
            },
        )

    target_url = f"{node.base_url}/{path}"
    ok = False
    try:
        try:
            upstream_resp = await gateway.client.request(
                "GET",
                target_url,
                params=request.query_params,
                timeout=REQUEST_TIMEOUT,
            )
        except httpx.TimeoutException as exc:
            await gateway.mark_unhealthy(node, f"请求超时 {type(exc).__name__}")
            return JSONResponse(
                status_code=504,
                content={"error": "upstream_timeout", "node": node.name},
            )
        except httpx.RequestError as exc:
            await gateway.mark_unhealthy(
                node, f"{type(exc).__name__}: {str(exc)[:100]}"
            )
            return JSONResponse(
                status_code=502,
                content={"error": "upstream_unreachable", "node": node.name},
            )

        # 5xx 视为上游故障：如实透传状态码并降级，绝不虚报成功
        if upstream_resp.status_code >= 500:
            await gateway.mark_unhealthy(
                node, f"上游返回 HTTP {upstream_resp.status_code}"
            )
        elif path == "config":
            # 不带门槛的配置请求：行为不变，仅被动记录观测到的版本
            await gateway.note_config_version(
                node,
                _parse_config_version(upstream_resp.headers.get("x-config-version")),
            )

        ok = upstream_resp.status_code < 500
        return _passthrough(upstream_resp, node)
    finally:
        # 无论成功、失败还是取消，在途计数都必须准确归还
        await gateway.release_node(node, ok)


async def proxy_config_monotonic(request: Request, min_version: int) -> Response:
    """带 X-Min-Version 的配置单调读。

    只让已确认达到门槛且健康、未排空的节点接流；选路后若节点版本回退
    （实际响应低于门槛或缺版本头），拒绝交付、更新选路依据并改投其他
    合格节点；全部不合格时返回 503 config_min_version_unavailable。
    """
    tried: set[str] = set()
    while True:
        node = await gateway.pick_node(
            min_version=min_version, exclude=frozenset(tried)
        )
        if node is None:
            return await _min_version_unavailable(min_version)

        ok = False
        try:
            try:
                upstream_resp = await gateway.client.request(
                    "GET",
                    f"{node.base_url}/config",
                    params=request.query_params,
                    timeout=REQUEST_TIMEOUT,
                )
            except httpx.TimeoutException as exc:
                await gateway.mark_unhealthy(node, f"请求超时 {type(exc).__name__}")
                return JSONResponse(
                    status_code=504,
                    content={"error": "upstream_timeout", "node": node.name},
                )
            except httpx.RequestError as exc:
                await gateway.mark_unhealthy(
                    node, f"{type(exc).__name__}: {str(exc)[:100]}"
                )
                return JSONResponse(
                    status_code=502,
                    content={"error": "upstream_unreachable", "node": node.name},
                )

            # 5xx 与常规路径一致：如实透传并降级，不虚报成功
            if upstream_resp.status_code >= 500:
                await gateway.mark_unhealthy(
                    node, f"上游返回 HTTP {upstream_resp.status_code}"
                )
                return _passthrough(upstream_resp, node)

            if upstream_resp.status_code < 300:
                version = _parse_config_version(
                    upstream_resp.headers.get("x-config-version")
                )
                if version is None or version < min_version:
                    # 选路后版本回退/缺版本：拒绝交付旧配置，把观测值记为
                    # 后续选路依据，并改投其他合格节点。落后不算节点故障。
                    await gateway.note_config_version(
                        node, version, stale_reject=True
                    )
                    tried.add(node.name)
                    ok = True  # 上游交互本身成功，不计失败
                    continue
                # 实际观测达到门槛：交付，同时刷新版本记录
                await gateway.note_config_version(node, version)

            ok = True
            return _passthrough(upstream_resp, node)
        finally:
            # 无论成功、拒绝、失败还是取消，在途计数都必须准确归还
            await gateway.release_node(node, ok)


# 非 GET 方法访问转发路径：FastAPI 自动返回 405 Method Not Allowed


# ----------------------------- 管理接口 -----------------------------

@app.get("/admin/status", tags=["admin"])
async def admin_status() -> dict:
    async with gateway.cond:
        return {
            "probe_interval": PROBE_INTERVAL,
            "request_timeout": REQUEST_TIMEOUT,
            "nodes": [n.snapshot() for n in gateway.nodes],
            "eligible_nodes": [n.name for n in gateway.nodes if n.can_serve()],
        }


@app.post("/admin/nodes/{name}/drain", tags=["admin"])
async def admin_drain(name: str, timeout: Optional[float] = DRAIN_DEFAULT_TIMEOUT):
    """排空节点：立即停止分配新请求，并阻塞等待在途请求全部结束。

    只有在途数归零才返回 drained=true；超时仍有在途请求时返回 202，
    drained=false 且节点继续保持排空状态。
    """
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    drained, inflight = await gateway.drain(node, timeout)
    body = {
        "node": name,
        "draining": True,
        "drained": drained,
        "inflight": inflight,
        "detail": "在途请求已全部结束" if drained else "等待在途请求结束超时，节点仍处于排空中",
    }
    return JSONResponse(status_code=200 if drained else 202, content=body)


@app.post("/admin/nodes/{name}/resume", tags=["admin"])
async def admin_resume(name: str):
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    await gateway.resume(node)
    snap = node.snapshot()
    return {
        "node": name,
        "draining": False,
        "healthy": snap["healthy"],
        "eligible": snap["eligible"],
        "last_probe_detail": snap["last_probe_detail"],
        "detail": "已解除排空；是否可接流以健康探测结论为准",
    }


@app.post("/admin/probe", tags=["admin"])
async def admin_probe() -> dict:
    """手动触发一轮探测（探测不会改变任何节点的排空标志）。"""
    await gateway.probe_all()
    async with gateway.cond:
        return {"nodes": [n.snapshot() for n in gateway.nodes]}
