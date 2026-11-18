#!/usr/bin/env python3
"""网关本地自测（不依赖 docker，使用 stdlib + 三个 uvicorn 子进程）。

覆盖关键场景：
  1.  轮询分发
  2.  状态 API
  3.  空闲节点排空 -> 立即 drained=true
  4.  排空后新请求不进入该节点
  5.  有在途慢请求时排空 -> 等在途结束才完成，且原响应可取回
  6.  全部不可接流 -> 503
  7.  上游 5xx -> 不虚报成功、节点降级、在途归零
  8.  探测成功不解除排空（交错核心）
  9.  恢复不绕过故障探测（交错核心）
  10. 仅 GET：POST 转发路径返回 405
  11. /docs 可打开
  12. 排空等待超时 -> 202，在途数准确
  13. 上游挂起 -> 网关请求超时 504，不虚报成功、在途归零
  14. 多并发在途请求排空
  15. 未知节点管理接口 404
  16. 配置单调读：版本门控生效，无门槛 GET 保持原行为
  17. 选路后版本回退：拒绝交付旧版、更新选路依据、改投合格节点
  18. 副本追赶：周期版本探测自动恢复候选资格（落后不算故障）
  19. 排空与版本检查交错：排空节点不收版本探测、在途版本请求计入排空等待
  20. 缺版本头响应：拒绝交付并排除，恢复发头后重新成为候选
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

PYLIBS = os.environ.get("PYLIBS", "/tmp/pylibs")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable

GW_PORT = 19080
U1_PORT = 19081
U2_PORT = 19082

passed = 0
failed = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


def http(method: str, url: str, timeout: float = 20.0, data: bytes | None = None,
         headers: dict | None = None):
    """返回 (status, headers, body_text)。4xx/5xx 不抛异常。响应头键统一小写。"""
    req = urllib.request.Request(url, method=method, data=data, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, {k.lower(): v for k, v in resp.headers.items()}, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}, e.read().decode()


def jget(url: str, timeout: float = 20.0):
    status, _, body = http("GET", url, timeout)
    return status, json.loads(body)


def wait_ready(url: str, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _, _ = http("GET", url, timeout=2)
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def spawn(module_dir: str, port: int, extra_env: dict | None = None):
    # -I 隔离模式会忽略 PYTHONPATH 且不含 cwd，故用 runpy 包装器显式注入依赖与应用目录
    launcher = (
        "import sys,runpy,os;"
        f"sys.path[:0]=[{PYLIBS!r}, os.getcwd()];"
        "sys.argv=['uvicorn']+sys.argv[1:];"
        "runpy.run_module('uvicorn', run_name='__main__')"
    )
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.update(extra_env or {})
    proc = subprocess.Popen(
        [PYTHON, "-I", "-c", launcher, "app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=module_dir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return proc


def kill(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=8)
        except Exception:
            proc.kill()


def node_status(node: str) -> dict:
    status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
    assert status == 200
    return next(n for n in body["nodes"] if n["name"] == node)


def set_mode(port: int, mode: str) -> None:
    status, _, body = http("POST", f"http://127.0.0.1:{port}/control/mode?mode={mode}")
    assert status == 200, body


def set_config(port: int, version: int, omit: bool | None = None) -> None:
    """直连上游调整配置版本（可升可降）；omit=True 时响应不再带版本头。"""
    q = f"version={version}"
    if omit is not None:
        q += f"&omit_version={'true' if omit else 'false'}"
    status, _, body = http("POST", f"http://127.0.0.1:{port}/control/config?{q}")
    assert status == 200, body


def get_config(min_version: int | None = None, query: str = "", timeout: float = 20.0):
    """经网关 GET /proxy/config，可带 X-Min-Version。返回 (status, headers, body)。"""
    headers = {"X-Min-Version": str(min_version)} if min_version is not None else None
    return http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/config{query}",
                timeout=timeout, headers=headers)


def main() -> int:
    procs = []
    try:
        print("== 启动两个上游和网关 ==")
        u1 = spawn(os.path.join(ROOT, "upstream"), U1_PORT, {"NODE_ID": "node1"})
        u2 = spawn(os.path.join(ROOT, "upstream"), U2_PORT, {"NODE_ID": "node2"})
        procs = [u1, u2]
        assert wait_ready(f"http://127.0.0.1:{U1_PORT}/")
        assert wait_ready(f"http://127.0.0.1:{U2_PORT}/")

        gw_env = {
            "UPSTREAMS": f"node1=http://127.0.0.1:{U1_PORT},node2=http://127.0.0.1:{U2_PORT}",
            "PROBE_INTERVAL": "1",
            "PROBE_TIMEOUT": "1",
            "REQUEST_TIMEOUT": "15",
            "DRAIN_DEFAULT_TIMEOUT": "10",
        }
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs"), "网关启动失败"
        time.sleep(1.2)  # 等首轮探测结果稳定

        # 1. 轮询分发
        print("== 1. 轮询分发 ==")
        seen = []
        for _ in range(4):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            assert status == 200, body
            seen.append(body["node"])
        check("4 个请求在两节点间交替", seen == ["node1", "node2", "node1", "node2"], str(seen))
        st = node_status("node1")
        check("rr_hits 各 2 次", st["rr_hits"] == 2, str(st))
        check("在途计数已归零", st["inflight"] == 0, str(st))

        # 2. 状态 API 内容
        print("== 2. 状态 API ==")
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
        check("两节点均健康", all(n["healthy"] for n in body["nodes"]))
        check("两节点均未排空", all(not n["draining"] for n in body["nodes"]))
        check("eligible 含两节点", sorted(body["eligible_nodes"]) == ["node1", "node2"])

        # 3. 空闲节点排空立即完成
        print("== 3. 空闲节点排空 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5")
        b = json.loads(body)
        check("排空返回 200", status == 200, body)
        check("drained=true", b.get("drained") is True, body)
        check("inflight=0", b.get("inflight") == 0, body)
        st = node_status("node1")
        check("状态显示 draining", st["draining"] is True and st["eligible"] is False)

        # 4. 排空后新请求不进入 node1
        print("== 4. 排空后新请求只去 node2 ==")
        nodes = set()
        for _ in range(3):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            nodes.add(body["node"])
        check("新请求全部落在 node2", nodes == {"node2"}, str(nodes))

        # 5. 在途慢请求 + 排空交错：排空等在途结束，原响应可取回
        print("== 5. 在途慢请求期间排空 node2 ==")
        result = {}

        def slow_call():
            result["status"], _, result["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20
            )

        t = threading.Thread(target=slow_call)
        t.start()
        # 等到请求确实进入 node2
        inflight_seen = 0
        deadline = time.time() + 5
        while time.time() < deadline:
            st2 = node_status("node2")
            inflight_seen = max(inflight_seen, st2["inflight"])
            if st2["inflight"] == 1:
                break
            time.sleep(0.1)
        check("慢请求已进入 node2 且 inflight=1", inflight_seen == 1, str(st2))

        drain_started = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=10"
        )
        drain_elapsed = time.time() - drain_started
        b = json.loads(body)
        t.join(timeout=20)
        check("排空请求被在途请求阻塞约 3s", drain_elapsed >= 2.5, f"{drain_elapsed:.2f}s")
        check("排空最终成功 drained=true", status == 200 and b.get("drained") is True, body)
        check("慢请求原响应成功取回 200", result.get("status") == 200, str(result))
        check("响应来自 node2 且内容完整",
              json.loads(result["body"]).get("node") == "node2"
              and json.loads(result["body"]).get("slept", 0) >= 2.5,
              result.get("body"))
        check("结束后两节点在途均为 0",
              all(node_status(n)["inflight"] == 0 for n in ("node1", "node2")))

        # 6. 两节点都排空 -> 503
        print("== 6. 无可用节点返回 503 ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("返回 503", status == 503, body)
        check("503 原因可解释", json.loads(body).get("error") == "no_eligible_upstream", body)

        # 7. 上游 5xx：不虚报成功 + 节点降级 + 在途归零
        print("== 7. 上游故障 (5xx) ==")
        # 先恢复 node1 以便发请求（node2 仍排空）
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 等周期性探测先发现故障
        st = node_status("node1")
        check("探测已将 node1 标记为不健康", st["healthy"] is False, str(st))
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("无健康节点时返回 503 而非虚假 200", status == 503, body)
        # 临时恢复健康标志位才能真正打到 5xx（模拟请求路径上的失败降级）
        set_mode(U1_PORT, "healthy")
        time.sleep(1.3)
        st = node_status("node1")
        check("探测恢复后 node1 健康", st["healthy"] is True, str(st))
        set_mode(U1_PORT, "error")  # 不等待探测，让请求先撞上 5xx
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("5xx 状态如实返回", status == 500, f"status={status} body={body}")
        time.sleep(0.3)
        st = node_status("node1")
        check("请求失败后节点即时降级", st["healthy"] is False, str(st))
        check("失败后在途计数仍归零", st["inflight"] == 0, str(st))

        # 8. 交错核心：探测成功不得解除排空
        print("== 8. 探测成功不解除排空 ==")
        set_mode(U1_PORT, "healthy")
        # node1 当前未排空，先排空，再让周期性探测持续成功
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        assert status == 200
        time.sleep(2.5)  # 跨越至少 2 轮探测，上游为 healthy，探测必然成功
        st = node_status("node1")
        check("多轮探测成功后排空标志仍在", st["draining"] is True, str(st))
        check("探测成功只更新健康位", st["healthy"] is True and st["eligible"] is False, str(st))

        # 9. 交错核心：恢复不绕过故障探测
        print("== 9. 恢复不绕过故障探测 ==")
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 探测确认故障
        st_before = node_status("node1")
        check("恢复前 node1 不健康", st_before["healthy"] is False, str(st_before))
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        b = json.loads(body)
        check("resume 清除排空但节点仍不可接流",
              status == 200 and b["draining"] is False and b["eligible"] is False, body)
        st = node_status("node1")
        check("draining=False 且 healthy=False",
              st["draining"] is False and st["healthy"] is False, str(st))
        # 故障节点上请求不会被分配：node2 仍排空 -> 503
        status, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("故障节点不会被轮询选中 (503)", status == 503)
        # 上游恢复后经探测自动回到可接流
        set_mode(U1_PORT, "healthy")
        time.sleep(1.5)
        st = node_status("node1")
        check("探测恢复后节点重新 eligible", st["eligible"] is True, str(st))
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("流量恢复进入 node1", status == 200 and body["node"] == "node1")

        # 10. 仅 GET 转发
        print("== 10. 非 GET 方法被拒 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/proxy/echo", data=b"x")
        check("POST 转发路径返回 405", status == 405, f"status={status}")

        # 11. /docs 可打开
        print("== 11. /docs 与 openapi.json ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/docs")
        check("/docs 返回 200", status == 200 and "swagger" in body.lower())
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/openapi.json")
        paths = body["paths"]
        check("openapi 含管理与转发接口",
              "/admin/nodes/{name}/drain" in paths and "/proxy/{path}" in paths)

        # 12. 排空等待超时：202 + 在途准确
        print("== 12. 排空超时返回 202 ==")
        result12 = {}

        def slow_call12():
            result12["status"], _, result12["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=4", timeout=15
            )

        t12 = threading.Thread(target=slow_call12)
        t12.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] != 1:
            time.sleep(0.1)
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=1"
        )
        b = json.loads(body)
        check("短超时排空返回 202", status == 202, body)
        check("drained=false 且 inflight=1", b.get("drained") is False and b.get("inflight") == 1, body)
        t12.join(timeout=15)
        check("在途慢请求仍拿到完整 200", result12.get("status") == 200, str(result12))
        time.sleep(0.3)
        # 在途归零后再次排空（节点已在排空中）应立即完成
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        b = json.loads(body)
        check("在途归零后排空立即完成", status == 200 and b["drained"] is True, body)

        # 13. 请求超时：重启网关为短超时，上游 hang（探测与业务请求都会挂住）
        print("== 13. 上游挂起 -> 网关 504 超时 ==")
        kill(gw)
        procs.remove(gw)
        short_env = dict(gw_env)
        short_env["REQUEST_TIMEOUT"] = "1"
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, short_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        set_mode(U1_PORT, "healthy")
        # 等首轮探测把 node1 标记为健康可接流（网关重启后状态全清）
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 经探测恢复可接流", node_status("node1")["eligible"] is True)
        # 切 hang 后立即发请求：赶在下一轮周期探测摘掉它之前验证网关自身的请求超时
        set_mode(U1_PORT, "hang")
        t0 = time.time()
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
        elapsed = time.time() - t0
        if status == 503:
            # 探测抢先把节点摘掉，则手动恢复后重试一次，确保覆盖请求超时路径
            set_mode(U1_PORT, "healthy")
            deadline = time.time() + 6
            while time.time() < deadline and not node_status("node1")["eligible"]:
                time.sleep(0.2)
            set_mode(U1_PORT, "hang")
            t0 = time.time()
            status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
            elapsed = time.time() - t0
        check("挂起请求在约 1s 内返回 504", status == 504 and elapsed < 4,
              f"status={status} elapsed={elapsed:.1f}s body={body}")
        check("未虚报成功", status != 200)
        time.sleep(0.3)
        st = node_status("node1")
        check("超时后在途计数准确归零", st["inflight"] == 0, str(st))
        check("超时后节点被标记不健康", st["healthy"] is False, str(st))
        set_mode(U1_PORT, "healthy")

        # 14. 多个并发在途请求：排空须等全部结束，在途计数全程准确
        print("== 14. 多并发在途请求排空 ==")
        # 恢复正常请求超时并重启网关（清空场景 13 的短超时与节点状态）
        kill(gw)
        procs.remove(gw)
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 可接流", node_status("node1")["eligible"] is True)
        # 先排空 node2，把流量集中到 node1
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        assert status == 200
        results14 = []

        def slow_call14(idx: int):
            s, _, b = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20)
            results14.append((idx, s, b))

        threads = [threading.Thread(target=slow_call14, args=(i,)) for i in range(4)]
        for th in threads:
            th.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] < 4:
            time.sleep(0.1)
        st = node_status("node1")
        check("4 个在途请求计数准确", st["inflight"] == 4, str(st))
        t0 = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=15"
        )
        elapsed = time.time() - t0
        b = json.loads(body)
        for th in threads:
            th.join(timeout=20)
        check("排空阻塞到全部 4 个在途请求结束", elapsed >= 2.5 and b.get("drained") is True,
              f"elapsed={elapsed:.1f}s body={body}")
        check("4 个慢请求全部取回完整 200",
              len(results14) == 4 and all(s == 200 for _, s, _ in results14),
              str(results14))
        check("全部结束后在途归零", node_status("node1")["inflight"] == 0)

        # 15. 未知节点管理接口返回 404
        print("== 15. 未知节点 404 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/drain")
        check("排空未知节点返回 404", status == 404, body)
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/resume")
        check("恢复未知节点返回 404", status == 404, body)

        # 16. 配置单调读：版本门控生效；不带门槛的 GET 保持原行为
        print("== 16. 配置单调读：版本门控 ==")
        for name in ("node1", "node2"):
            status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/{name}/resume")
            assert status == 200, body
        set_config(U1_PORT, 5)
        set_config(U2_PORT, 3)
        time.sleep(1.6)  # 等周期版本探测刷新两节点版本
        st1, st2 = node_status("node1"), node_status("node2")
        check("网关已跟踪两副本配置版本",
              st1["config_version"] == 5 and st2["config_version"] == 3,
              f"node1={st1['config_version']} node2={st2['config_version']}")
        # 无门槛：两节点轮询，较低版本照样交付（原行为）
        seen, versions = set(), set()
        for _ in range(2):
            s, h, b = get_config()
            assert s == 200, b
            seen.add(json.loads(b)["node"])
            versions.add(h.get("x-config-version"))
        check("无门槛请求仍在两节点间轮询", seen == {"node1", "node2"}, str(seen))
        check("无门槛请求透传各自版本头（含较低版本）", versions == {"5", "3"}, str(versions))
        # 门槛 4：只有 node1(v5) 合格
        seen = set()
        for _ in range(3):
            s, h, b = get_config(min_version=4)
            assert s == 200 and h.get("x-config-version") == "5", f"{s} {h} {b}"
            seen.add(json.loads(b)["node"])
        check("门槛 4 的请求全部落在 node1(v5)", seen == {"node1"}, str(seen))
        # 门槛 6：超过所有副本 -> 明确 503 暂不可读，不交付旧配置
        s, h, b = get_config(min_version=6)
        check("门槛超过全部副本返回 503", s == 503, f"status={s} body={b}")
        check("503 明确标识暂不可读", json.loads(b).get("error") == "config_min_version_unavailable", b)
        check("503 未夹带配置体", "config" not in json.loads(b), b)
        # 非法门槛 -> 422；门槛头只作用于 /proxy/config
        s, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/config",
                       headers={"X-Min-Version": "abc"})
        check("非整数门槛被拒 (422)", s == 422, f"status={s}")
        s, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo",
                       headers={"X-Min-Version": "99"})
        check("门槛头不影响其他转发路径", s == 200, f"status={s}")

        # 18. 副本追赶：周期版本探测自动恢复候选资格（落后不算故障）
        print("== 18. 副本追赶自动恢复候选 ==")
        st2 = node_status("node2")
        check("落后期间 node2 未被记为故障",
              st2["healthy"] is True and st2["stale_count"] == 0, str(st2))
        set_config(U2_PORT, 8)  # node2 追赶到 v8
        time.sleep(1.6)         # 周期版本探测自动发现，无需人工干预
        st2 = node_status("node2")
        check("追赶后版本记录自动刷新为 8", st2["config_version"] == 8,
              str(st2["config_version"]))
        seen = set()
        for _ in range(3):
            s, h, b = get_config(min_version=6)
            assert s == 200 and h.get("x-config-version") == "8", f"{s} {b}"
            seen.add(json.loads(b)["node"])
        check("门槛 6 全部落在追赶后的 node2(v8)", seen == {"node2"}, str(seen))

        # 19. 排空与版本检查交错
        print("== 19. 排空与版本检查交错 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5")
        check("空闲 node1 排空完成", status == 200 and json.loads(body)["drained"] is True, body)
        set_config(U1_PORT, 30)   # 排空期间把 node1 版本调得很高
        time.sleep(1.6)           # 跨越多轮探测
        st1 = node_status("node1")
        check("版本探测不打排空节点（记录仍为 5）", st1["config_version"] == 5,
              str(st1["config_version"]))
        check("健康探测仍覆盖排空节点", st1["healthy"] is True and st1["draining"] is True, str(st1))
        s, _, b = get_config(min_version=10)
        check("门槛 10 无合格节点 -> 503 而非旧版",
              s == 503 and json.loads(b).get("error") == "config_min_version_unavailable",
              f"{s} {b}")
        s, h, b = get_config(min_version=6)
        check("门槛 6 由未排空的 node2(v8) 交付",
              s == 200 and json.loads(b)["node"] == "node2", f"{s} {b}")
        # 在途版本请求计入排空等待
        result19 = {}

        def slow_cfg():
            result19["status"], result19["headers"], result19["body"] = \
                get_config(min_version=4, query="?delay=3")

        t19 = threading.Thread(target=slow_cfg)
        t19.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node2")["inflight"] != 1:
            time.sleep(0.1)
        check("在途版本请求已进入 node2", node_status("node2")["inflight"] == 1)
        t0 = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=10"
        )
        elapsed = time.time() - t0
        b19 = json.loads(body)
        t19.join(timeout=15)
        check("排空等待在途版本请求结束 (~3s)",
              elapsed >= 2.5 and b19.get("drained") is True, f"{elapsed:.2f}s {body}")
        check("在途版本请求取回完整 200/v8",
              result19.get("status") == 200
              and result19["headers"].get("x-config-version") == "8",
              str(result19.get("status")))
        # 恢复后版本探测立即刷新（node1 实际已到 30）
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/resume")
        assert status == 200
        st1 = node_status("node1")
        check("恢复后 node1 版本立即刷新为 30", st1["config_version"] == 30,
              str(st1["config_version"]))
        s, h, b = get_config(min_version=10)
        check("门槛 10 由恢复后的 node1(v30) 交付",
              s == 200 and json.loads(b)["node"] == "node1", f"{s} {b}")

        # 17. 选路后版本回退（长探测间隔 + 手动探测，排除周期探测竞态）
        print("== 17. 选路后版本回退：拒绝旧版并更新选路依据 ==")
        kill(gw)
        procs.remove(gw)
        c2_env = dict(gw_env)
        c2_env["PROBE_INTERVAL"] = "3600"  # 关闭周期探测，全程手动探测保证确定性
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, c2_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        check("C2 网关启动探测后两节点可接流",
              all(node_status(n)["eligible"] for n in ("node1", "node2")))
        set_config(U1_PORT, 5)
        set_config(U2_PORT, 5)
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/probe")
        assert status == 200
        check("手动探测后两节点记录为 v5",
              all(node_status(n)["config_version"] == 5 for n in ("node1", "node2")))
        # 用 /proxy/echo 定位轮询指针（不影响版本记录），下一个被选中的是 other
        _, _, b = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        first = json.loads(b)["node"]
        other = "node2" if first == "node1" else "node1"
        other_port = U2_PORT if other == "node2" else U1_PORT
        first_port = U1_PORT if first == "node1" else U2_PORT
        stale0 = node_status(other)["stale_count"]
        fail0 = node_status(other)["failure_count"]
        set_config(other_port, 2)   # other 实际回退到 v2；无周期探测，网关仍记 v5
        s, h, b = get_config(min_version=4)
        check("选路后回退的响应被拒绝并改投合格节点",
              s == 200 and json.loads(b)["node"] == first, f"{s} {b}")
        check("交付版本满足门槛", h.get("x-config-version") == "5", str(h))
        st_o = node_status(other)
        check("选路依据已更新为实际版本 2", st_o["config_version"] == 2,
              str(st_o["config_version"]))
        check("回退记 stale 但不算节点故障",
              st_o["stale_count"] == stale0 + 1
              and st_o["failure_count"] == fail0
              and st_o["healthy"] is True,
              str(st_o))
        s, h, b = get_config(min_version=4)
        check("更新后的选路依据生效（仍走合格节点）",
              s == 200 and json.loads(b)["node"] == first, f"{s} {b}")
        # first 也回退：所有候选实际版本都不足 -> 明确 503，不交付旧配置
        set_config(first_port, 1)
        s, _, b = get_config(min_version=4)
        check("全部实际回退后返回 503 暂不可读",
              s == 503 and json.loads(b).get("error") == "config_min_version_unavailable",
              f"{s} {b}")
        check("first 的记录同步为实际版本 1", node_status(first)["config_version"] == 1)
        hits0 = sum(node_status(n)["rr_hits"] for n in ("node1", "node2"))
        s, _, b = get_config(min_version=4)
        hits1 = sum(node_status(n)["rr_hits"] for n in ("node1", "node2"))
        check("无合格节点时直接 503 不再打上游", s == 503 and hits1 == hits0,
              f"status={s} hits {hits0}->{hits1}")

        # 20. 缺版本头响应：拒绝交付并排除，恢复发头后重新成为候选
        print("== 20. 缺版本头：拒绝并自动恢复 ==")
        set_config(U1_PORT, 5, omit=False)
        set_config(U2_PORT, 5, omit=False)
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/probe")
        assert status == 200
        check("两节点恢复 v5 记录",
              all(node_status(n)["config_version"] == 5 for n in ("node1", "node2")))
        _, _, b = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        first = json.loads(b)["node"]
        other = "node2" if first == "node1" else "node1"
        other_port = U2_PORT if other == "node2" else U1_PORT
        stale0 = node_status(other)["stale_count"]
        set_config(other_port, 5, omit=True)   # 版本不变但不再发版本头
        s, h, b = get_config(min_version=4)
        check("缺版本响应被拒绝并改投合格节点",
              s == 200 and json.loads(b)["node"] == first, f"{s} {b}")
        check("交付响应带版本头", h.get("x-config-version") == "5", str(h))
        st_o = node_status(other)
        check("缺版本节点记录清空并被排除",
              st_o["config_version"] is None and st_o["stale_count"] == stale0 + 1,
              str(st_o))
        check("缺版本不算节点故障", st_o["healthy"] is True, str(st_o))
        set_config(other_port, 5, omit=False)
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/probe")
        assert status == 200
        check("恢复发头后重新成为候选", node_status(other)["config_version"] == 5)

        print(f"\n==== 结果: {passed} passed, {failed} failed ====")
        return 1 if failed else 0

    finally:
        for p in procs:
            kill(p)


if __name__ == "__main__":
    sys.exit(main())
