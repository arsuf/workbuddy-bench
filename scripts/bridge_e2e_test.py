"""End-to-end smoke test for the eval bridge (not part of the suite).

Starts the bridge in-process, registers a workspace bound to a throwaway
container, then drives the MCP tools through a real Streamable HTTP client —
the same shape of interaction ADP's plugin-gateway will perform.

Usage:
    docker run -d --rm --name wbb-bridge-e2e alpine:3.20 sleep 300
    uv run python scripts/bridge_e2e_test.py --container wbb-bridge-e2e
"""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from pathlib import Path

import httpx
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession

from workbuddy_bench.bridge.server import create_app

WS_ID = "e2e-test-ws"


async def run_bridge(port: int, token: str) -> asyncio.subprocess.Process:
    env = {
        **__import__("os").environ,
        "WB_BRIDGE_TOKEN": token,
        "WB_BRIDGE_LOG_DIR": tempfile.mkdtemp(prefix="wbb-bridge-log-"),
    }
    proc = await asyncio.create_subprocess_exec(
        "uv", "run", "python", "-m", "workbuddy_bench.bridge",
        "--host", "127.0.0.1", "--port", str(port), "--token", token,
        "--log-dir", env["WB_BRIDGE_LOG_DIR"],
        env=env,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    # Wait for /health.
    for _ in range(60):
        try:
            async with httpx.AsyncClient(timeout=2) as c:
                r = await c.get(f"http://127.0.0.1:{port}/health")
                if r.status_code == 200:
                    return proc
        except httpx.HTTPError:
            pass
        await asyncio.sleep(0.5)
    out = await proc.stdout.read()
    raise RuntimeError(f"bridge did not become healthy:\n{out.decode()}")


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--container", default="wbb-bridge-e2e")
    parser.add_argument("--port", type=int, default=8939)
    parser.add_argument("--token", default="e2e-token")
    args = parser.parse_args()

    proc = await run_bridge(args.port, args.token)
    base = f"http://127.0.0.1:{args.port}"
    headers = {"X-Bridge-Token": args.token}
    failures: list[str] = []

    def check(name: str, cond: bool, detail: str = "") -> None:
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""))
        if not cond:
            failures.append(name)

    try:
        # 1. auth enforcement
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{base}/admin/workspaces",
                             json={"workspace_id": WS_ID, "container_id": args.container})
            check("admin rejects missing token", r.status_code == 401, str(r.status_code))

        # 2. workspace registration
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{base}/admin/workspaces", headers=headers,
                             json={"workspace_id": WS_ID, "container_id": args.container})
            check("workspace registration", r.status_code == 200, r.text)

        # 3. MCP session over Streamable HTTP: list tools + call each wb_* tool
        async with streamablehttp_client(f"{base}/mcp") as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                names = sorted(t.name for t in tools.tools)
                print("  tools:", ", ".join(names))
                check("7 wb_* tools exposed", names == [
                    "wb_bash", "wb_edit_file", "wb_glob", "wb_grep",
                    "wb_list_dir", "wb_read_file", "wb_write_file"], str(names))

                res = await session.call_tool("wb_bash", {
                    "workspace_id": WS_ID,
                    "command": "echo hello && mkdir -p /workspace && echo 'alpha beta' > /workspace/notes.txt",
                })
                out = res.content[0].text if res.content else ""
                check("wb_bash", "hello" in out and "[exit code: 0]" in out, out)

                res = await session.call_tool("wb_read_file", {
                    "workspace_id": WS_ID, "path": "/workspace/notes.txt"})
                out = res.content[0].text if res.content else ""
                check("wb_read_file", out.strip() == "alpha beta", out)

                res = await session.call_tool("wb_edit_file", {
                    "workspace_id": WS_ID, "path": "/workspace/notes.txt",
                    "old_str": "beta", "new_str": "gamma"})
                out = res.content[0].text if res.content else ""
                check("wb_edit_file", "replaced 1 occurrence" in out, out)

                res = await session.call_tool("wb_write_file", {
                    "workspace_id": WS_ID, "path": "/workspace/sub/new.txt",
                    "content": "written by bridge"})
                out = res.content[0].text if res.content else ""
                check("wb_write_file", "wrote 17 chars" in out, out)

                res = await session.call_tool("wb_glob", {
                    "workspace_id": WS_ID, "pattern": "*.txt", "path": "/workspace"})
                out = res.content[0].text if res.content else ""
                check("wb_glob", "notes.txt" in out and "sub/new.txt" in out, out)

                res = await session.call_tool("wb_grep", {
                    "workspace_id": WS_ID, "pattern": "gamma", "path": "/workspace"})
                out = res.content[0].text if res.content else ""
                check("wb_grep", "notes.txt:1:alpha gamma" in out, out)

                res = await session.call_tool("wb_list_dir", {
                    "workspace_id": WS_ID, "path": "/workspace"})
                out = res.content[0].text if res.content else ""
                check("wb_list_dir", "notes.txt" in out and "sub" in out, out)

                # unknown workspace must error, not silently execute
                res = await session.call_tool("wb_bash", {
                    "workspace_id": "no-such-ws", "command": "echo x"})
                check("unknown workspace_id errors",
                      res.isError or (res.content and "Unknown workspace_id" in res.content[0].text))

        # 4. call journal
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"{base}/admin/workspaces/{WS_ID}/calls", headers=headers)
            calls = r.json().get("calls") or []
            recorded = sorted({c.get("tool") for c in calls})
            check("call journal has all tools", len(recorded) == 7, str(recorded))
            check("journal redacts workspace_id from args",
                  all("workspace_id" not in (c.get("arguments") or {}) for c in calls))

            # 5. unregister
            r = await c.delete(f"{base}/admin/workspaces/{WS_ID}", headers=headers)
            check("unregister", r.status_code == 200, r.text)
            r = await c.get(f"{base}/admin/workspaces", headers=headers)
            check("registry empty after unregister", r.json() == [], r.text)
    finally:
        proc.terminate()
        await proc.wait()

    print()
    if failures:
        print(f"E2E FAILED: {failures}")
        return 1
    print("E2E OK — all bridge checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
