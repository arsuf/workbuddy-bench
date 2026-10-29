"""Integration smoke test for the AdpAgent harness path (not part of the suite).

Wires the full route-1 chain with fakes only where the real ADP platform
would sit::

    AdpAgent (real) ──▶ fake ADP SSE server (real HTTP/SSE; the handler plays
      the remote agent and really calls the bridge MCP tools) ──▶ eval bridge
      (real) ──▶ throwaway Docker container (real)

Verifies: workspace registration, instruction preamble, tool effects landing
in the sandbox, SSE reply collection, StatInfo token fill, bridge call
journal → ATIF trajectory, and workspace cleanup.

Usage:
    docker run -d --rm --name wbb-bridge-e2e alpine:3.20 sleep 600
    uv run python scripts/adp_agent_e2e_test.py --container wbb-bridge-e2e
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from harbor.models.agent.context import AgentContext
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from workbuddy_bench.agents.adp_agent import AdpAgent

BRIDGE_PORT = 8940
FAKE_ADP_PORT = 8941
TOKEN = "e2e-token"


class FakeEnvironment:
    """Just enough of BaseEnvironment for AdpAgent.setup()."""

    def __init__(self, container_id: str) -> None:
        self._cid = container_id

    async def exec(self, command: str, cwd=None, env=None,
                   timeout_sec=None, user=None):
        from harbor.environments.base import ExecResult
        if command == "cat /etc/hostname":
            return ExecResult(stdout=self._cid + "\n", stderr=None, return_code=0)
        return ExecResult(stdout="", stderr="not implemented", return_code=1)


def build_fake_adp(bridge_url: str) -> FastAPI:
    app = FastAPI()

    @app.post("/adp/v2/chat")
    async def chat(request: Request) -> StreamingResponse:
        body = await request.json()
        prompt = body["Contents"][0]["Text"]
        assert body.get("BotAppKey") and body.get("AppKey"), "app key missing"
        m = re.search(r'workspace_id = "([^"]+)"', prompt)
        assert m, "instruction preamble with workspace_id missing"
        workspace_id = m.group(1)
        assert "WorkBuddy Bench evaluation session" in prompt

        # Play the remote ADP agent: do real work through the bridge MCP.
        async with streamablehttp_client(f"{bridge_url}/mcp") as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                res = await session.call_tool("wb_bash", {
                    "workspace_id": workspace_id,
                    "command": "echo made-by-adp > /workspace/adp-wrote.txt",
                })
                assert not res.isError, res.content

        async def sse():
            def chunk(event: str, data: dict[str, Any]) -> bytes:
                return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()

            yield chunk("text.delta", {"Text": "Task done. "})
            yield chunk("text.delta", {"Text": "Wrote the file."})
            yield chunk("response.completed", {
                "TraceId": "trace-fake-1",
                "StatInfo": {"InputTokens": 1234, "OutputTokens": 57,
                             "ModelName": "fake-model", "TotalCost": 0.42},
            })
            yield b"event: done\ndata: [DONE]\n\n"

        return StreamingResponse(sse(), media_type="text/event-stream")

    return app


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--container", default="wbb-bridge-e2e")
    args = parser.parse_args()

    log_root = Path(tempfile.mkdtemp(prefix="wbb-adp-e2e-"))
    logs_dir = log_root / "trial-e2e" / "agent"

    # 1. bridge subprocess
    bridge_env = {**os.environ, "WB_BRIDGE_LOG_DIR": str(log_root / "bridge-logs")}
    bridge = await asyncio.create_subprocess_exec(
        "uv", "run", "python", "-m", "workbuddy_bench.bridge",
        "--host", "127.0.0.1", "--port", str(BRIDGE_PORT), "--token", TOKEN,
        "--log-dir", bridge_env["WB_BRIDGE_LOG_DIR"],
        env=bridge_env,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    # 2. fake ADP subprocess
    adp = await asyncio.create_subprocess_exec(
        "uv", "run", "python", "-c",
        "import uvicorn, sys; uvicorn.run(__import__('scripts.adp_agent_e2e_test', "
        "fromlist=['x']).build_fake_adp('http://127.0.0.1:%d'), host='127.0.0.1', "
        "port=%d, log_level='warning')" % (BRIDGE_PORT, FAKE_ADP_PORT),
        env={**os.environ, "PYTHONPATH": "scripts" + os.pathsep + os.environ.get("PYTHONPATH", "")},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )

    async def wait_health(url: str) -> bool:
        for _ in range(60):
            try:
                async with httpx.AsyncClient(timeout=2) as c:
                    if (await c.get(url)).status_code == 200:
                        return True
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.5)
        return False

    failures: list[str] = []

    def check(name: str, cond: bool, detail: str = "") -> None:
        print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail and not cond else ""))
        if not cond:
            failures.append(name)

    try:
        check("bridge healthy", await wait_health(f"http://127.0.0.1:{BRIDGE_PORT}/health"))
        # fake ADP has no /health; probe the chat path with a bad request.
        for _ in range(60):
            try:
                async with httpx.AsyncClient(timeout=2) as c:
                    await c.post(f"http://127.0.0.1:{FAKE_ADP_PORT}/adp/v2/chat",
                                 json={"Contents": []})
                break
            except httpx.HTTPError:
                await asyncio.sleep(0.5)
        else:
            check("fake ADP healthy", False)

        # 3. run the real AdpAgent against the fakes
        os.environ.update({
            "ADP_BASE_URL": f"http://127.0.0.1:{FAKE_ADP_PORT}",
            "ADP_APP_KEY": "test-app-key",
            "ADP_BRIDGE_URL": f"http://127.0.0.1:{BRIDGE_PORT}",
            "ADP_BRIDGE_TOKEN": TOKEN,
        })
        agent = AdpAgent(logs_dir=logs_dir, model_name="adp/managed",
                         ADP_VERSION="v1", ADP_TIMEOUT_SEC=60,
                         connection={"mode": "direct", "proxy_url": "",
                                     "model_route": ""})
        context = AgentContext()
        await agent.run("Create the deliverable.", FakeEnvironment(args.container),
                        context)

        # 4. assertions
        reply = (logs_dir / "adp-reply.txt").read_text()
        check("reply collected", reply == "Task done. Wrote the file.", repr(reply))

        events = [json.loads(l) for l in
                  (logs_dir / "adp-events.jsonl").read_text().splitlines()]
        names = [e["event"] for e in events]
        check("SSE events recorded",
              names == ["text.delta", "text.delta", "response.completed", "done"],
              str(names))

        calls = json.loads((logs_dir / "bridge-calls.json").read_text())
        check("bridge journal pulled", len(calls) == 1
              and calls[0]["tool"] == "wb_bash", str(calls)[:200])

        traj = json.loads((logs_dir / "trajectory.json").read_text())
        fm = traj.get("final_metrics") or {}
        check("trajectory step from bridge call",
              len(traj.get("steps") or []) == 1
              and traj["steps"][0]["tool_calls"][0]["function_name"] == "wb_bash")
        check("StatInfo tokens in trajectory",
              fm.get("total_prompt_tokens") == 1234
              and fm.get("total_completion_tokens") == 57
              and fm.get("total_cost_usd") == 0.42, str(fm))

        check("context filled", context.n_input_tokens == 1234
              and context.n_output_tokens == 57
              and context.cost_usd == 0.42)

        # tool effect landed in the sandbox via the fake ADP agent
        import subprocess
        out = subprocess.run(
            ["docker", "exec", args.container, "cat", "/workspace/adp-wrote.txt"],
            capture_output=True, text=True, timeout=10)
        check("ADP tool call landed in sandbox", out.stdout.strip() == "made-by-adp",
              out.stdout + out.stderr)

        # workspace cleaned up
        async with httpx.AsyncClient(timeout=5, headers={"X-Bridge-Token": TOKEN}) as c:
            r = await c.get(f"http://127.0.0.1:{BRIDGE_PORT}/admin/workspaces")
            check("workspace unregistered", r.json() == [], r.text)
    finally:
        for proc in (bridge, adp):
            proc.terminate()
            await proc.wait()

    print()
    if failures:
        print(f"ADP AGENT E2E FAILED: {failures}")
        return 1
    print("ADP AGENT E2E OK — full harness path verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
