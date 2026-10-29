"""Eval bridge server: MCP Streamable HTTP + workspace admin API.

Architecture (route-1 ADP harness adapter for WorkBuddy Bench)::

    ADP platform (cloud sandbox, agent runs server-side)
        │  MCP tools (Streamable HTTP): wb_read_file / wb_bash / ...
        ▼
    this bridge (bench host, public via tunnel or private network)
        │  docker exec <container_id> sh -c ...
        ▼
    per-trial Docker sandbox (workspace + tests + verifier)

One bridge serves many concurrent trials: each ``AdpAgent`` trial registers a
*workspace* (``workspace_id`` → sandbox container id) via the admin API before
sending the task to ADP, and the task instruction tells the remote agent to
pass that ``workspace_id`` as the first argument of every tool call. Tool
invocations are journaled per workspace so the agent can pull them afterwards
to build an ATIF trajectory (remote platforms expose no stream-json).

Security posture:
  * The admin API (workspace registration — the sensitive part) is guarded by
    ``X-Bridge-Token`` (``WB_BRIDGE_TOKEN`` env; unset = dev mode, localhost).
  * MCP tools can only touch containers that were explicitly registered, and
    those are one-off eval sandboxes — equivalent to what the CLI harnesses do
    inside the same containers.
  * Secrets stay env-only; nothing is logged beyond tool call summaries with
    truncated output tails.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from mcp.server.fastmcp import FastMCP

from workbuddy_bench.bridge.docker_exec import DockerExecError, docker_exec, q
_MCP_PATH = "/mcp"
_DEFAULT_BASH_TIMEOUT = 120.0
_MAX_TOOL_RESULT_CHARS = 60_000
_MAX_READ_CHARS = 128_000
_MAX_CALL_LOG_IO_CHARS = 4_000
_MAX_GLOB_RESULTS = 500


@dataclass
class Workspace:
    workspace_id: str
    container_id: str
    exec_user: str | None = None
    created_ts: float = field(default_factory=time.time)
    calls: list[dict[str, Any]] = field(default_factory=list)


class WorkspaceRegistry:
    """In-memory workspace_id → container mapping with journaled tool calls."""

    def __init__(self, log_dir: Path, ttl_sec: float) -> None:
        self._log_dir = log_dir
        self._ttl_sec = ttl_sec
        self._workspaces: dict[str, Workspace] = {}
        self._lock = asyncio.Lock()
        self._log_dir.mkdir(parents=True, exist_ok=True)

    def _sweep_expired(self) -> None:
        now = time.time()
        stale = [wid for wid, ws in self._workspaces.items()
                 if now - ws.created_ts > self._ttl_sec]
        for wid in stale:
            self._workspaces.pop(wid, None)

    async def register(self, workspace_id: str, container_id: str,
                       exec_user: str | None) -> Workspace:
        async with self._lock:
            self._sweep_expired()
            ws = Workspace(workspace_id=workspace_id, container_id=container_id,
                           exec_user=exec_user or None)
            self._workspaces[workspace_id] = ws
            return ws

    async def unregister(self, workspace_id: str) -> bool:
        async with self._lock:
            return self._workspaces.pop(workspace_id, None) is not None

    def get(self, workspace_id: str) -> Workspace:
        self._sweep_expired()
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            raise KeyError(workspace_id)
        return ws

    def snapshot(self) -> list[dict[str, Any]]:
        self._sweep_expired()
        return [{
            "workspace_id": ws.workspace_id,
            "container_id": ws.container_id,
            "exec_user": ws.exec_user,
            "created_ts": ws.created_ts,
            "call_count": len(ws.calls),
        } for ws in self._workspaces.values()]

    def record(self, ws: Workspace, tool: str, arguments: dict[str, Any],
               return_code: int | None, duration_ms: int,
               out_tail: str, err_tail: str) -> None:
        entry = {
            "ts": time.time(),
            "tool": tool,
            "arguments": {k: v for k, v in arguments.items() if k != "workspace_id"},
            "return_code": return_code,
            "duration_ms": duration_ms,
            "stdout_tail": out_tail,
            "stderr_tail": err_tail,
        }
        ws.calls.append(entry)
        path = self._log_dir / f"workspace-{ws.workspace_id}.jsonl"
        try:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError:
            pass  # journaling is best-effort; the in-memory copy is primary


def _clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n... [truncated, {len(text)} chars total]"


def _decode(data: bytes) -> str:
    return data.decode("utf-8", errors="replace")


def _posix_parent(path: str) -> str:
    """Posix dirname that never returns '' (keeps `mkdir -p` sane)."""
    parent = str(PurePosixPath(path).parent)
    return parent if parent not in ("", ".") else "."


# ── MCP server ─────────────────────────────────────────────────────────

def build_mcp_server(registry: WorkspaceRegistry) -> FastMCP:
    mcp: FastMCP = FastMCP(
        "workbuddy-eval-bridge",
        # Tools are stateless (routing is via the explicit workspace_id), so
        # per-connection MCP sessions are unnecessary — stateless mode is the
        # most tolerant for gateway-managed MCP clients that may reconnect.
        stateless_http=True,
    )

    def _ws(workspace_id: str) -> Workspace:
        try:
            return registry.get(workspace_id)
        except KeyError:
            raise RuntimeError(
                f"Unknown workspace_id {workspace_id!r}. It must be passed "
                "exactly as given in the task instruction; if the evaluation "
                "run already finished, the workspace has been unregistered."
            ) from None

    async def _exec(ws: Workspace, tool: str, arguments: dict[str, Any],
                     script: str, *, stdin: bytes | None = None,
                     timeout_sec: float) -> tuple[str, str, int]:
        started = time.monotonic()
        try:
            outcome = await docker_exec(
                ws.container_id, script, stdin=stdin,
                timeout_sec=timeout_sec, user=ws.exec_user,
            )
            duration_ms = int((time.monotonic() - started) * 1000)
            out, err = _decode(outcome.stdout), _decode(outcome.stderr)
            registry.record(ws, tool, arguments, outcome.return_code,
                            duration_ms,
                            _clip(out, _MAX_CALL_LOG_IO_CHARS),
                            _clip(err, _MAX_CALL_LOG_IO_CHARS))
            return out, err, outcome.return_code
        except asyncio.TimeoutError:
            registry.record(ws, tool, arguments, None,
                            int((time.monotonic() - started) * 1000), "", "timeout")
            return "", "ERROR: command timed out", -1
        except DockerExecError as exc:
            registry.record(ws, tool, arguments, None,
                            int((time.monotonic() - started) * 1000), "", str(exc))
            return "", f"ERROR: docker exec failed: {exc}", -2

    @mcp.tool()
    async def wb_bash(workspace_id: str, command: str,
                      timeout_sec: int = 120) -> str:
        """Run a shell command in the task workspace and return its output.

        The working directory starts at the container root; the task files
        live under /workspace. Returns stdout, then stderr, then the exit
        code. `workspace_id` must be copied verbatim from the task
        instruction.
        """
        ws = _ws(workspace_id)
        out, err, rc = await _exec(
            ws, "wb_bash", {"command": command, "timeout_sec": timeout_sec},
            command, timeout_sec=min(max(timeout_sec, 1), 1800),
        )
        parts = []
        if out:
            parts.append(out)
        if err:
            parts.append("[stderr]\n" + err)
        parts.append(f"[exit code: {rc}]")
        return _clip("\n".join(parts), _MAX_TOOL_RESULT_CHARS)

    @mcp.tool()
    async def wb_read_file(workspace_id: str, path: str) -> str:
        """Read a text file from the task workspace (absolute path or
        relative to /workspace)."""
        ws = _ws(workspace_id)
        out, err, rc = await _exec(
            ws, "wb_read_file", {"path": path},
            f"cat -- {q(path)}", timeout_sec=30,
        )
        if rc != 0:
            return f"ERROR: cannot read {path}: {err.strip() or f'exit {rc}'}"
        return _clip(out, _MAX_READ_CHARS)

    @mcp.tool()
    async def wb_write_file(workspace_id: str, path: str, content: str) -> str:
        """Write (create or overwrite) a text file in the task workspace."""
        ws = _ws(workspace_id)
        parent = _posix_parent(path)
        out, err, rc = await _exec(
            ws, "wb_write_file", {"path": path, "content_bytes": len(content)},
            f"mkdir -p -- {q(parent)} && cat > {q(path)}",
            stdin=content.encode("utf-8"), timeout_sec=30,
        )
        if rc != 0:
            return f"ERROR: cannot write {path}: {err.strip() or f'exit {rc}'}"
        return f"wrote {len(content)} chars to {path}"

    @mcp.tool()
    async def wb_edit_file(workspace_id: str, path: str, old_str: str,
                           new_str: str, replace_all: bool = False) -> str:
        """Edit a file by exact string replacement (like the Edit tool).

        `old_str` must match the file content exactly; unless replace_all is
        set it must occur exactly once.
        """
        ws = _ws(workspace_id)
        out, err, rc = await _exec(
            ws, "wb_edit_file", {"path": path}, f"cat -- {q(path)}",
            timeout_sec=30,
        )
        if rc != 0:
            return f"ERROR: cannot read {path}: {err.strip() or f'exit {rc}'}"
        text = out
        count = text.count(old_str)
        if count == 0:
            return "ERROR: old_str not found in file; re-read the file and retry."
        if count > 1 and not replace_all:
            return (f"ERROR: old_str occurs {count} times; make it unique or "
                    "set replace_all=true.")
        new_text = text.replace(old_str, new_str) if replace_all else \
            text.replace(old_str, new_str, 1)
        parent = _posix_parent(path)
        out2, err2, rc2 = await _exec(
            ws, "wb_edit_file", {"path": path},
            f"mkdir -p -- {q(parent)} && cat > {q(path)}",
            stdin=new_text.encode("utf-8"), timeout_sec=30,
        )
        if rc2 != 0:
            return f"ERROR: cannot write {path}: {err2.strip() or f'exit {rc2}'}"
        replaced = count if replace_all else 1
        return f"replaced {replaced} occurrence(s) in {path}"

    @mcp.tool()
    async def wb_list_dir(workspace_id: str, path: str = ".") -> str:
        """List a directory in the task workspace."""
        ws = _ws(workspace_id)
        out, err, rc = await _exec(
            ws, "wb_list_dir", {"path": path},
            f"ls -la -- {q(path)}", timeout_sec=30,
        )
        if rc != 0:
            return f"ERROR: cannot list {path}: {err.strip() or f'exit {rc}'}"
        return _clip(out, _MAX_TOOL_RESULT_CHARS)

    @mcp.tool()
    async def wb_glob(workspace_id: str, pattern: str, path: str = ".") -> str:
        """Find files matching a glob pattern (e.g. '*.py') under a path."""
        ws = _ws(workspace_id)
        out, err, rc = await _exec(
            ws, "wb_glob", {"pattern": pattern, "path": path},
            f"find {q(path)} -maxdepth 8 -type f -name {q(pattern)} "
            f"| head -{_MAX_GLOB_RESULTS}",
            timeout_sec=60,
        )
        if rc != 0:
            return f"ERROR: glob failed: {err.strip() or f'exit {rc}'}"
        return out or "(no matches)"

    @mcp.tool()
    async def wb_grep(workspace_id: str, pattern: str, path: str = ".",
                      include: str = "*") -> str:
        """Search file contents with an extended regex, recursively."""
        ws = _ws(workspace_id)
        # find|xargs instead of `grep --include`: task images are frequently
        # Alpine (busybox grep), which has no --include.
        out, err, rc = await _exec(
            ws, "wb_grep", {"pattern": pattern, "path": path, "include": include},
            f"find {q(path)} -maxdepth 8 -type f -name {q(include)} -print0 "
            f"| xargs -0 grep -Hn -E -e {q(pattern)} "
            f"| head -{_MAX_GLOB_RESULTS}",
            timeout_sec=60,
        )
        if rc == 1:  # grep: no matches (or find matched nothing → rc 0 + empty)
            return "(no matches)"
        if rc != 0:
            return f"ERROR: grep failed: {err.strip() or f'exit {rc}'}"
        return _clip(out, _MAX_TOOL_RESULT_CHARS)

    return mcp


# ── Admin API (FastAPI) ────────────────────────────────────────────────

class WorkspaceIn(BaseModel):
    workspace_id: str
    container_id: str
    exec_user: str | None = None


def create_app(*, log_dir: Path, ttl_sec: float = 24 * 3600,
               bridge_token: str | None = None) -> FastAPI:
    registry = WorkspaceRegistry(log_dir=log_dir, ttl_sec=ttl_sec)
    token = bridge_token if bridge_token is not None else \
        os.environ.get("WB_BRIDGE_TOKEN", "").strip() or None

    mcp = build_mcp_server(registry)
    mcp_app = mcp.streamable_http_app()
    # The SDK wires its session manager into mcp_app's own lifespan; since we
    # mount it under FastAPI, run the session manager from our lifespan.
    session_manager = mcp.session_manager

    @asynccontextmanager
    async def _lifespan(app):
        async with session_manager.run():
            yield

    app = FastAPI(
        title="workbuddy-bench eval bridge",
        lifespan=_lifespan,
    )

    def _check_token(x_bridge_token: str | None) -> None:
        if token is None:
            return  # dev mode: no token configured
        if x_bridge_token != token:
            raise HTTPException(status_code=401, detail="invalid bridge token")

    @app.get("/health")
    async def health() -> dict:
        return {
            "status": "ok",
            "mcp_path": _MCP_PATH,
            "workspaces": len(registry.snapshot()),
            "token_required": token is not None,
        }

    @app.get("/admin/workspaces")
    async def list_workspaces(
        x_bridge_token: str | None = Header(default=None),
    ) -> list[dict]:
        _check_token(x_bridge_token)
        return registry.snapshot()

    @app.post("/admin/workspaces")
    async def register_workspace(
        body: WorkspaceIn,
        x_bridge_token: str | None = Header(default=None),
    ) -> dict:
        _check_token(x_bridge_token)
        if not body.workspace_id or not body.container_id:
            raise HTTPException(status_code=400,
                                detail="workspace_id and container_id required")
        await registry.register(body.workspace_id, body.container_id,
                                body.exec_user)
        return {"status": "registered", "workspace_id": body.workspace_id}

    @app.delete("/admin/workspaces/{workspace_id}")
    async def unregister_workspace(
        workspace_id: str,
        x_bridge_token: str | None = Header(default=None),
    ) -> dict:
        _check_token(x_bridge_token)
        removed = await registry.unregister(workspace_id)
        return {"status": "removed" if removed else "absent",
                "workspace_id": workspace_id}

    @app.get("/admin/workspaces/{workspace_id}/calls")
    async def workspace_calls(
        workspace_id: str,
        x_bridge_token: str | None = Header(default=None),
    ) -> JSONResponse:
        _check_token(x_bridge_token)
        try:
            ws = registry.get(workspace_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="unknown workspace")
        return JSONResponse({"workspace_id": ws.workspace_id,
                             "container_id": ws.container_id,
                             "calls": ws.calls})

    # Mount last: the MCP app owns everything not matched above (its own
    # route lives at /mcp).
    app.mount("/", mcp_app)
    return app
