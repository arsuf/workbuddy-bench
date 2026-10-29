"""AdpAgent — ADP (server-side agent platform) harness for the bench.

Unlike the CLI harnesses (cbc / claude-code), the evaluated agent does **not**
run inside the task container: ADP executes it server-side in its own cloud
sandbox. This agent class therefore follows Harbor's *external* agent pattern
(cf. Harbor's ``CorpAssistant``): it subclasses ``BaseAgent`` directly, drives
the platform over HTTP from the host, and bridges the workspace gap with the
eval bridge (`workbuddy_bench.bridge`):

    ┌────────────── host ───────────────┐      ┌──────── cloud ────────┐
    │ AdpAgent.run():                   │      │ ADP platform          │
    │  1. register workspace            │─────▶│  (BotAppKey, SSE)     │
    │     (trial → container id)        │      │   agent calls wb_*    │
    │  2. POST /adp/v2/chat (SSE)  ─────┼─────▶│   MCP tools with the  │
    │  3. collect reply + StatInfo      │      │   workspace_id        │
    │  4. pull bridge call journal      │◀─────┼── eval bridge /mcp ───┼──▶ docker exec <container>
    │  5. ATIF trajectory + context     │      │                       │
    └───────────────────────────────────┘      └───────────────────────┘

Consequences of the remote-execution model:

- Token/cost stats come from the platform's ``response.completed`` StatInfo,
  not a CLI stream (``context`` is filled directly in ``run()``).
- The trajectory's tool steps come from the bridge's journaled calls — the
  only per-trial ground truth for what the remote agent actually did.
- Secrets (``ADP_APP_KEY``) are env-only, mirroring the bench's ``*_env``
  convention; nothing is written into configs or logs.

All knobs come from harness/model config via kwargs or the environment; see
``configs/harnesses/adp/CONFIG.md``.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

import httpx

from harbor.agents.base import BaseAgent
from harbor.environments.base import BaseEnvironment
from harbor.models.agent.context import AgentContext
from harbor.models.trajectories.agent import Agent
from harbor.models.trajectories.final_metrics import FinalMetrics
from harbor.models.trajectories.step import Step
from harbor.models.trajectories.tool_call import ToolCall
from harbor.models.trajectories.trajectory import Trajectory

_REPLY_FILENAME = "adp-reply.txt"
_EVENTS_FILENAME = "adp-events.jsonl"
_BRIDGE_CALLS_FILENAME = "bridge-calls.json"
_TRAJECTORY_FILENAME = "trajectory.json"

# Prepended to the task instruction so the remote agent operates the sandbox
# through the bridge instead of its own (cloud) workspace.
_BRIDGE_PREAMBLE = """\
[WorkBuddy Bench evaluation session]
You are being evaluated on a task whose workspace lives in a remote sandbox
container. You do NOT have a local workspace for this task: every file and
shell operation MUST go through the MCP tools wb_bash, wb_read_file,
wb_write_file, wb_edit_file, wb_list_dir, wb_glob and wb_grep, always passing
the following exact value as the first `workspace_id` argument:

  workspace_id = "{workspace_id}"

The task files are under /workspace in that sandbox. Produce all required
files/changes there — anything written elsewhere (including any platform-side
workspace) will NOT be graded. A tool call with a missing or wrong
workspace_id will fail.

The task follows below.

"""


def _deep_get(data: Any, dotted: str, default: Any = None) -> Any:
    node = data
    for part in dotted.split("."):
        if not isinstance(node, dict):
            return default
        node = node.get(part)
        if node is None:
            return default
    return node


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


class AdpAgent(BaseAgent):
    """ADP platform agent (external/remote harness) for harbor."""

    SUPPORTS_ATIF: bool = True

    def __init__(self, logs_dir: Path, *args, **kwargs):
        # ── harness knobs (bench kwargs; static values from configs) ──
        self._adp_version: str = str(kwargs.pop("ADP_VERSION", None) or "unknown")
        self._chat_path: str = str(kwargs.pop("ADP_CHAT_PATH", None) or "/adp/v2/chat")
        timeout = kwargs.pop("ADP_TIMEOUT_SEC", None)
        self._read_timeout_sec: float = float(timeout) if timeout else 600.0
        # Consumed-and-ignored knobs injected by prepare_job for CLI harnesses.
        kwargs.pop("connection", None)
        kwargs.pop("model_params", None)
        kwargs.pop("context_window", None)
        kwargs.pop("context_compact_pct", None)
        kwargs.pop("instance_id", None)
        # Record for diagnostics; the agent addresses ADP itself.
        self._instance_id = None

        # ── secrets/endpoint resolution: env-only (never kwargs/config) ──
        self._base_url = (os.environ.get("ADP_BASE_URL") or "").rstrip("/")
        self._app_key = os.environ.get("ADP_APP_KEY") or ""
        self._bridge_url = (os.environ.get("ADP_BRIDGE_URL") or "").rstrip("/")
        self._bridge_token = os.environ.get("ADP_BRIDGE_TOKEN") or ""

        # Per-trial state.
        self._container_id: str = ""
        self._session_id = self._trial_id_from_logs_dir(logs_dir)

        super().__init__(logs_dir, *args, **kwargs)

    @staticmethod
    def _trial_id_from_logs_dir(logs_dir: Path | None) -> str:
        # logs_dir is ``.../<trial_name>/agent``; the parent's name is the
        # per-trial id used as the bridge workspace_id.
        try:
            p = Path(logs_dir or "")
        except TypeError:
            return ""
        return p.parent.name if p.name == "agent" else ""

    # ── BaseAgent interface ──────────────────────────────────────────

    @staticmethod
    def name() -> str:
        return "adp"

    def version(self) -> str | None:
        return self._adp_version

    async def setup(self, environment: BaseEnvironment) -> None:
        missing = [
            name for name, value in (
                ("ADP_BASE_URL", self._base_url),
                ("ADP_APP_KEY", self._app_key),
                ("ADP_BRIDGE_URL", self._bridge_url),
            ) if not value
        ]
        if missing:
            raise RuntimeError(
                "AdpAgent: missing required environment variables: "
                + ", ".join(missing)
                + " (set them in .env; ADP_APP_KEY is a secret and is only "
                  "read from the environment)"
            )
        # The sandbox container's hostname is its docker container id — the
        # stable handle the bridge needs for `docker exec`.
        result = await environment.exec("cat /etc/hostname", timeout_sec=30)
        container_id = (result.stdout or "").strip()
        if result.return_code != 0 or not container_id:
            raise RuntimeError(
                "AdpAgent: could not determine the sandbox container id "
                f"(cat /etc/hostname → rc={result.return_code}, "
                f"stderr={ (result.stderr or '')[:200] })"
            )
        self._container_id = container_id

    async def run(
        self, instruction: str, environment: BaseEnvironment, context: AgentContext
    ) -> None:
        if not self._container_id:
            await self.setup(environment)

        workspace_id = self._session_id or f"wbb-{uuid.uuid4().hex[:12]}"
        await self._register_workspace(workspace_id)
        try:
            reply, events, meta = await self._chat_via_adp(
                _BRIDGE_PREAMBLE.format(workspace_id=workspace_id) + instruction
            )
        finally:
            calls = await self._pull_bridge_calls(workspace_id)
            await self._unregister_workspace(workspace_id)

        # Persist raw artifacts next to the other agent logs.
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        (self.logs_dir / _REPLY_FILENAME).write_text(reply, encoding="utf-8")
        with open(self.logs_dir / _EVENTS_FILENAME, "w", encoding="utf-8") as fh:
            for ev in events:
                fh.write(json.dumps(ev, ensure_ascii=False) + "\n")
        if calls is not None:
            (self.logs_dir / _BRIDGE_CALLS_FILENAME).write_text(
                json.dumps(calls, ensure_ascii=False, indent=2), encoding="utf-8"
            )

        # Context + trajectory (context is filled here: BaseAgent subclasses
        # are expected to populate it as they execute).
        self._write_trajectory(calls or [], meta)
        context.n_input_tokens = meta.get("input_tokens") or 0
        context.n_output_tokens = meta.get("output_tokens") or 0
        context.n_cache_tokens = 0
        if meta.get("total_cost_usd") is not None:
            context.cost_usd = float(meta["total_cost_usd"])
        if meta.get("error"):
            self.logger.warning("ADP chat ended with error: %s", meta["error"])

    # ── bridge admin API ─────────────────────────────────────────────

    def _bridge_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._bridge_token:
            headers["X-Bridge-Token"] = self._bridge_token
        return headers

    async def _register_workspace(self, workspace_id: str) -> None:
        body = {
            "workspace_id": workspace_id,
            "container_id": self._container_id,
        }
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self._bridge_url}/admin/workspaces",
                json=body, headers=self._bridge_headers(),
            )
        if resp.status_code >= 300:
            raise RuntimeError(
                f"AdpAgent: bridge workspace registration failed "
                f"(HTTP {resp.status_code}): {resp.text[:300]}"
            )

    async def _unregister_workspace(self, workspace_id: str) -> None:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                await client.delete(
                    f"{self._bridge_url}/admin/workspaces/{workspace_id}",
                    headers=self._bridge_headers(),
                )
        except httpx.HTTPError as exc:
            self.logger.debug("bridge unregister failed (non-fatal): %s", exc)

    async def _pull_bridge_calls(self, workspace_id: str) -> list | None:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(
                    f"{self._bridge_url}/admin/workspaces/{workspace_id}/calls",
                    headers=self._bridge_headers(),
                )
            if resp.status_code == 200:
                return list(resp.json().get("calls") or [])
        except httpx.HTTPError as exc:
            self.logger.warning("could not pull bridge call journal: %s", exc)
        return None

    # ── ADP chat (SSE) ───────────────────────────────────────────────

    async def _chat_via_adp(self, prompt: str) -> tuple[str, list[dict], dict]:
        request_id = str(uuid.uuid4())
        conversation_id = str(uuid.uuid4())
        visitor_id = f"wbbench-{self._session_id or uuid.uuid4().hex[:8]}"
        # RequestV2 (PascalCase). `AppKey` covers the experience endpoint and
        # `BotAppKey` the released-app endpoint; the server ignores whichever
        # it does not read, so both are sent for endpoint tolerance.
        body = {
            "RequestId": request_id,
            "ConversationId": conversation_id,
            "Contents": [{"Type": "text", "Text": prompt}],
            "VisitorId": visitor_id,
            "Incremental": True,
            "IsEvaluateTest": True,
            "AppKey": self._app_key,
            "BotAppKey": self._app_key,
        }
        meta: dict[str, Any] = {
            "request_id": request_id,
            "conversation_id": conversation_id,
            "trace_id": "",
            "input_tokens": 0,
            "output_tokens": 0,
            "model_name": "",
            "error": None,
        }
        events: list[dict] = []
        reply_parts: list[str] = []
        url = f"{self._base_url}{self._chat_path}"
        timeout = httpx.Timeout(connect=30, read=self._read_timeout_sec,
                                write=60, pool=30)
        started = time.monotonic()
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("POST", url, json=body) as resp:
                if resp.status_code >= 300:
                    detail = (await resp.aread()).decode("utf-8", "replace")[:500]
                    raise RuntimeError(
                        f"AdpAgent: ADP chat failed (HTTP {resp.status_code}) "
                        f"at {url}: {detail}"
                    )
                async for event_name, data in self._iter_sse(resp):
                    events.append({"event": event_name, "data": data})
                    self._apply_event(event_name, data, reply_parts, meta)
        meta["elapsed_ms"] = int((time.monotonic() - started) * 1000)
        return "".join(reply_parts), events, meta

    @staticmethod
    async def _iter_sse(resp: httpx.Response):
        """Yield (event_name, data_dict) from an SSE stream.

        Tolerates both standard SSE blocks (``event:`` + ``data:``) and bare
        per-line JSON (no ``event:`` prefix), mirroring eval-demo's parser.
        """
        event_name = ""
        data_lines: list[str] = []

        def flush():
            nonlocal event_name, data_lines
            if not (event_name or data_lines):
                return None
            data_str = "\n".join(data_lines).strip()
            name, data = event_name, {}
            if data_str == "" or data_str == "[DONE]":
                name = name or "done"
            else:
                try:
                    data = json.loads(data_str)
                except json.JSONDecodeError:
                    data = {"_raw": data_str}
                if not name and isinstance(data, dict):
                    name = str(data.get("Type", "") or "")
            event_name, data_lines = "", []
            return (name, data)

        async for raw in resp.aiter_lines():
            line = raw.rstrip("\r\n")
            if line == "":
                item = flush()
                if item:
                    yield item
                continue
            if line.startswith("event:"):
                event_name = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data_lines.append(line[len("data:"):].strip())
            elif line.startswith(":"):
                continue  # SSE comment
            else:
                data_lines.append(line)
        item = flush()
        if item:
            yield item

    @staticmethod
    def _apply_event(event_name: str, data: dict,
                     reply_parts: list[str], meta: dict) -> None:
        if event_name == "text.delta":
            text = data.get("Text")
            if isinstance(text, str):
                reply_parts.append(text)
        elif event_name == "text.replace":
            text = data.get("Text")
            if isinstance(text, str):
                reply_parts[:] = [text]
        elif event_name == "response.completed":
            stat = data.get("StatInfo") if isinstance(data.get("StatInfo"), dict) else data
            meta["trace_id"] = str(
                _deep_get(data, "TraceId") or _deep_get(stat, "TraceId") or ""
            )
            meta["input_tokens"] = _to_int(
                _deep_get(stat, "InputTokens") or _deep_get(data, "InputTokens")
            )
            meta["output_tokens"] = _to_int(
                _deep_get(stat, "OutputTokens") or _deep_get(data, "OutputTokens")
            )
            meta["model_name"] = str(
                _deep_get(stat, "ModelName") or _deep_get(data, "ModelName") or ""
            )
            cost = _deep_get(stat, "TotalCost") or _deep_get(data, "TotalCost")
            if isinstance(cost, (int, float)):
                meta["total_cost_usd"] = float(cost)
        elif event_name == "error":
            code = _deep_get(data, "Code") or _deep_get(data, "Error.Code")
            msg = _deep_get(data, "Message") or _deep_get(data, "Error.Message") or ""
            meta["error"] = f"{code}: {msg}" if code else (msg or "unknown error")
            meta["trace_id"] = str(_deep_get(data, "TraceId") or meta.get("trace_id", ""))

    # ── trajectory ───────────────────────────────────────────────────

    def _write_trajectory(self, calls: list[dict], meta: dict) -> None:
        steps: list[Step] = []
        for idx, call in enumerate(calls, start=1):
            tool = str(call.get("tool") or "unknown")
            rc = call.get("return_code")
            summary = " ".join(
                f"{k}={v!r}" for k, v in (call.get("arguments") or {}).items()
            )[:200]
            steps.append(Step(
                step_id=idx,
                source="agent",
                model_name=self.model_name,
                message=f"{tool}({summary}) [exit code: {rc}]",
                tool_calls=[ToolCall(
                    tool_call_id=f"bridge-{idx}",
                    function_name=tool,
                    arguments=call.get("arguments") or {},
                )],
                llm_call_count=0,  # no per-call LLM usage from a remote platform
            ))
        final_metrics = FinalMetrics(
            total_prompt_tokens=int(meta.get("input_tokens") or 0),
            total_completion_tokens=int(meta.get("output_tokens") or 0),
            total_cached_tokens=0,
            total_cost_usd=meta.get("total_cost_usd"),
            total_steps=len(steps),
            extra={
                "bridge_tool_calls": len(steps),
                "adp_trace_id": meta.get("trace_id") or "",
                "adp_request_id": meta.get("request_id") or "",
                "adp_conversation_id": meta.get("conversation_id") or "",
                "adp_error": meta.get("error"),
                "adp_elapsed_ms": meta.get("elapsed_ms"),
            },
        )
        trajectory = Trajectory(
            session_id=meta.get("conversation_id"),
            agent=Agent(name="adp", version=self._adp_version,
                        model_name=self.model_name),
            steps=steps,
            final_metrics=final_metrics,
            notes=(
                "Remote-platform harness: tool steps come from the eval "
                "bridge's journaled wb_* calls; token/cost figures come from "
                "the platform's response.completed StatInfo (aggregated over "
                "the whole conversation, no per-step usage)."
            ),
        )
        try:
            with open(self.logs_dir / _TRAJECTORY_FILENAME, "w",
                      encoding="utf-8") as fh:
                json.dump(trajectory.to_json_dict(), fh, indent=2,
                          ensure_ascii=False)
        except (OSError, AttributeError) as exc:
            self.logger.debug("Failed writing trajectory.json: %s", exc)
