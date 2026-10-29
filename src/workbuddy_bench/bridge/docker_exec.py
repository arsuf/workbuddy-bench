"""Async ``docker exec`` plumbing for the eval bridge.

The bridge runs on the bench host (where the Docker daemon lives) and forwards
tool calls into the per-trial sandbox container. Every command is executed via
``docker exec ... sh -c <script>`` with argument-list construction only — the
host-side command line is never assembled through a shell, so container ids and
paths cannot break out of the exec invocation itself. The *script* runs inside
the one-shot eval sandbox, which is exactly the evaluated surface (identical to
what cbc/claude-code harnesses do with ``environment.exec``).
"""

from __future__ import annotations

import asyncio
import shlex
from dataclasses import dataclass


class DockerExecError(Exception):
    """docker CLI failed to run at all (daemon down / container missing)."""


@dataclass(slots=True)
class ExecOutcome:
    return_code: int
    stdout: bytes
    stderr: bytes


async def docker_exec(
    container_id: str,
    script: str,
    *,
    stdin: bytes | None = None,
    timeout_sec: float = 120.0,
    user: str | None = None,
) -> ExecOutcome:
    """Run ``sh -c <script>`` inside the container, returning the raw outcome.

    Raises :class:`DockerExecError` when the docker CLI itself fails (bad
    container id, daemon unreachable, …) — distinct from a non-zero exit of the
    script, which is returned as a normal outcome so the agent sees stderr.
    """
    argv = ["docker", "exec", "-i"]
    if user:
        argv += ["-u", user]
    argv += [container_id, "sh", "-c", script]

    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(input=stdin), timeout=timeout_sec)
    except asyncio.TimeoutError:
        # wait_for cancels the task; make sure the process is reaped.
        try:
            proc.kill()
            await proc.wait()
        except ProcessLookupError:
            pass
        raise
    if proc.returncode is None:  # pragma: no cover - defensive
        raise DockerExecError("docker exec finished without a return code")
    return ExecOutcome(return_code=proc.returncode, stdout=out or b"", stderr=err or b"")


def q(path: str) -> str:
    """Shell-quote a path for embedding into the container-side script."""
    return shlex.quote(path)
