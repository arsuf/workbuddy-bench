"""CLI entry: start the eval bridge server.

    uv run python -m workbuddy_bench.bridge --host 0.0.0.0 --port 8931

See the package README for the ADP-side MCP mounting and the public-URL
(tunnel) requirements.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import uvicorn

from workbuddy_bench.bridge.server import create_app


def main() -> int:
    parser = argparse.ArgumentParser(description="WorkBuddy Bench eval bridge")
    parser.add_argument("--host", default=os.environ.get("WB_BRIDGE_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int,
                        default=int(os.environ.get("WB_BRIDGE_PORT", "8931")))
    parser.add_argument("--log-dir", type=Path,
                        default=Path(os.environ.get("WB_BRIDGE_LOG_DIR", "logs/bridge")))
    parser.add_argument("--token", default=None,
                        help="Admin API token (X-Bridge-Token). Defaults to "
                             "$WB_BRIDGE_TOKEN; unset = dev mode (no auth).")
    parser.add_argument("--ttl-hours", type=float, default=24.0,
                        help="Expire registered workspaces after this many "
                             "hours (default 24).")
    args = parser.parse_args()

    token = args.token if args.token is not None else \
        (os.environ.get("WB_BRIDGE_TOKEN", "").strip() or None)
    if token is None and args.host not in ("127.0.0.1", "localhost"):
        print("WARNING: no WB_BRIDGE_TOKEN and binding on a non-loopback "
              "interface — anyone who can reach this port can register "
              "containers / run commands in eval sandboxes.", flush=True)

    app = create_app(
        log_dir=args.log_dir,
        ttl_sec=int(args.ttl_hours * 3600),
        bridge_token=token,
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
