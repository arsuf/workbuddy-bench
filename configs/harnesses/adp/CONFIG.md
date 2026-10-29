# ADP harness — mechanism notes

## What is being evaluated

The ADP **platform end-to-end**: its agent runtime (planning, tool use),
its model configuration, and the app's MCP setup. Unlike `codebuddy-code` /
`claude-code` (CLI inside the task container), the ADP agent runs server-side
in ADP's own cloud sandbox — so the bench must bridge the workspace gap:

    AdpAgent (host, BaseAgent)
      ├─ POST {ADP_BRIDGE_URL}/admin/workspaces   (trial → container id)
      ├─ POST {ADP_BASE_URL}/adp/v2/chat (SSE)    (task → ADP app)
      │      ADP agent ── MCP /mcp ──▶ eval bridge ── docker exec ──▶ sandbox
      ├─ GET  {ADP_BRIDGE_URL}/admin/workspaces/{id}/calls  (trajectory)
      └─ DELETE /admin/workspaces/{id}             (cleanup)

The task instruction is prefixed with a preamble telling the remote agent to
do every file/shell operation through the `wb_*` MCP tools with the trial's
`workspace_id`. Verifier, rewards, proxy, sharding and reporting are reused
unchanged.

## One-time setup

1. **Eval bridge** — start it on the bench host and expose it to ADP's
   egress (tunnel with access control, or a server with firewall rules):

   ```bash
   WB_BRIDGE_TOKEN=... uv run python -m workbuddy_bench.bridge \
       --host 0.0.0.0 --port 8931
   ```

   See `src/workbuddy_bench/bridge/README.md`.

2. **ADP app** — create/release a benchmark app and mount a Streamable HTTP
   MCP server pointing at `https://<bridge-public-host>/mcp`. The app must be
   released (published), since `/adp/v2/chat` serves release-scene apps; use
   `/adp/v2/chat/experience` (override `ADP_CHAT_PATH`) for draft apps.

3. **.env**:

   ```
   ADP_BASE_URL=https://<adp-gateway-host>
   ADP_APP_KEY=<released app's BotAppKey>     # secret, env-only
   ADP_BRIDGE_URL=http://127.0.0.1:8931       # bridge admin base
   ADP_BRIDGE_TOKEN=...                       # must match WB_BRIDGE_TOKEN
   ```

4. **Job** — copy `configs/jobs/_example-adp-code.yaml` and adjust the
   dataset path. Run with `uv run ./scripts/run.sh --job <slug>`.

## Dataset compatibility

Datasets that declare `harness_delivery = "split-mount"` in `dataset.toml`
will refuse this harness (it has no mount block). For those, add a per-harness
exception to the dataset's `dataset.toml`:

```toml
[runtime]
harness_delivery = "split-mount"
harness_delivery_by_harness = { adp = "remote" }
```

## Known limitations / follow-ups

- **Token accounting** is the platform's aggregated StatInfo from
  `response.completed` — no per-step usage, no cache-token breakdown, and no
  bench-proxy insertion (ADP manages the model hop). Cost is only recorded if
  the platform reports it.
- **Model params** (`model.params`, `extra_body`) are ignored: ADP owns
  sampling. Keep `model_connection: direct`; `local_proxy` would try to point
  a CLI at the proxy and is meaningless here.
- **Fairness vs CLI harnesses**: tool surface is `wb_*` only (7 tools); the
  CLI harnesses have richer built-ins. Compare within-harness or accept the
  confound. The bridge executes as root by default (register with
  `exec_user` if the dataset forces a non-root agent user).
- **Endpoint field names** (`AppKey` vs `BotAppKey`) are sent both ways for
  tolerance; if the gateway rejects one, file the exact request/response in
  the trial log and tighten `ADP_CHAT_PATH`/body accordingly.
