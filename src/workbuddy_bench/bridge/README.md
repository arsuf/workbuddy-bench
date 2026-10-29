# Eval Bridge — 让服务端 Agent 平台（ADP）操作判分沙箱

## 是什么

一个常驻在 bench 宿主机上的服务，由两部分组成：

- **MCP Streamable HTTP server**（`/mcp`，stateless）：暴露 `wb_bash` /
  `wb_read_file` / `wb_write_file` / `wb_edit_file` / `wb_list_dir` /
  `wb_glob` / `wb_grep` 七个工具，把调用经 `docker exec` 转发进某个评测
  沙箱容器。每个工具的第一个参数都是 `workspace_id`。
- **Admin API**（`X-Bridge-Token` 鉴权）：`AdpAgent` 在每个 trial 开始时
  `POST /admin/workspaces` 注册 `workspace_id → 容器 ID`，结束后注销；
  `GET /admin/workspaces/{id}/calls` 拉取该 trial 的工具调用流水（用于
  构建 ATIF trajectory）。

一个 bridge 实例可同时服务多个并发 trial。workspace 24h（`--ttl-hours`）
自动过期，进程重启即清空（内存态）。

## 运行

```bash
uv run python -m workbuddy_bench.bridge --host 127.0.0.1 --port 8931
```

- `--token` / `WB_BRIDGE_TOKEN`：admin API 鉴权 token（生产必设；未设且绑
  非回环地址会打 WARNING）。
- `--log-dir`：调用流水 JSONL 落盘目录（默认 `logs/bridge/`）。

## 公网可达性（关键）

ADP 的云端沙箱要主动连到这个 bridge，因此 bridge 必须能被 ADP 侧访问：

- 本地开发：用带鉴权的隧道（cloudflared / frp / ngrok 等）把本地端口映
  射成公网 URL，隧道层务必启用访问控制；或者只允许 ADP 出口网段入站。
- 服务器部署：直接绑 `0.0.0.0` + `WB_BRIDGE_TOKEN`，防火墙放行 ADP 侧
  网段。

MCP endpoint 本身不做应用层鉴权（ADP plugin-gateway 侧挂载时可自行加
header），admin API 必须带 token。

## ADP 应用侧挂载（一次性配置）

1. 在 ADP 平台上创建/选择一个被测应用，给它挂载一个 **Streamable HTTP
   MCP** 工具，URL 为 `https://<bridge 公网地址>/mcp`。
2. 应用内不要启用与 `wb_*` 冲突的本地文件/shell 工具语义（保留亦可，
   但任务指令会强制要求走 `wb_*`，否则产物不会落在判分容器里）。
3. ADP 应用的模型配置照常（评测链路对模型透明）。

> MCP 协议版本：本实现基于官方 python SDK（Streamable HTTP，
> stateless）。ADP plugin-gateway 支持版本映射（2025-06-18 及协商版本），
> 若网关侧报协议版本不兼容，先查 plugin-gateway 的版本注册表。

## 安全模型

- `docker exec` 的目标只能是已注册的容器 ID，容器是一次性评测沙箱——与
  cbc/claude-code harness 直接在同一批容器内执行 shell 等价。
- 宿主侧命令行全部走参数列表构造（无 shell 拼接），容器内脚本经
  `shlex.quote` 处理路径与模式。
- token / URL 等密钥只从环境变量读取（`WB_BRIDGE_TOKEN`）。
