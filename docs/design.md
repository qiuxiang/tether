# Tether — 远程环境管理服务设计文档

**Date:** 2026-05-15
**Status:** Draft (approved for implementation planning)

## 1. 概述

Tether 把一组防火墙后的设备（mac/linux/windows）通过出站长连接"拴"到一台公网中继机，并把它们作为可执行环境暴露给 AI（Claude Code 等 MCP 客户端）。AI 可以列出已注册设备、在某设备上执行一次性命令、启动并管理常驻进程、收发 stdin/stdout——本质上是"给 AI 的 ssh + 轻量进程管理器"，但不需要每台设备自己有公网或隧道。

**目标用例（个人使用）：**
- AI 在 mac 上跑 `flutter run` 并通过 stdin 发送 `r`/`R`/`h` 触发热重载
- AI 在 linux 工作机跑 build/test，常驻几小时，中途多次拉日志看进度
- AI 在树莓派/windows 机器跑一次性诊断命令

**显式非目标：**
- ❌ 文件传输（scp 等价物）
- ❌ 端口转发（ssh -L 等价物）
- ❌ Web UI / dashboard
- ❌ 多租户 / 多用户隔离
- ❌ Process group / job control（扁平 process 列表）

## 2. 架构

```
┌─────────────────┐                  ┌──────────────────────────────┐   WSS + CBOR     ┌──────────────────┐
│  Claude Code    │ ── HTTP/SSE ───► │  Tether Hub (公网机)         │ ◄─────────────── │ Tether Agent     │
│ (linux 工作机)  │   MCP over HTTP  │   - /device  WS endpoint     │  长连接 + 多路复用│ (mac/win/linux)  │
│  + 其它 MCP host│                  │   - /mcp     MCP HTTP/SSE    │                  │  - 进程封装       │
└─────────────────┘                  │   - /health                  │                  │  - 日志写本地文件 │
                                     └──────────────────────────────┘                  └──────────────────┘
```

- **两个角色，一份代码**：同一个 Go 二进制 `tether`，子命令切换 `tether serve` / `tether agent`
- **传输**：设备 ↔ Hub 用 WSS + CBOR 二进制帧；Claude ↔ Hub 用 HTTP/SSE（标准 MCP transport）
- **认证**：共享 `token`，设备和 MCP 客户端都用同一个 secret（不同 endpoint 校验）
- **TLS**：由用户自己的反代（nginx/caddy）处理；tether 本身只跑 HTTP
- **端口**：单端口（默认 `:7000`），通过 path 区分 `/device` / `/mcp` / `/health`

## 3. 组件

### Tether Hub（中继 + MCP server，同进程）

- **设备注册表**：内存数据结构 `map[hostname]Device`，记录在线状态、`last_seen`、操作系统信息
- **WS 处理器**：每个连接对应一台设备；握手时校验 token + hostname 唯一性
- **MCP server**：实现 §6 列出的 7 个工具，每个工具内部翻译成对 Agent 的 WS 请求
- **请求路由**：MCP 工具调用 → Hub 生成 `msg_id` → 通过该设备的 WS 发出 → 等 reply（或流式 chunks）→ 返回给 MCP 客户端
- **离线处理**：设备 WS 断开后立即标记 offline；后续对该设备的 MCP 调用直接返回 `device_offline`

### Tether Agent（设备端常驻进程）

- **WS 客户端**：到 Hub 的持久连接，心跳 30s，断线指数退避重连
- **进程注册表**：内存 `map[process_id]Process`，每个条目含 `cmd, status, started_at, last_active_at, exit_code, log_path, name?`
- **本地日志**：每个 process 一个 append-only 文件 `<log_dir>/<process_id>.log`
- **保留策略**：按 `last_active_at` 降序，cap 50；超过时只淘汰 `exited` 状态的，同步删 log 文件；`running` 永不淘汰
- **PTY 支持**：跨平台用 `github.com/aymanbagabas/go-pty`（unix PTY + Windows ConPTY 统一 API）

## 4. Wire Protocol（设备 ↔ Hub）

**传输**：WSS binary frames，CBOR 编码（`github.com/fxamacker/cbor/v2`）。

**Hub → Agent（请求，带 `msg_id`）：**

| `type` | 字段 | 说明 |
|---|---|---|
| `exec` | `msg_id, cmd, cwd?, env?, stdin?, tty?, timeout?` | 一次性命令，流式回输出 |
| `exec_cancel` | `msg_id` | 取消进行中的 exec（reuse msg_id 做 handle） |
| `start` | `msg_id, process_id, cmd, cwd?, env?, tty?, name?` | 启动后台进程 |
| `stdin` | `process_id, data` | fire-and-forget，无 reply |
| `kill` | `msg_id, process_id, signal?` | 终止进程 |
| `get_output` | `msg_id, process_id, offset?, length?` | 从 log 文件按 offset 读 |
| `list` | `msg_id, status_filter?, limit?` | 列出本设备 process |
| `ping` | — | 心跳 |

**Agent → Hub：**

| `type` | 字段 | 说明 |
|---|---|---|
| `hello` | `hostname, os, arch, agent_version, token` | 连接握手 |
| `reply` | `msg_id, ok, error?, data?` | 通用应答 |
| `exec_output` | `msg_id, stream("stdout"\|"stderr"), data` | exec 期间流式输出 |
| `exec_exit` | `msg_id, code, error?` | exec 结束 |
| `event` | `kind("exit"), process_id, code` | 异步事件（后台进程结束） |
| `pong` | — | 心跳应答 |

**PTY 模式下：** stdout/stderr 合并到 `stdout`（PTY 本来就合并），`data` 包含 ANSI 转义。

**Stdin** 不返回 reply：如果进程已退出，stdin 写入静默丢弃，AI 在下次 `list`/`get_output` 看到状态变化。

## 5. 进程模型

**两类原语：**

| | `exec` | `start_process` |
|---|---|---|
| 用途 | 一次性命令 | 常驻任务 |
| 输出处理 | 流式推送，整体返回 | 写 agent 本地 log 文件，AI 按 offset 拉 |
| 生命周期 | 跟 MCP 调用绑定 | 独立于 MCP 调用，跨连接存活 |
| 默认 timeout | 60s | 无 |

**边界规则：** 期望几秒内返回 → `exec`；需要跨 MCP 调用存活、需要 stdin 交互、需要 tail/grep 日志 → `start_process`。

**TTY 模式（`tty: true`）：**
- 用 PTY 拉起进程，stdin 是 raw（AI 发 `"r"` 单字符就被 flutter 收到）
- stdout/stderr 合并；输出含 ANSI 转义
- 跨平台：unix 用 PTY，Windows 用 ConPTY（统一通过 `go-pty`）
- 默认 `tty: false`，普通 cmd 不开 PTY

**Process 状态机：**

```
        start_process
            │
            ▼
        running ──────► exited (code, exit_at)
            │ kill          │
            └──► killed     │
                            ▼
                  (按 last_active_at LRU 淘汰，仅在超过 cap 50 时)
```

**Agent 重启时：** 不恢复任何 running 进程（agent 是父进程，挂了子进程也挂）；exited 的 log 文件留磁盘但不进 in-memory map。

**设备断网时：** 进行中的 process 不被 kill（agent 仍在跑、连接断的是它和 hub）；重连后通过下次 `list` 同步状态。MCP 在断网期间对该设备的所有调用返回 `device_offline`。

## 6. MCP 工具表面

| Tool | 参数 | 返回 |
|---|---|---|
| `list_devices` | — | `[{hostname, os, arch, online, last_seen}]` |
| `exec` | `device, cmd, cwd?, env?, stdin?, tty?=false, timeout?=60` | `{stdout, stderr, exit_code, timed_out}` |
| `start_process` | `device, cmd, cwd?, env?, tty?=false, name?` | `{process_id}` |
| `list_processes` | `device?, limit?=50, status_filter?="all"` | `[{process_id, name?, cmd, status, started_at, last_active_at, exit_code?}]` |
| `get_output` | `device, process_id, offset?=0, length?=65536` | `{data, next_offset, eof}` |
| `send_stdin` | `device, process_id, data` | `{ok}` |
| `kill_process` | `device, process_id, signal?="TERM"` | `{ok}` |

**约定：**
- `device` 用 hostname 标识（AI 视角直观），注册时 hostname 冲突拒绝
- `process_id` 由 Hub 生成 UUID，是稳定 handle
- `name` 是可选可读标签，仅供 list 显示
- `data` 字段总是 raw bytes（CBOR `byte string`），MCP 序列化为 base64 字符串返回给 AI
- `get_output` 的 length 上限 1MB
- 没有 `cleanup_process`：保留策略由 agent LRU 自动处理
- 没有 `attach`/`detach`：所有交互 stateless RPC
- 没有 `restart`：AI 自己组合 `kill_process` + `start_process`

## 7. 部署 / 配置

**二进制 + 子命令：**

```bash
tether serve              # 中继 + MCP server（公网机）
tether join               # 设备端，加入中继网络（被管设备）
tether join --once --tail   # 调试：前台 + 打印 inbound/outbound 帧
```

**Hub 配置（`/etc/tether/config.yaml` 或 `~/.config/tether/config.yaml`）：**

```yaml
listen: ":7000"
token: "xxx"
# Paths served:
#   /device   WS endpoint for agents
#   /mcp      MCP HTTP/SSE
#   /health   liveness probe (no auth)
```

**设备端配置（`~/.config/tether/config.yaml`）：**

```yaml
hub_url: "wss://tether.example.com/device"
token: "xxx"
hostname_override: ""      # 默认 os.Hostname()
log_dir: "~/.local/share/tether/logs"
```

**Claude Code MCP 配置：**

```json
{
  "mcpServers": {
    "tether": {
      "transport": "http",
      "url": "https://tether.example.com/mcp",
      "headers": { "Authorization": "Bearer xxx" }
    }
  }
}
```

**Service 集成：**
- Linux：systemd unit 模板放 `dist/systemd/tether-hub.service`（跑 `serve`）和 `tether-node.service`（跑 `join`）
- macOS：launchd plist 模板放 `dist/launchd/`
- Windows：先只出文档（NSSM 或 Task Scheduler），不自动化

**反代（用户自管）：** 单 upstream 指 `localhost:7000`，nginx/caddy 处理 TLS。

## 8. 实现栈

- **语言**：Go（cross-compile 一条命令出 linux/mac/windows × amd64/arm64）
- **关键依赖**：
  - `github.com/coder/websocket` 或 `nhooyr.io/websocket`（轻量 WS）
  - `github.com/fxamacker/cbor/v2`（CBOR 编解码）
  - `github.com/aymanbagabas/go-pty`（跨平台 PTY，unix + Windows ConPTY）
  - MCP SDK：官方 Go SDK 或 `github.com/mark3labs/mcp-go`（待选型时评估）

## 9. 后续可扩展项（不在初版范围）

按优先级：
1. **Per-device token** + token 吊销（当前共享 token 不能吊销单设备）
2. **设备掉线期间的 output 补传**（当前掉线就 device_offline）
3. **Process group / 标签筛选**（当前扁平 list）
4. **多租户**（当前单租户）
