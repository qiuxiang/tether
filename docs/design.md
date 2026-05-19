# Tether — 远程环境管理服务设计文档

**Status:** Implemented (initial release)

## 1. 概述

Tether 把一组防火墙后的设备（mac/linux/windows）通过出站长连接"拴"到一台公网中继机，并把它们作为可执行环境暴露给 AI（Claude Code 等 MCP 客户端）。AI 可以列出已注册设备、在某设备上执行一次性命令、启动并管理常驻进程、收发 stdin、抓取虚拟终端屏幕、在设备和本机之间传文件——本质上是"给 AI 的 ssh + 轻量进程管理器 + scp"，但不需要每台设备自己有公网或隧道。

**目标用例（个人使用）：**
- AI 在 mac 上跑 `flutter run` 并通过 stdin 发送 `r`/`R`/`h` 触发热重载
- AI 在 linux 工作机跑 build/test，常驻几小时，中途多次抓屏看进度
- AI 在树莓派/windows 机器跑一次性诊断命令
- AI 把构建产物从一台 node 推到另一台 node，或拉回本地

**显式非目标：**
- ❌ Web UI / dashboard
- ❌ 多租户 / 多用户隔离
- ❌ Process group / job control（扁平 process 列表）
- ❌ 目录递归传输（单文件，需要打包请先 tar/zip）

## 2. 架构

```
┌─────────────────┐   stdio MCP    ┌──────────────────────┐   WSS + CBOR   ┌──────────────────┐
│  Claude Code    │ ◄────────────► │  tether mcp (本机)   │ ◄────────────► │  Tether Hub      │
│ (本地 AI host)  │                │  - WSS client (/client)              │  (公网机)        │
└─────────────────┘                └──────────────────────┘                │  - /device  WS   │
                                                                          │  - /client  WS   │
                                                                          │  - /health       │
                                                                          └─────────┬────────┘
                                                                                    │ WSS + CBOR
                                                                                    ▼
                                                                          ┌──────────────────┐
                                                                          │  Tether Node     │
                                                                          │ (mac/win/linux)  │
                                                                          │  - PTY 子进程    │
                                                                          │  - VT 渲染       │
                                                                          │  - 本地 log 文件 │
                                                                          └──────────────────┘
```

- **一份代码三种角色**：同一个 Go 二进制 `tether`，子命令切换
  - `tether serve`：中继 hub（公网机）
  - `tether join`：node（被管设备，常驻）
  - `tether mcp`：stdio MCP server（Claude Code 本地启动）
- **传输**：全链路 WSS + CBOR 二进制帧（`github.com/fxamacker/cbor/v2`）
- **MCP**：`tether mcp` 是 stdio MCP server，Claude Code 直接 spawn 它；Hub **不**自己跑 MCP server
- **认证**：共享 `token`，node 和 client 都用同一个 secret，握手时 `Hello.token` 校验；`Hello.role` 区分 `node` 与 `client`
- **TLS**：由用户自己的反代（nginx/caddy）处理；tether hub 本身只跑 HTTP
- **端口**：单端口（默认 `:7000`），path 区分 `/device` / `/client` / `/health`

## 3. 组件

### Tether Hub（`tether serve`）

- **设备注册表**：内存 `map[hostname]Device`，记录在线状态、`last_seen`、OS/arch
- **客户端注册表**：当前连接的 MCP client（`tether mcp` 实例）
- **WS 处理器**：`/device` 接 node、`/client` 接 MCP client；握手校验 token + role；node 上 hostname 唯一
- **Router**：纯转发器。Client → Hub 的 frame 按 `target`（hostname）路由到对应 node 的 WS；Node → Hub 的 reply/event 按 `msg_id` 反向路由回正确的 client
- **Relay Coordinator**：协调 node↔node 文件中转（`file_relay`），把源端的 `file_chunk` 流转给目标端
- **本地工具**：`list_devices` 在 hub 上直接答（无需打 node）
- **离线处理**：node WS 断开立即标记 offline；后续打该 node 的调用返回 `device_offline`

### Tether Node（`tether join`）

- **WS 客户端**：到 Hub 的持久连接（path `/device`），断线指数退避重连
- **进程注册表**：内存 `map[process_id]Process`，每条含 `cmd, status, started_at, last_active_at, exit_code, log_path, name?`
- **本地日志**：每个进程一个 append-only 文件 `<log_dir>/<process_id>.log`
- **保留策略**：按 `last_active_at` LRU，cap 50；只淘汰 `exited`/`killed` 的，同步删 log；`running` 永不淘汰
- **PTY**：跨平台 `github.com/aymanbagabas/go-pty`（unix PTY + Windows ConPTY）；所有子进程**始终**在 PTY 内拉起，TIOCGWINSZ 报告 200×50 给子进程（curses 程序看到正常 canvas）
- **虚拟终端**：`github.com/hinshun/vt10x`，200 列 × 10000 行 scrollback，喂入子进程的 stdout/stderr 字节流；`capture_screen` 从里面读渲染后的文本
- **文件 IO**：直接读写本地路径，chunk size 256 KiB，全程 sha256 校验

### Tether MCP Client（`tether mcp`）

- **stdio MCP server**：基于 `github.com/mark3labs/mcp-go`，对外暴露 8 个工具
- **WS 客户端**：到 Hub `/client` 的持久连接，role=`client`
- **RPC 路由**：每次工具调用生成 `msg_id`，注册 reply channel，发出 frame；收到 reply/stream 后转回 MCP

## 4. Wire Protocol（统一 client/node ↔ hub）

**传输**：WSS binary frames，CBOR 编码。同一套 envelope 在 node 和 client 两侧通用：`target` 字段决定 hub 转发去哪台 node（client 发出时填，node 发出时省略；hub 按 `msg_id` 反向路由 reply）。

### 控制 / 进程类

**Client → (Hub →) Node：**

| `type` | 字段 | 说明 |
|---|---|---|
| `exec` | `msg_id, target, cmd, cwd?, env?, stdin?, timeout_ms?` | 一次性命令，PTY 内运行；输出喂入 vt10x，进程退出后渲染整屏一次性回传 |
| `exec_cancel` | `msg_id, target` | 取消进行中的 exec |
| `start` | `msg_id, target, process_id, cmd, cwd?, env?, name?` | 启动常驻进程 |
| `stdin` | `target, process_id, data` | fire-and-forget |
| `kill` | `msg_id, target, process_id, signal?` | 终止进程 |
| `capture_screen` | `msg_id, target, process_id, start_line?, end_line?` | 读取虚拟终端渲染结果（tmux 风格行号） |
| `list` | `msg_id, target, status_filter?, limit?` | 列本设备进程 |
| `list_devices` | `msg_id` | hub-local：列已注册设备 |

**Node → (Hub →) Client：**

| `type` | 字段 | 说明 |
|---|---|---|
| `hello` | `hostname, os, arch, agent_version, token, role?` | 握手（node 必填 hostname；client 用 `role:"client"`） |
| `reply` | `msg_id, ok, error?, data?` | 通用应答 |
| `exec_output` | `msg_id, stream, data` | exec 结束后单帧回传渲染后的整屏（仍走这个 frame 以保持 envelope 形状） |
| `exec_exit` | `msg_id, code, error?` | exec 结束 |
| `event` | `kind, process_id, code?` | 异步事件（后台进程 exit） |

**PTY 模式下：** stdout/stderr 合并到 `stdout`（PTY 物理合并）；`data` 含 ANSI 转义。`stdin` 不返回 reply：进程已退出时静默丢弃，下一次 `list`/`capture_screen` 看到状态变化。

### 文件传输类

| `type` | 方向 | 说明 |
|---|---|---|
| `file_get_open` | client→node | 下载请求；node 先 reply 元数据，再 push `file_chunk` 直到 EOF |
| `file_put_open` | client→node | 上传请求；node reply 就绪后 client push `file_chunk`；node 收齐校验 sha256 再发最终 reply |
| `file_chunk` | 双向 | 流式分片，按 `msg_id` 关联，`seq` 计数，`eof` 标记最后一帧 |
| `file_abort` | 双向 | 取消传输 |
| `file_relay` | client→hub | hub 协调 node↔node：起一对 get/put，相互转 chunk |
| `file_local_copy` | client→node | 同 node 两路径间复制 |

Chunk size 256 KiB。每次传输返回 `{ok, bytes, sha256, duration_ms}`。

### 端口转发类

`stream_id` 由发起 accept 的一侧生成 uuid，全局唯一；`forward_id` 由 client 启动时为每条规则分配。

**Client → (Hub →) Node：**

| `type` | 字段 | 说明 |
|---|---|---|
| `forward_listen` | `msg_id, target, forward_id, listen_addr, dest_host, dest_port` | -R 规则：让 node 在 `listen_addr` 起 TCP listener。reply `{ok, error?}`。Hub 记 `forward_id → client_ws` |
| `forward_unlisten` | `msg_id, target, forward_id` | 关闭 node 上对应 listener；Hub 同步清 listeners 表 |
| `forward_dial` | `msg_id, target, stream_id, dest_host, dest_port` | -L 路径：node 拨号 `dest_host:dest_port`，reply `{ok, error?}`。Hub 记 `stream_id → (client_ws, node_ws)` |
| `forward_data` | `target, stream_id, data` | 双向；fire-and-forget；Hub 按 `stream_id` 转发。单帧软上限 32 KiB |
| `forward_close` | `target, stream_id, half?` | `half ∈ {"read","write","both"}`（缺省 `"both"`）；fire-and-forget |

**Node → (Hub →) Client：**

| `type` | 字段 | 说明 |
|---|---|---|
| `forward_dial` | `msg_id, stream_id, forward_id` | -R 路径：node accept 后请 client 拨号；client 用 `forward_id` 查本地 dest 信息 dial。Hub 按 `forward_id → client_ws` 路由 |
| `forward_data`, `forward_close` | 同上 | 对称 |

**Hub → Client 事件（扩展 `event.kind`）：**

| `kind` | 字段 | 说明 |
|---|---|---|
| `device_online` | `device` | node 完成握手、注册成功时推送给所有 client；client 用此事件重发 -R 规则的 `forward_listen` |
| `device_offline` | `device` | node WS 断开时推送；client 标记相关 -R 规则待恢复 |

## 5. 进程模型

**两类原语：**

| | `exec` | `start_process` |
|---|---|---|
| 用途 | 一次性命令 | 常驻任务 |
| 输出处理 | 字节流喂入一次性 vt10x，退出后返回渲染后的整屏 + exit_code | 写 node 本地 log；AI 通过 `capture_screen` 读渲染结果，或 `file_transfer` 拉原始 log 文件 |
| 生命周期 | 跟 MCP 调用绑定 | 独立于 MCP 调用，跨连接存活 |
| 默认 timeout | 60s | 无 |

**边界规则：** 期望几秒内返回 → `exec`；需要跨调用存活、需要 stdin 交互、需要看实时屏幕 → `start_process`。

**PTY 模式（始终开启，无 tty 开关）：**
- 所有进程在 PTY 内拉起，stdin raw（AI 发 `"r"` 单字符即被收到）
- stdout/stderr 合并；输出含 ANSI 转义
- TIOCGWINSZ 报告 200×50 给子进程；VT 内部 buffer 200×10000 scrollback
- pager/REPL/prompt 由调用方 `capture_screen` + `send_stdin` 处理

**Process 状态机：**

```
        start_process
            │
            ▼
        running ───── exit ────► exited (code, exit_at)
            │                         │
            │ kill                    │
            └──► killed ──────────────┤
                                      ▼
                          (LRU 淘汰，仅在超过 cap 50 时；只动非 running)
```

**Node 重启时：** 不恢复任何 running 进程（node 是父进程）；exited 的 log 文件留磁盘但不进 in-memory map。

**设备断网时：** 进行中的进程不被 kill（node 仍在跑，断的是它和 hub）；重连后通过下一次 `list` 同步状态。MCP 在断网期间对该 node 的所有调用返回 `device_offline`。

## 6. MCP 工具表面

| Tool | 参数 | 返回 |
|---|---|---|
| `list_devices` | — | `[{hostname, os, arch, online, last_seen}]` |
| `exec` | `device, cmd, cwd?, env?, stdin?, timeout?=60` | `{stdout, stderr, exit_code, timed_out}` |
| `start_process` | `device, cmd, cwd?, env?, name?` | `{process_id}` |
| `list_processes` | `device?, limit?=50, status_filter?` | `[{process_id, name?, cmd, status, started_at, last_active_at, exit_code?, log_path, device}]` |
| `capture_screen` | `device, process_id, start_line?, end_line?` | `{lines, cursor:{row,col}, cols, total_lines}` |
| `send_stdin` | `device, process_id, data` | `{ok}` |
| `kill_process` | `device, process_id, signal?="TERM"` | `{ok}` |
| `file_transfer` | `from, to, overwrite?=false` | `{ok, bytes, sha256, duration_ms}` |

**约定：**
- `cmd` 字符串经 `sh -c` 执行（exec / start_process 都一样）
- `device` 用 hostname 标识，注册时 hostname 冲突拒绝
- `process_id` 由 client 生成 UUID，是稳定 handle
- `name` 可选可读标签，仅供 list 显示
- `list_processes` 省略 `device` 时由 client 端 fan-out 到所有 device，结果加 `device` 字段
- `capture_screen` 用 tmux 行号：负数从末尾倒数，省略表示极值（top/bottom）；VT 上限 10000 行 scrollback，超出部分仍在 log 文件里，通过 `list_processes.log_path` + `file_transfer` 取回
- `file_transfer` 路径语法：`node:/abs/path` 或 `node:~/path` 表示某 node 路径；`/abs/path` 或 `~/path` 表示运行 `tether mcp` 这台机器上的路径。`from == to == 本地` 拒绝（直接用 OS 工具）；同 node 走 `file_local_copy`；跨 node 走 `file_relay`
- 没有 `cleanup_process`：保留策略由 node LRU 自动处理
- 没有 `attach`/`detach`：所有交互 stateless RPC
- 没有 `restart`：AI 自己组合 `kill_process` + `start_process`

## 7. 部署 / 配置

**二进制 + 子命令：**

```bash
tether serve              # 中继 hub（公网机）
tether join               # node，连入中继（被管设备）
tether mcp                # stdio MCP server（Claude Code 本地起）
```

**Hub 配置（`/etc/tether/config.yaml`）：**

```yaml
listen: ":7000"
token: "xxx"
# 路径：
#   /device   node WS endpoint
#   /client   MCP client WS endpoint
#   /health   liveness probe (no auth)
```

**Node 配置（`~/.config/tether/config.yaml`）：**

```yaml
hub_url: "wss://tether.example.com/device"
token: "xxx"
hostname_override: ""                  # 默认 os.Hostname()
log_dir: "~/.local/share/tether/logs"
```

**MCP client 配置（`~/.config/tether/client.yaml`）：**

```yaml
hub_url: "wss://tether.example.com/client"
token: "xxx"
```

**Claude Code MCP 配置：**

```json
{
  "mcpServers": {
    "tether": {
      "command": "/usr/local/bin/tether",
      "args": ["mcp"]
    }
  }
}
```

**Service 集成：**
- Linux：systemd unit 模板放 `dist/systemd/`（`tether-hub.service`、`tether-node.service`、`tether-mcp.service`）
- macOS / Windows：先只出文档，不自动化

**反代（用户自管）：** 单 upstream 指 `localhost:7000`，nginx/caddy 处理 TLS。

## 8. 实现栈

- **语言**：Go（cross-compile 出 linux/mac/windows × amd64/arm64）
- **关键依赖**：
  - `github.com/coder/websocket`（WS）
  - `github.com/fxamacker/cbor/v2`（CBOR 编解码）
  - `github.com/aymanbagabas/go-pty`（跨平台 PTY / ConPTY）
  - `github.com/hinshun/vt10x`（虚拟终端渲染）
  - `github.com/mark3labs/mcp-go`（MCP server SDK）
  - `gopkg.in/yaml.v3`（配置）

## 9. 后续可扩展项（不在初版范围）

按优先级：
1. **Per-device token** + 单设备吊销（当前共享 token）
2. **设备掉线期间的 output 补传**（当前掉线即 `device_offline`）
3. **目录 / glob 文件传输**（当前单文件）
4. **Process group / 标签筛选**（当前扁平 list）
5. **多租户**

## 10. 参考文档

- [`docs/superpowers/specs/2026-05-19-port-forwarding-design.md`](superpowers/specs/2026-05-19-port-forwarding-design.md) — 端口转发详细设计（背景、架构、配置语法、wire protocol、错误处理、测试计划）
