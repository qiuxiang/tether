# Tether — 远程环境管理服务设计文档

**Status:** Implemented (initial release)

## 1. 概述

Tether 把一组防火墙后的设备（mac/linux/windows）通过出站长连接"拴"到一台公网中继机，并把它们作为可执行环境暴露给 AI（Claude Code 等 MCP 客户端）。AI 可以列出已注册设备、在某设备上执行命令、在设备和本机之间传文件——本质上是"给 AI 的 ssh + scp"，但不需要每台设备自己有公网或隧道。

**目标用例（个人使用）：**
- AI 在 mac 上通过 `exec` 跑 `tmux` 会话，执行 `flutter run` 并发送热重载命令
- AI 在 linux 工作机跑 build/test，多次通过 `exec` 检查进度
- AI 在树莓派/windows 机器跑一次性诊断命令
- AI 把构建产物从一台 node 推到另一台 node，或拉回本地

**显式非目标：**
- ❌ Web UI / dashboard
- ❌ 多租户 / 多用户隔离
- ❌ 内置进程管理（长期任务通过 `exec` 调用 tmux 管理）
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
                                                                          │  - exec 子进程   │
                                                                          │  - 文件 IO       │
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
- **exec 处理**：收到 `exec` frame 后，通过 `sh -c` 起普通子进程（非 PTY），独立进程组；等待退出或超时（默认 30s）后 kill 整个进程组，返回 `{stdout, stderr, exit_code, timed_out, truncated}`；stdout/stderr 各限 4 MiB，超出截断并置 `truncated=true`
- **文件 IO**：直接读写本地路径，chunk size 256 KiB，全程 sha256 校验

### Tether MCP Client（`tether mcp`）

- **stdio MCP server**：基于 `github.com/mark3labs/mcp-go`，对外暴露 `list_devices`、`exec` 及文件/转发工具
- **WS 客户端**：到 Hub `/client` 的持久连接，role=`client`
- **RPC 路由**：每次工具调用生成 `msg_id`，注册 reply channel，发出 frame；收到 reply 后转回 MCP

## 4. Wire Protocol（统一 client/node ↔ hub）

**传输**：WSS binary frames，CBOR 编码。同一套 envelope 在 node 和 client 两侧通用：`target` 字段决定 hub 转发去哪台 node（client 发出时填，node 发出时省略；hub 按 `msg_id` 反向路由 reply）。

### 控制类

**Client → (Hub →) Node：**

| `type` | 字段 | 说明 |
|---|---|---|
| `exec` | `msg_id, target, cmd, cwd?, env?, timeout?` | 同步子进程（`sh -c`）；node 等待退出或超时后回一个 `reply` |
| `list_devices` | `msg_id` | hub-local：列已注册设备 |

**Node / Client → Hub（及反向）：**

| `type` | 字段 | 说明 |
|---|---|---|
| `hello` | `hostname, os, arch, agent_version, token, role?` | 握手（node 必填 hostname；client 用 `role:"client"`） |
| `reply` | `msg_id, ok, error?, data?` | 通用应答；`exec` 的 reply 中 `data` 含 `{stdout, stderr, exit_code, timed_out, truncated}` |
| `event` | `kind, device?` | hub 推送的异步事件（如 `device_online` / `device_offline`） |

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

`stream_id` 由发起 accept 的一侧生成 uuid，全局唯一；`forward_id` 由发起节点（node）启动时为每条规则分配。

> **注意：** 转发规则配置在 node 的 `config.yaml` 的 `forwards:` 字段下，由 `tether join` 启动时读取并生效。MCP client（`tether mcp`）在转发中没有任何角色——两端均为 node。

**Node (local) → (Hub →) Node (peer)：**

| `type` | 字段 | 说明 |
|---|---|---|
| `forward_listen` | `msg_id, target, forward_id, listen_addr, dest_host, dest_port` | -R 规则：让对端 node 在 `listen_addr` 起 TCP listener。reply `{ok, error?}`。Hub 记 `forward_id → initiating_node_ws` |
| `forward_unlisten` | `msg_id, target, forward_id` | 关闭对端 node 上对应 listener；Hub 同步清 listeners 表 |
| `forward_dial` | `msg_id, target, stream_id, dest_host, dest_port` | -L 路径：对端 node 拨号 `dest_host:dest_port`，reply `{ok, error?}`。Hub 记 `stream_id → (local_node_ws, peer_node_ws)` |
| `forward_data` | `target, stream_id, data` | 双向；fire-and-forget；Hub 按 `stream_id` 转发。单帧软上限 32 KiB |
| `forward_close` | `target, stream_id, half?` | `half ∈ {"read","write","both"}`（缺省 `"both"`）；fire-and-forget |

**Node (peer) → (Hub →) Node (local)：**

| `type` | 字段 | 说明 |
|---|---|---|
| `forward_dial` | `msg_id, stream_id, forward_id` | -R 路径：对端 node accept 后请本端 node 拨号；本端用 `forward_id` 查本地 dest 信息 dial。Hub 按 `forward_id → initiating_node_ws` 路由 |
| `forward_data`, `forward_close` | 同上 | 对称 |

**Hub → Node 事件（扩展 `event.kind`）：**

| `kind` | 字段 | 说明 |
|---|---|---|
| `device_online` | `device` | node 完成握手、注册成功时推送给所有在线 node；本端 node 用此事件重发 -R 规则的 `forward_listen` |
| `device_offline` | `device` | node WS 断开时推送；本端 node 标记相关 -R 规则待恢复 |

## 5. exec 模型

`exec` 是唯一的命令执行原语，本质是**同步单次子进程**：

- 命令字符串经 `sh -c` 执行，在独立进程组内运行（非 PTY）
- stdout / stderr 分别捕获，各限 4 MiB；超出则截断并置 `truncated=true`
- 等待子进程退出，或在 `timeout` 秒（默认 30）后 kill 整个进程组并置 `timed_out=true`
- 返回单个 reply：`{stdout, stderr, exit_code, timed_out, truncated}`

**长期运行 / 交互式工作：** 通过 `exec` 调用 `tmux`（`tmux new-session -d -s foo 'long-running-cmd'`）在设备上管理长期任务；后续通过 `exec tmux capture-pane` 读取输出，通过 `exec tmux send-keys` 发送输入。

**设备断网时：** 正在运行的子进程不受影响（node 进程仍在，断的是与 hub 的连接）；MCP 在断网期间对该 node 的所有调用返回 `device_offline`。

## 6. MCP 工具表面

| Tool | 参数 | 返回 |
|---|---|---|
| `list_devices` | — | `[{hostname, os, arch, online, last_seen}]` |
| `exec` | `device, cmd, cwd?, env?, timeout?=30` | `{stdout, stderr, exit_code, timed_out, truncated}` |
| `file_transfer` | `from, to, overwrite?=false` | `{ok, bytes, sha256, duration_ms}` |
| `read_file` | `path, offset?=0, limit?=2000` | `{lines, total_lines, truncated, sha256, binary}` |
| `write_file` | `path, content, overwrite?=false, create_dirs?=false` | `{bytes, sha256}` |
| `edit_file` | `path, old_string, new_string, replace_all?=false` | `{replacements, sha256}` |

**约定：**
- `cmd` 字符串经 `sh -c` 执行
- `device` 用 hostname 标识，注册时 hostname 冲突拒绝
- `file_transfer` 路径语法：`node:/abs/path` 或 `node:~/path` 表示某 node 路径；`/abs/path` 或 `~/path` 表示运行 `tether mcp` 这台机器上的路径。`from == to == 本地` 拒绝（直接用 OS 工具）；同 node 走 `file_local_copy`；跨 node 走 `file_relay`
- `read_file` / `write_file` / `edit_file` 的 `path` 必须是 `node:/abs/path` 或 `node:~/path` 格式；单文件 10 MiB 上限，超出用 `file_transfer`

## 7. 部署 / 配置

**二进制 + 子命令：**

```bash
tether serve              # 中继 hub（公网机）
tether join               # node，连入中继（被管设备）
tether mcp                # stdio MCP server（Claude Code 本地起）
```

**统一配置文件（`~/.config/tether/config.yaml`）：**

三个子命令（`serve`、`join`、`mcp`）共用同一份配置文件，每个子命令只读取自己角色所需的字段，忽略其余字段。一台机器同时承担多个角色时无需维护多份配置文件。

```yaml
# ~/.config/tether/config.yaml — 一个文件，驱动任意角色
token: "shared-secret"               # 必填，所有角色共用
listen: ":7000"                      # hub：监听地址（默认 :7000）
hub_url: "wss://hub.example/device"  # node + MCP client：hub WebSocket 地址
hostname_override: "my-host"         # node：注册时使用的主机名（可选，默认 os.Hostname()）
forwards:                            # node：端口转发规则（可选，见 §4 端口转发类）
  - "L 9000:peer:5037"               # 本节点 127.0.0.1:9000 → peer 的 localhost:5037
  - "R peer:8080:3000"               # peer 的 127.0.0.1:8080 → 本节点的 127.0.0.1:3000
```

字段与角色的对应关系：

| 字段 | hub (`serve`) | node (`join`) | MCP client (`mcp`) |
|---|---|---|---|
| `token` | ✓ | ✓ | ✓ |
| `listen` | ✓ | — | — |
| `hub_url` | — | ✓ | ✓ |
| `hostname_override` | — | ✓ | — |
| `forwards` | — | ✓ | — |

转发规则配置在 node 自身的 `config.yaml` 下，MCP client（`tether mcp`）无需配置任何转发项。

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
  - `github.com/mark3labs/mcp-go`（MCP server SDK）
  - `gopkg.in/yaml.v3`（配置）

## 9. 后续可扩展项（不在初版范围）

按优先级：
1. **Per-device token** + 单设备吊销（当前共享 token）
2. **设备掉线期间的 output 补传**（当前掉线即 `device_offline`）
3. **目录 / glob 文件传输**（当前单文件）
4. **多租户**

## 10. 参考文档

- [`docs/superpowers/specs/2026-05-19-port-forwarding-design.md`](superpowers/specs/2026-05-19-port-forwarding-design.md) — 端口转发详细设计（背景、架构、配置语法、wire protocol、错误处理、测试计划）。**注：** 原始设计将转发配置放在 MCP client 侧，实现时已调整为配置在 node 的 `config.yaml` 的 `forwards:` 字段，MCP client 不参与转发。
