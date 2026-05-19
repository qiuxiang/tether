# Tether 端口转发设计

**Status:** Draft
**Date:** 2026-05-19

## 1. 背景与目标

Tether 当前把"端口转发"列为显式非目标（`docs/design.md` §1）。本设计撤销该非目标，新增 ssh `-L` / `-R` 等价的 TCP 端口转发能力，作为 `tether mcp` 的配置驱动常驻功能。

**两个方向都要：**

- **本地 → node（-L）**：`tether mcp` 所在机器 bind 一个本地端口，连接被转发到指定 node 视角的某个 `host:port`
- **node → 本地（-R）**：指定 node 上 bind 一个端口，连接被转发回 `tether mcp` 所在机器视角的某个 `host:port`

**目标用例：**

- 把本地的 mock server / LLM API 暴露给跑在 node 上的进程
- 从本地 curl/IDE 直接访问 node 上的 dev server、adb、DB

**显式非目标（本期）：**

- ❌ UDP / Unix socket（先 TCP，足够覆盖 HTTP/DB/gRPC/adb）
- ❌ MCP tool 形式的动态开关（配置驱动；改规则重启 `tether mcp`）
- ❌ 流级 credit 流控（共享 WS 背压够用）
- ❌ 转发认证 / per-rule token（已有 hub token 守门）
- ❌ 动态 reload

## 2. 架构

复用现有"client ↔ hub ↔ node"WSS+CBOR 通道，端口转发的每条 TCP 连接走一条逻辑 stream，多路复用在同一条 WS 上。Hub 维护两张路由表，纯转发，不解释规则。

```
┌────────────┐    forward_data{stream_id}    ┌─────┐    forward_data{stream_id}    ┌──────┐
│ tether mcp │ ◄──────── WSS+CBOR ─────────► │ Hub │ ◄──────── WSS+CBOR ─────────► │ Node │
│  (client)  │                               │     │                               │      │
└────────────┘                               └─────┘                               └──────┘
     │                                                                                  │
     ▼ TCP listener (for -L)                          TCP listener (for -R) ◄───────────┘
```

### 角色职责

**MCP client（`tether mcp`）**

- 启动时解析 `client.yaml` 的 `forwards`，每条规则分配 `forward_id` (uuid)
- 对 -L 规则：本地起 TCP listener（按 bind 地址）；每次 accept 创建 `stream_id`，发 `forward_dial` 给目标 node
- 对 -R 规则：向目标 node 发 `forward_listen` 让 node 起 listener；node 反向的 `forward_dial` 由 client 查 `forward_id` 拿到本地 dest，dial 后双向泵字节
- 维护 `stream_id → net.Conn`、`forward_id → rule` 映射
- 监听 hub 推送的 `device_online` 事件，触发该 node 的 -R 规则重发

**Hub（`tether serve`）**

新增路由表：

```
listeners : forward_id → client_ws          // forward_listen 创建；client 断开时清
streams   : stream_id  → (client_ws, node_ws)  // 首个 forward_dial 建立；forward_close 或任一端断开时清
```

`forward_*` 帧的路由：

- `forward_listen` / `forward_unlisten` / client→node 向 `forward_dial`：按 `target`（hostname）路由到 node；同时更新 listeners / streams 表
- node→client 向 `forward_dial`：按帧里的 `forward_id` 查 listeners 表找 client_ws
- `forward_data` / `forward_close`：按 `stream_id` 查 streams 表 O(1) 转发
- 断开级联：node WS 断 → evict 所有涉该 node 的 streams，向各 client 发 `forward_close`；listeners 表中 `target=该 hostname` 的条目保留（client 会在 device_online 时重发 forward_listen）。client WS 断 → 清该 client 的 listeners 条目并向相关 node 发 `forward_unlisten`；evict 该 client 的 streams 并通知 node 端

新增 hub→client 事件：扩展现有 `event.kind` 集合，加 `device_online` / `device_offline`。

**Node（`tether join`）**

- 收 `forward_listen` → 本地 bind TCP listener；reply ok/error；listener 跟 node↔hub WS 同生共死（WS 断时关闭）
- 收 `forward_dial` → 拨号 `dest_host:dest_port`；reply ok/error；起字节泵
- 收 `forward_unlisten` → 关闭对应 listener
- accept（-R 路径）→ 生成 `stream_id`，向 hub 发 `forward_dial{forward_id, stream_id}`；reply ok 后起字节泵，否则 close
- 维护 `forward_id → net.Listener`、`stream_id → net.Conn`

### 生命周期

- **-L listener 始终在 client 侧 bind**：node 离线时本地端口仍可 connect，但 `forward_dial` 会失败 / 永不 reply（hub 没有路径），client 在收到错误 reply 或超时后立即 close 本地 accepted conn
- **-R listener 跟 node 同生共死**：node 离线即 unbind；node 重连后 client 重发 `forward_listen` 恢复
- **MCP client 退出**：WS 主动 close，hub 级联清理所有 listeners + streams

## 3. 配置语法

`client.yaml` 新增 `forwards` 字段，列表中每项是 ssh 风格紧凑字符串。

```yaml
hub_url: "wss://tether.example.com/client"
token: "xxx"
forwards:
  - "L 9000:mac:5037"                  # 本地 127.0.0.1:9000 → mac 上的 localhost:5037
  - "L 0.0.0.0:9000:mac:5037"          # 本地 0.0.0.0:9000 → mac 上的 localhost:5037
  - "L 9000:mac:192.168.1.5:5037"      # 本地 127.0.0.1:9000 → mac 视角的 192.168.1.5:5037
  - "R mac:8080:3000"                  # mac 上 127.0.0.1:8080 → 本地 127.0.0.1:3000
  - "R mac:0.0.0.0:8080:3000"          # mac 上 0.0.0.0:8080 → 本地 127.0.0.1:3000
  - "R mac:8080:db.local:5432"         # mac 上 127.0.0.1:8080 → 本地视角的 db.local:5432
```

**EBNF：**

```
rule        := direction SP spec
direction   := "L" | "R"
spec_L      := [bind ":"] port ":" device ":" [host ":"] port
spec_R      := device ":" [bind ":"] port ":" [host ":"] port
bind        := IPv4 | IPv6-in-brackets | hostname
host        := IPv4 | IPv6-in-brackets | hostname
port        := uint16 (1..65535)
device      := hostname (须匹配 node 注册名)
```

**缺省：**

- `bind` 缺省 `127.0.0.1`
- `host` 缺省 `localhost`（在 listener 对面那台机器视角解析）

**解析时机：** `tether mcp` 启动时一次解析。任何非法字符串、未知字段、重复 listen 端口 → fail-fast 退出（不局部跳过）。引用了当前未注册的 hostname 不算错误（接受，等 device_online 时再 bind / 在 -L 路径上等 dial 时失败）。

## 4. Wire Protocol

沿用现有 CBOR envelope（`internal/protocol`），新增 5 个 `type`。`stream_id` 由发起 accept 的那一侧生成 uuid，全局唯一。

### Client → (Hub →) Node

| `type` | 字段 | 说明 |
|---|---|---|
| `forward_listen` | `msg_id, target, forward_id, listen_addr, dest_host, dest_port` | -R：让 node 在 `listen_addr` 起 TCP listener。reply `{ok, error?}`。Hub 记 `forward_id → client_ws` |
| `forward_unlisten` | `msg_id, target, forward_id` | 关 node 上的 listener。Hub 同步清 listeners 表 |
| `forward_dial` | `msg_id, target, stream_id, dest_host, dest_port` | -L 路径：node 拨号 `dest_host:dest_port`，reply `{ok, error?}`。Hub 记 `stream_id → (client_ws, node_ws)` |
| `forward_data` | `target, stream_id, data` | 双向；fire-and-forget；Hub 按 `stream_id` 转发 |
| `forward_close` | `target, stream_id, half?` | `half ∈ {"read","write","both"}`（缺省 `"both"`）；fire-and-forget |

### Node → (Hub →) Client

| `type` | 字段 | 说明 |
|---|---|---|
| `forward_dial` | `msg_id, stream_id, forward_id` | -R 路径：node accept 后请 client 拨号；client 用 `forward_id` 查本地 dest 信息 dial。Hub 按 `forward_id → client_ws` 路由 |
| `forward_data`, `forward_close` | 同上 | 对称 |
| `event` | `kind ∈ {"device_online","device_offline"}, device` | 扩展现有 `event` 帧，hub 主动推 |

### 帧大小与背压

- 单 `forward_data` 帧软上限 32 KiB；发送侧切片
- 不引入流级 credit；所有 stream 共享所在 WS 的写阻塞作为背压
- CBOR 编解码已支持 `[]byte` 直传，零拷贝路径不变

### 半关语义

- 一侧 conn 看到 EOF on read → 发 `forward_close{stream_id, half:"write"}`，对端 close write 半
- 一侧 conn 写出错 → 发 `forward_close{stream_id, half:"both"}`，本地 close 整条
- 收到 `half:"both"` → close 本地 conn 并从 streams 表移除
- Hub 不解释 `half`，纯转发；按"两半都关了 或 任一端 WS 断"清理路由

## 5. 错误处理

| 场景 | 表现 |
|---|---|
| -L node 离线 | 本地 listener 仍 bind；accept 后 forward_dial 经 hub 找不到 target_ws → hub 立即 reply `{ok:false, error:"device_offline"}`；client close 本地 conn |
| -L node 在线但 dial 失败 | node reply `{ok:false, error:"dial: <reason>"}`；client close 本地 conn |
| -R bind 失败 | node reply `{ok:false, error:"bind: <reason>"}`；client 日志一行，规则标 `degraded`；下次该 node device_online 时重试一次 |
| -R node 离线 | listener 在 node 上随 WS 断而消失；client 端不做事；node 重连时 client 收 device_online → 重发 forward_listen |
| client 退出 | WS close → hub 清该 client 的 listeners 并向相关 node 发 forward_unlisten；evict 该 client 的 streams 并通知 node 端 |
| 引用未注册 hostname | 接受配置；-L accept 时 hub reply `device_offline`；-R 在 device_online 时 bind |
| 重复 listen 端口 / 非法字符串 | client 启动时 fail-fast |

所有错误以**单行 client 日志**呈现，不抛 panic、不退出 mcp。

## 6. 测试

**Unit：**

- `internal/protocol`：5 个新 frame type 的 CBOR roundtrip
- `internal/cli`（或 `internal/config`）：紧凑字符串解析器表驱动测试（合法/非法/缺省/IPv6/重复端口）
- `internal/hub`：listeners / streams 路由表的并发安全 + 断开级联清理（mock WS）
- `internal/node`：forward_listen → 起真 listener；forward_dial → 拨真 echo server

**E2E（扩 `e2e_test.go`，全进程内）：**

1. -L 正向 echo：client 本地端口 → node 上 echo server 一来回
2. -R 正向 echo：node 上 listener → client 上 echo server 一来回
3. node 离线场景：-L accept 立即 close；node 重连后 -R listener 自动恢复
4. client 退出：node 上 -R listener 关闭，端口可立即 rebind
5. dial 不存在端口：accepted conn 立即 close + 错误日志

## 7. 实现切片

按提交顺序：

1. **Protocol**：5 个 frame type + CBOR + `event.kind` 扩展（device_online/offline）
2. **Config**：紧凑字符串解析器 + `client.yaml.forwards` 字段
3. **Hub**：路由表（listeners + streams）+ 转发逻辑 + 断开级联 + device 事件推送
4. **Node**：listener registry + dial 逻辑 + 字节泵 + WS 断时清理
5. **Client**：listener 管理 + dial-back 处理 + 字节泵 + device_online 重连恢复
6. **E2E + 文档**：扩 `e2e_test.go`；README 加 `forwards` 一节；`docs/design.md` 把"端口转发"从非目标移除并指向本 spec

## 8. 文档更新

- `docs/design.md` §1：删除"❌ 端口转发"一行；§4 协议表加新 frame type；§6 MCP 工具表面不变（无新工具）；§9 把"端口转发"相关项移除
- `README.md`：新增 "Port forwarding" 一节，含 `forwards` 字段说明、ssh 风格语法、缺省 bind/host、几个示例
