# 文件传输 + MCP 架构重构 设计

日期:2026-05-16
状态:草案 → 待 user review

## 背景与目标

Tether 当前架构:behind-firewall 设备(node)通过 WSS 主动连接公网 Hub;Hub 在 `/mcp` 暴露 HTTP/SSE MCP 端点,Claude Code 通过 HTTP MCP 调用工具,Hub 将其翻译为 CBOR 协议消息转发到目标 node。已有 7 个工具:`list_devices` / `exec` / `start_process` / `list_processes` / `get_output` / `send_stdin` / `kill_process`。

需求:**支持双向文件传输**,以一个 scp 风格的 `file_transfer(from, to)` MCP 工具提供。

文件传输需求暴露了一个架构约束:**远程 HTTP MCP 服务端(Hub)无法读写 MCP 客户端(用户机器)上的文件**。要让「本机路径」语义成立,必须在用户机器上跑一个本地 MCP 进程。

本设计因此包含两部分:

1. **架构重构(Phase 1)**:废除 Hub 的 `/mcp` 端点,Hub 退化为纯 WS 消息中转;新增 `tether mcp` 子命令作为本地 stdio MCP 进程,7 个现有工具全部迁移到本地实现。
2. **file_transfer 实现(Phase 2)**:协议层新增 6 条文件相关消息,client/hub/node 各自实现对应逻辑。

两阶段共享同一 spec,分两个实施计划独立执行。

## 整体架构

```
┌─────────────────────────┐                  ┌─────────────────────────┐
│   Claude Code (user)    │                  │   Tether node (device)  │
│                         │                  │                         │
│  ─── stdio ───►         │                  │     ▲                   │
│       ┌──────────────┐  │                  │     │ WSS (outbound)    │
│       │ tether mcp   │──┼─── WSS ─────────►┼─────┤                   │
│       │ (local proxy)│  │                  │     │                   │
│       └──────────────┘  │     ┌────────┐   │                         │
│                         │     │  Hub   │   │                         │
│                         │     │ (relay)│   │                         │
└─────────────────────────┘     └────────┘   └─────────────────────────┘
```

三个组件:

- **`tether serve`(Hub)**:纯 WS 消息中转。两类客户端连入:
  - **node**(`/device`):执行端,注册到 device registry
  - **client**(`/client`):控制端,跑 MCP 工具调用,注册到 client registry
  Hub 维护两张 registry + 一张 in-flight 路由表(`msg_id → conn`),不再懂 MCP。`/mcp` 路径删除,`internal/hub/mcp.go` 整个废弃。
- **`tether join`(node)**:基本不变,继续提供 exec / process 能力,新增 file 操作。
- **`tether mcp`(本地,新增)**:stdio MCP server。启动时按配置连 Hub 的 `/client`,认证后保持 WS。把 MCP 工具调用翻译为 CBOR 协议消息发给 Hub,Hub 转发到目标 node,回包路由回来再转成 MCP 响应。

**鉴权:** Hub 上 `/device` 和 `/client` 共用同一个 `token`。Hello 消息新增 `role` 字段:`"node"`(默认)或 `"client"`。MVP 不做细粒度权限,持有 token 即可调任意 node。

## 协议消息

### 现有(不变)

`Exec / ExecCancel / Start / Stdin / Kill / GetOutput / List / Ping / Hello / Reply / ExecOutput / ExecExit / Event / Pong`

### Hello 扩展

```go
type Hello struct {
    // ... 现有字段
    Role string `cbor:"role,omitempty"`  // "node"(默认) | "client"
}
```

### 新增 6 条 file 消息

```go
// 下载: client → hub → node
type FileGetOpen struct {
    Type  string `cbor:"type"`   // "file_get_open"
    MsgID string `cbor:"msg_id"`
    Path  string `cbor:"path"`
}
// node 回 Reply{ok:true, data:{size:int64, mode:uint32, sha256:string}}
// 然后 node 主动 push FileChunk 直到 EOF。

// 上传: client → hub → node
type FilePutOpen struct {
    Type      string `cbor:"type"`   // "file_put_open"
    MsgID     string `cbor:"msg_id"`
    Path      string `cbor:"path"`
    Size      int64  `cbor:"size"`
    Mode      uint32 `cbor:"mode,omitempty"`
    Overwrite bool   `cbor:"overwrite,omitempty"`
    SHA256    string `cbor:"sha256,omitempty"`
}
// node 回 Reply{ok:true} 表示就绪;client 开始 push FileChunk;
// 结束后 client 发最后一帧 EOF=true;node 校验 sha256 后回最终 Reply。

// 流式分块,关联 msg_id
type FileChunk struct {
    Type  string `cbor:"type"`   // "file_chunk"
    MsgID string `cbor:"msg_id"`
    Seq   int64  `cbor:"seq"`
    Data  []byte `cbor:"data"`
    EOF   bool   `cbor:"eof,omitempty"`
}

// 任一方主动取消
type FileAbort struct {
    Type  string `cbor:"type"`   // "file_abort"
    MsgID string `cbor:"msg_id"`
    Error string `cbor:"error"`
}

// node ↔ node: client → hub 专用,Hub 协调
type FileRelay struct {
    Type      string `cbor:"type"`   // "file_relay"
    MsgID     string `cbor:"msg_id"`
    FromNode  string `cbor:"from_node"`
    FromPath  string `cbor:"from_path"`
    ToNode    string `cbor:"to_node"`
    ToPath    string `cbor:"to_path"`
    Overwrite bool   `cbor:"overwrite,omitempty"`
}

// 同 node 内两路径间的本机 copy
type FileLocalCopy struct {
    Type      string `cbor:"type"`   // "file_local_copy"
    MsgID     string `cbor:"msg_id"`
    FromPath  string `cbor:"from_path"`
    ToPath    string `cbor:"to_path"`
    Overwrite bool   `cbor:"overwrite,omitempty"`
}
```

### 协议要点

- **Chunk 大小**:固定 256 KB。
- **流模型**:open 建立会话 → 单向连续 push chunks → 末帧 EOF=true。无窗口/无 ack,依赖 WS/TCP 自身的 backpressure。
- **Hub 转发 FileChunk**:不解码 `Data` 字段,直接转发(zero-copy)。
- **同步语义**:MCP 工具同步阻塞直到传完或 abort;不报中间进度。
- **大小限制**:配置项 `max_file_size_bytes`(默认 5 GiB),node 在 FilePutOpen 时拒绝、下载在元数据回包阶段判断。
- **并发限制**:每 client 同时最多 4 个 in-flight 传输(MVP 静态值)。
- **路径规则**:必须绝对或 `~` 开头;`~` 在执行端展开。

## MCP 工具接口

### 新工具 `file_transfer`

入参 schema:

```jsonc
{
  "from": "node:/abs/path | /abs/path | ~/path",
  "to":   "node:/abs/path | /abs/path | ~/path",
  "overwrite": false
}
```

路径语法:

- `<nodename>:/path` 或 `<nodename>:~/path` —— 该 node 上的路径
- `/path` 或 `~/path` —— 运行 `tether mcp` 的本机(Claude Code 主机)
- 不支持目录(单文件传输)
- 禁止 from == to(同机同路径)

5 种组合的执行路径:

| from | to | 实现 |
|------|----|------|
| local → local | 拒绝,返回 error "use os tools" |
| local → node | client 读本地文件,push 到 node:`FilePutOpen` → 多 `FileChunk` → 最终 Reply |
| node → local | client 从 node pull,写到本地:`FileGetOpen` → 接 chunks → 本地落盘 |
| nodeA → nodeB | client 发 `FileRelay` 给 Hub,Hub 在两 node 间流式串接 |
| nodeA → nodeA(同 node) | client 发 `FileLocalCopy` 给 Hub → node 本机 OS copy |

返回值:

```jsonc
{
  "ok": true,
  "bytes": 1234567,
  "duration_ms": 842,
  "sha256": "abc123..."
}
```

错误时 `ok: false` + `error: "..."`,常见错误码:`path_not_found` / `permission_denied` / `destination_exists` / `device_offline` / `size_limit_exceeded` / `hash_mismatch` / `disk_full` / `source_disconnected` / `dest_disconnected`。

### 保留的其他 7 个 MCP 工具

`list_devices` / `exec` / `start_process` / `list_processes` / `get_output` / `send_stdin` / `kill_process` —— 全部从 Hub 搬到 `tether mcp` 本地,逻辑等价(参数 → CBOR msg via WSS → Hub 转发 → reply → MCP response)。MVP 期间外部行为完全不变,仅传输路径调整。

## Hub Relay 流程(node↔node)

Client 不能同时跟两个 node 直接对话(协议是 client↔Hub↔node 单跳),所以 Hub 需要一点协调逻辑:

1. 收到 client 的 `FileRelay { msg_id_X, from_node, from_path, to_node, to_path, overwrite }`
2. Hub 给 `from_node` 发 `FileGetOpen { msg_id_A, path=from_path }`,等元数据 Reply
3. Hub 给 `to_node` 发 `FilePutOpen { msg_id_B, path=to_path, size, sha256, overwrite }`,等就绪 Reply
4. 进入流式状态:`from_node` 推 `FileChunk { msg_id_A, ... }` 给 Hub;Hub 改写为 `FileChunk { msg_id_B, ... }` 转发给 `to_node`
5. 末帧 EOF 后,`to_node` 校验 sha256,回最终 Reply 给 Hub
6. Hub 把最终 Reply 转给 client(挂在 msg_id_X 上)
7. 任一侧 abort,Hub 取消另一侧的 in-flight,并向 client 报告

Hub 是核心扇出点,但不持久化任何字节,内存只滑动 1~2 帧。

## 文件布局与模块边界

```
internal/
  cli/
    serve.go            ← 改:去掉 mcp 启动
    join.go             ← 不变
    mcp.go              ← 新:tether mcp 子命令入口
  hub/
    server.go           ← 改:只保留 /device /client /health
    endpoint_ws.go      ← device_ws.go 改名,统一处理 /device 和 /client
    registry.go         ← 改:支持两类(devices, clients)
    router.go           ← 改:支持 sticky 路由
    relay.go            ← 新:file_relay 协调器
    mcp.go              ← 删
    mcp_test.go         ← 删
  client/               ← 新包
    config.go           ← 本地配置(hub_url / token)
    conn.go             ← WSS 到 Hub 的连接 + 重连
    rpc.go              ← 请求/响应路由
    mcp_server.go       ← stdio MCP server
    tools_exec.go       ← list_devices/exec/process 类工具
    tools_file.go       ← file_transfer 工具
  node/
    handler.go          ← 改:dispatch 新增 file 分支
    file.go             ← 新:FileGetOpen/FilePutOpen/FileLocalCopy 处理
    process.go / pty.go ← 不变
  protocol/
    messages.go         ← 加 6 条 file 消息 + Hello.Role
    codec.go            ← 注册新类型
```

**Hub router 扩展(sticky 路由):**
当前 router 是「msg_id → 一个 reply channel」的请求/响应模型。文件分块是多帧 + 中间没有 reply,需要新增「msg_id → 持续转发通道」的注册类型。简化做法:扩展 router 用「sticky 路由表」记录 msg_id → 对端连接,直到 EOF 或 abort 才清除。

**节点端原子写入(`internal/node/file.go`):**

- 接 `FileGetOpen`:打开文件 → 发 Reply 含元数据 → goroutine 流式读 → 分块发 `FileChunk` → EOF
- 接 `FilePutOpen`:校验路径/overwrite/可写 → Reply{ok:true} → 收 `FileChunk` 直到 EOF → 写入并校验 sha256 → 最终 Reply
- 接 `FileLocalCopy`:本机两路径间用 `io.Copy` + 临时文件 + 原子 rename
- **写入策略**:先写 `<path>.tether-tmp-<msg_id>`,sha 校验通过后 `os.Rename` 到目标。失败时清理临时文件。**目标文件要么完全存在要么完全不存在**,不留半截文件。

## 错误处理

| 场景 | 表现 |
|------|------|
| Node 中途断线(下载)| Hub 检测到 from_node 关闭,向 client 发 `FileAbort{error:"source_disconnected"}`,client 清理已写的临时文件,MCP 工具返回 error |
| Node 中途断线(上传)| Hub 检测到 to_node 关闭,client abort 停止 push |
| Hub 重启 | client 和 node WSS 都断重连;in-flight 传输全部 abort(MVP 不做断点续传)|
| Client 中途断线 | Hub 取消相关传输,通知 node 删除 `.tether-tmp-*` |
| Disk full(node 侧)| node 写入时捕获 `ENOSPC`,发 `FileAbort{error:"disk_full"}`,清理临时文件 |
| sha256 不匹配 | node 不 rename,删临时文件,Reply{ok:false, error:"hash_mismatch"} |
| 路径不存在 / 权限拒绝 | open 阶段同步报错,返回 ok:false |
| 文件大小超限 | node 在 FilePutOpen 时拒绝;下载在 FileGetOpen 元数据阶段判断 |

**关键不变量:**

- 节点端目标文件**要么完全成功要么完全不存在**(失败清理临时文件)
- Hub 不持久化任何字节,内存只有 1~2 个 chunk 的滑动窗口
- 任何 abort 都双向传播,Hub 是扇出点

## 测试策略

- **协议单测**:新增消息的 CBOR encode/decode round-trip(`internal/protocol/codec_test.go` 扩展)
- **Hub relay 单测**(`internal/hub/relay_test.go`):用两个 in-memory fake conn 模拟 from_node/to_node,验证 chunk 转发、abort 传播、msg_id 改写
- **Node 文件单测**(`internal/node/file_test.go`):用 `t.TempDir()` 跑 FileGetOpen / FilePutOpen / FileLocalCopy,覆盖 overwrite、目标不存在、目标已存在、权限拒绝、sha 不匹配
- **Client 工具单测**(`internal/client/tools_file_test.go`):本地 ↔ fake node 端到端
- **e2e_test.go 扩展**:内存集群跑 `tether mcp` ↔ Hub ↔ node,完整传 ~10 MB 文件验证 sha;node↔node 路径独立验证
- **关闭路径测试**:中途关闭 conn 验证临时文件被清理

## 实施阶段

### Phase 1:架构重构(不改外部功能)

1. 新增 `internal/client/` 包:stdio MCP server 骨架 + WSS conn + 7 个现有工具迁移
2. Hub:新增 `/client` endpoint,扩展 `Hello.Role`,registry 拆双表,endpoint_ws.go 统一两类客户端
3. CLI:新增 `tether mcp` 子命令
4. 删 `internal/hub/mcp.go` + `mcp_test.go`,`server.go` 不再挂 `/mcp`
5. 更新 README、systemd 模板
6. `e2e_test.go` 改为走新路径,功能等价
7. **验收**:7 个原工具行为不变,部署方式从 HTTP MCP 改为本地 stdio MCP

### Phase 2:file_transfer

1. `protocol/messages.go` 加 6 条 file 消息 + Codec 注册
2. `node/file.go` 实现 FileGetOpen / FilePutOpen / FileLocalCopy
3. `hub/router.go` 加 sticky 路由,`hub/relay.go` 实现 file_relay 协调
4. `client/tools_file.go` 实现 MCP `file_transfer` 工具,本机读写 + 调度 5 种实际组合
5. 端到端测试 + 中等大小压测(几百 MB)
6. 文档 / README 更新

### 估算

- Phase 1:约 700~900 行新增 + 400 行删除
- Phase 2:约 600~800 行新增

两期可分别合并、独立验证。

## 非目标(明确不做的事)

- 断点续传(Hub 重启或 conn 断开后从头开始)
- 目录递归传输(由用户先 tar 再传)
- 传输进度上报(同步阻塞,完成才返回)
- 压缩(WS 自带 permessage-deflate,够用)
- 多 token / 细粒度权限(MVP 单 token)
- 多 client 并发到同一 Hub 的资源公平分配(MVP 先做硬上限)
- 删除 `/mcp` 后的向后兼容(整体切换,无 deprecation 期)
