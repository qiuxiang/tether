# Unify `exec` with `start_process`

Date: 2026-05-19

## 目标

让设备上每条命令都对应一个可追踪、可观测的进程,把 `exec` 改造成 `start_process` 的薄包装。给两个工具都加一个自由文本的 `description` 注释字段。

效果:`exec` 卡住时,agent 可以照常用 `list_processes` / `capture_screen` / `send_stdin` / `kill_process` 接管;描述字段还能在 `process_id` 因 MCP ctx 中断丢失时作为恢复锚点。

## 现状

- `exec`(`internal/client/tools_exec.go`)走独立的 `protocol.Exec` / `ExecCancel` / `ExecOutput` / `ExecExit` 流式 RPC。超时时发 `ExecCancel` 杀进程,部分输出连同 `timed_out: true` 返回,进程在 node 上立刻终止,无法事后观测。
- `start_process` 通过 `protocol.Start` 启动 pty 进程,记录持久存在,可被 `list_processes` / `capture_screen` 等观察。当前带一个未充分利用的 `name` 字段。
- 两套路径并存,exec 完全不可观测。

## 设计

### 单一进程模型

`exec` 不再是独立协议。它在 client 侧由三步组成:

1. 生成 `process_id`,发送 `Start{... Description}`,等回包(标准 req/reply,返回 `{process_id}`)。
2. 发送 `Attach{ProcessID}`,订阅该进程的输出流。Node 端把 pty 字节缓冲区(`capture_screen` 用的同一份)先发已有内容,再推增量。
3. 循环消费输出流,直到:
   - 收到 `ProcessExit{Code}` → 返回 `{output, exit_code, process_id, timed_out: false}`。
   - MCP ctx 取消(client 超时或用户中断)→ 发送 `Detach{ProcessID}`,返回**成功结果** `{output, process_id, timed_out: true}`,**进程在 node 上继续运行**。

返回的 `output` 是累积的 pty 原始字节(stdout/stderr 已在 pty 层合并),保留 ANSI 序列。Agent 想要渲染后的视图可以用 `capture_screen` 读同一进程,两者读的是同一份缓冲区。

### 超时

`exec` 有可选的 `timeout` 参数(秒,默认 30)。client 侧用 `context.WithTimeout` 包一层,到点等同于 ctx.Done() 路径:发 `Detach`、返回成功 + `process_id` + `timed_out:true`,进程在 node 继续跑。

为什么需要默认值:MCP client 不一定会强制 request 超时,如果没有内部 deadline,卡住的命令会无限阻塞 agent 的 turn。30s 是个保守默认,agent 可以按命令预期时长覆盖。预期会跑很久的命令本来就该走 `start_process`,不是 `exec`。

ctx 取消时的 caveat:部分 MCP transport 在 ctx 取消后 client 已停止读 response,这种情况下我们的"优雅返回"未必能送达,`process_id` 会丢失。**兜底**:agent 用 `list_processes` 按 `description` 查回那条进程。

### 描述字段

`description` 是给 agent 自释义的自由文本注释,贴在进程记录上,`list_processes` 返回时一并带出。同时取代当前的 `name` 字段(`name` 删除)。

### 协议变更

`internal/protocol/messages.go`:

- `Start`:删除 `Name`,新增 `Description string`。
- 删除 `Exec`、`ExecCancel`、`ExecOutput`、`ExecExit`。
- 新增 `Attach{Type, MsgID, Target, ProcessID, FromOffset int64}`:订阅进程输出流。
- 新增 `Detach{Type, MsgID, Target, ProcessID}`:停止订阅(不影响进程运行)。
- 新增 `ProcessOutput{Type, MsgID, Data []byte}`:Attach 后流式推送 pty 字节。
- 新增 `ProcessExit{Type, MsgID, Code int}`:进程退出的流式终结事件。

### Client 端(`internal/client/tools_exec.go`)

- `exec` 工具:
  - `timeout` 参数保留,单位秒,默认 30。client 侧用 `context.WithTimeout` 包一层,到点等同于 ctx.Done() 路径(发 Detach,进程不杀)。
  - 新增 `description` 参数(可选)。
  - 实现改为:`Start` → 等 reply → `Attach` → 消费 `ProcessOutput` 累积到 `output` 字节缓冲,遇 `ProcessExit` 正常返回,遇 `ctx.Done()`(超时或外层取消)发 `Detach` 并返回成功结果(带 `timed_out: true`)。
- `start_process` 工具:
  - 删除 `name` 参数。
  - 新增 `description` 参数(可选)。
  - 透传到 `Start{Description}`。
- `list_processes`:返回记录里包含 `description`。
- `capture_screen` / `send_stdin` / `kill_process`:不变。

### Node 端(`internal/node/pty.go` 等)

- 进程记录:`Name` 字段重命名为 `Description`。`List` 回包字段同步。
- 新增 `Attach` handler:
  - 找到 `ProcessID` 对应的 pty 记录;读取其字节缓冲区从 `FromOffset` 开始的内容,作为首批 `ProcessOutput` 发出。
  - 注册一个 listener,后续每次 pty 有新字节时 push `ProcessOutput`。
  - 进程退出时发 `ProcessExit{Code}` 然后结束这个 stream。
  - 同 `process_id` 收到 `Detach` 则注销 listener 并结束 stream。
- 彻底删除 `handleExec` 及其流式路径。

### Hub 端

按 hub 现有"按 device 路由 + msg_id 关联流"的规则,新增的 `Attach` / `Detach` / `ProcessOutput` / `ProcessExit` 走与现有流式 RPC 相同的派发逻辑。`Exec*` 系列消息的派发分支同步删除。

## 兼容性

不需要。单一发布,hub + node 同版本部署。`exec` 的调用方只有本仓库的 MCP client tools。

## 测试

### 单元

- Node 侧 `Attach`:验证能返回 `FromOffset=0` 后的初始 buffer,后续 pty 写入实时推送,进程退出后发 `ProcessExit` 并关闭流。
- `Detach`:验证注销后不再收 `ProcessOutput`,且进程继续运行。
- 进程记录 `Description` 在 `list_processes` 中正确返回。

### E2E(沿用 `e2e_test.go` 模式)

- `exec` 跑 `echo hi`:返回 `output="hi\n"`、`exit_code=0`、`timed_out=false`;`list_processes` 可见该记录、状态 exited、description 匹配。
- `exec` 跑 `sleep 10`,client 侧 ctx 在 1s 后 cancel:返回成功结果带 `process_id` 和 `timed_out=true`;`list_processes` 中该 pid 状态 running;`capture_screen(pid)` 可读;`kill_process(pid)` 能正常清理。
- `start_process` 带 `description` 启动 + `list_processes` 按 description 找回 pid。
- 同一 process_id 多次 `Attach`/`Detach` 不互相干扰。

## 明确不做

- exited 记录的 TTL —— 以后真有列表膨胀问题再加。
- pipes 模式(非 pty)的 exec —— 已确定统一走 pty,stdout/stderr 合并。
- 给 `start_process` 加 `timeout` —— 它本来就是后台进程入口。
- 兼容旧的 `Exec*` 协议消息 —— 直接全删。
