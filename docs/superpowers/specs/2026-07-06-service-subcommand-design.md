# Tether — 跨平台 `service` 子命令设计

**Status:** Design approved, pending implementation
**Issue:** [#2](https://github.com/qiuxiang/tether/issues/2)
**Date:** 2026-07-06

## 1. 背景与目标

`tether` 一份二进制承担三种角色（`serve` / `join` / `mcp`），其中 hub 与 node 需要作为常驻服务运行（开机自启、崩溃自愈）。现状是三套手工搭建：

- Linux：systemd unit（`dist/systemd/*.service`）
- macOS：`dist/mac-restart.sh` 在 tmux 里手动跑，**不是** launchd 服务
- Windows：Task Scheduler + 每分钟重复触发的 watchdog（**不是真服务**，自愈延迟最长 1 分钟）

目标：新增 `tether service <install|start|stop|uninstall|status>` 子命令，用
[`kardianos/service`](https://github.com/kardianos/service) 统一封装 systemd（Linux）、
launchd（macOS）、Windows SCM，替换三套手工方式。Windows node 从 1 分钟 watchdog 升级为
秒级自愈的真 SCM 服务。

**非目标：** 不生成/校验/管理 tether 的 `config.yaml`（假设已存在，由 `--args` 用绝对路径引用）。

## 2. 命令表面

```
tether service install   --name <svc> --args "<serve|join …>" [--env K=V]… [--system|--user]
tether service start     --name <svc>
tether service stop      --name <svc>
tether service uninstall --name <svc>
tether service status    --name <svc>
```

设计取向：**薄封装 `--args` 透传**。`tether` 不理解 `serve`/`join` 的语义，只把 `--args`
原样作为二进制的启动参数注册给服务管理器。

| Flag | 说明 |
|---|---|
| `--name` | 服务标识（`tether-hub-b` / `tether-node-b`）。多实例共存靠它区分。**所有子命令必填。** |
| `--args` | 原样透传给二进制的启动参数字符串（`install` 专用）。写进 service 定义的启动命令。tether 不解析其语义。 |
| `--env K=V` | 可重复。补充/覆盖 service 环境变量（`install` 专用）。 |
| `--system` / `--user` | 可选逃生阀，覆盖 §3 的自动决策。正常不需要传。 |

多实例示例：

```
tether service install --name tether-hub-b  --args "serve --config /root/.config/tether/hub-b.yaml"
tether service install --name tether-node-b --args "join  --config /root/.config/tether/config-b.yaml"
```

`status` 输出映射 kardianos `Status()`：`running` / `stopped` / `not-installed`。

## 3. system/user 自动决策

不传 `--system`/`--user` 时，按平台自动选择（原则："能 user 就 user，不行才 system"，
只有 Windows 没有 per-user 服务）：

| 平台 | 自动 | kardianos 落地 |
|---|---|---|
| Linux | **user** | `Option{"UserService": true}` → `~/.config/systemd/user/<name>.service`；install 后自动执行 `loginctl enable-linger <user>`，使其不登录、开机也常驻 |
| macOS | **user** | `Option{"UserService": true}` → `~/Library/LaunchAgents/<name>.plist`（登录触发，拿完整用户环境） |
| Windows | **system** | SCM 服务（无 per-user 概念，回退），以 LocalSystem 运行 |

覆盖规则：

- 显式 `--system` / `--user` 覆盖自动值。
- 在 Windows 上传 `--user` → 明确报错 `--user is not supported on Windows`。

hub 通常也走 user（Linux 上即 root 的 `systemctl --user` + lingering，读它自己的 config）；
需要 system 级时显式传 `--system`。

## 4. serve/join 变成 service-aware（Windows SCM 前提）

### 4.1 为什么必须改

systemd / launchd 只是 `fork+exec` 拉起进程，进程活着即服务正常，对被管进程无特殊要求；
现有 `tether serve` / `tether join` 原样即可被它们管理。

**Windows SCM 不同**：SCM 启动服务进程后，要求该进程在数秒内主动向 SCM 报到
（`StartServiceCtrlDispatcher`）并注册 stop/pause 控制回调，否则判定启动失败（error 1053）
并杀掉进程。这正是当前 Windows 只能用 Task Scheduler（与 systemd 一样宽容）的原因。要成为
真 SCM 服务，二进制必须会说 SCM 握手协议。

### 4.2 改造方式

新增 `internal/service` 包，把一个 kardianos `Program`（`Start` 起 goroutine 跑现有逻辑、
`Stop` 取消 context 并调现有 `Shutdown()`）包在 `serve` / `join` 现有阻塞主循环外，用
`svc.Run(prg)` 取代原来的阻塞调用。

```go
type program struct { run func(context.Context); shutdown func(); cancel context.CancelFunc }

func (p *program) Start(s service.Service) error {   // 不能阻塞
    ctx, cancel := context.WithCancel(context.Background())
    p.cancel = cancel
    go p.run(ctx)
    return nil
}
func (p *program) Stop(s service.Service) error {
    p.cancel()
    p.shutdown()
    return nil
}
```

**向后兼容**：终端里直接跑 `tether serve` 时，kardianos 检测到无服务管理器
（`service.Interactive() == true`），`svc.Run()` 等价于"调 Start → 阻塞等 Ctrl-C/SIGTERM → 调 Stop"，
体验与现状一致。仅被 SCM/systemd/launchd 拉起时才由 kardianos 接管生命周期。三平台共用同一层包装。

### 4.3 运行时如何认领服务名（`TETHER_SERVICE_NAME`）

Windows 下进程向 SCM 报到必须报注册时的服务名，名字对不上 SCM 不认。但一个二进制承载多实例
（hub-b / node-b 共用一个 `tether.exe`），进程启动时本身不知道"这次以哪个服务名被拉起"。

解决：`service install` 时自动往该服务的环境注入 `TETHER_SERVICE_NAME=<name>`；serve/join
启动时读它设 kardianos `Config.Name`。

- 被 SCM 拉起 → 环境有 `TETHER_SERVICE_NAME` → 用它报到，匹配成功。
- 终端手动跑 → 无此变量 → `Interactive()==true` → 正常直跑，名字无关。

`--args` 因此保持干净（只写 `join --config ...`），服务名由 install 自动处理。

## 5. 环境 / PATH 注入

三平台 service 环境的 PATH 都很精简，node 的 exec/bash 需要能找到用户工具链。install 时：

1. 自动抓取当前 shell 的 `$PATH`，写进 service 定义（systemd `Environment=`、launchd
   `EnvironmentVariables`、SCM 经 kardianos `EnvVars`）。
2. `--env K=V` 合并在后，可覆盖 `PATH` 等任意变量。
3. 自动注入 `TETHER_SERVICE_NAME=<name>`（§4.3）。

这条同时兜住 Windows LocalSystem/Session 0 下 node exec/bash 找不到工具链的问题——即便无交互
桌面，注入的 PATH 也保证工具链可达（仅损失桌面/keychain 上下文，对以 exec/bash/文件为主的
node 足够）。

## 6. 自愈 / 重启策略

| 平台 | 配置 |
|---|---|
| systemd | `Option{"Restart": "always", "RestartSec": 5}` |
| launchd | kardianos 默认 `KeepAlive=true`（崩溃秒级拉起） |
| Windows SCM | `Option{"OnFailure": "restart", "OnFailureDelayDuration": 5s, "OnFailureResetPeriod": 60s}` —— 秒级失败动作，取代 1 分钟 Task Scheduler watchdog |

## 7. 代码落点

```
main.go                       + case "service": cli.Service(args[2:], stderr)
internal/cli/service.go       子命令解析 + install/start/stop/uninstall/status 分发
internal/service/program.go   kardianos Program 包装（serve/join 共用）
internal/service/config.go    从 name/args/env/resolved-scope 构造 service.Config（含 PATH 抓取、
                              TETHER_SERVICE_NAME 注入、平台 Option）
internal/service/scope.go     system/user 自动决策（按 runtime.GOOS）+ Windows --user 报错
internal/cli/serve.go         改为经 internal/service 跑
internal/cli/join.go          改为经 internal/service 跑
go.mod                        + github.com/kardianos/service
dist/                         更新/删除手工模板；README 记新命令
```

Linux user 服务的 lingering：kardianos 不自动开，install 在 Linux+user 场景额外执行
`loginctl enable-linger <user>`。

## 8. 测试计划

**单元（纯函数，不碰真 OS）：**

- `service.Config` 构造：给定 name/args/env/scope，断言生成的 kardianos `Config` 字段——
  `Name`、`Arguments`、`EnvVars`（含抓取的 `PATH` + `TETHER_SERVICE_NAME`）、`Option`
  （`UserService` / `Restart` / `OnFailure` 按平台正确）。
- scope 自动决策：按 `runtime.GOOS` 断言预期 scope；Windows + `--user` 返回错误。
- serve/join 的 Program 生命周期：`Start` 起 goroutine、`Stop` 触发 context 取消并调 `Shutdown`。

**手工 / E2E（写进部署 runbook）：**

- 三台真机各装一次：Linux（user + lingering）、macOS（LaunchAgent）、Windows（SCM）。
- 各跑一次 `exec` 验证注入的 PATH 生效。
- kill 进程验证自愈延迟（预期秒级，Windows 不再是 1 分钟）。

## 9. 迁移与兼容

- 现有手工 setup 与新命令可并存；逐台迁移。
- `dist/systemd/*.service`、`dist/mac-restart.sh` 在新命令验证通过后清理，README/design.md §7
  的 "Service 集成" 段更新为 `tether service install`。
- 平时手动 `tether serve` / `tether join`（含 `--once` / `--tail` 调试）行为不变（§4.2 向后兼容）。
