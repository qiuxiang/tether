# Drop process management; simplify exec to a plain subprocess

Date: 2026-05-21

## Motivation

The node currently keeps a process registry and exposes process-management
tools (`start_process`, `list_processes`, `capture_screen`, `send_stdin`,
`kill_process`). `exec` runs commands inside a PTY with a `vt10x` terminal
emulator and a live byte-fanout bus, reachable via a three-step
`Start` → `Attach` → stream dance.

In practice this is worse than just using `tmux` for long-running and
interactive work. We are removing process management entirely and reducing
`exec` to a plain synchronous subprocess: run the command, wait, return its
output. For long-running or interactive sessions, callers run `tmux` through
`exec`.

## Scope

In scope:

- Remove the five process-management MCP tools and their protocol/node code.
- Remove the PTY, `vt10x` terminal emulator, byte-bus, and process registry.
- Replace `exec` with a one-shot request/reply over a plain subprocess.

Out of scope: port forwarding, file transfer, and file edit tools — all
unchanged.

## Decisions

- **Timeout**: enforced on the node. When a command runs past its timeout the
  node kills the subprocess group, then replies `OK` with `timed_out: true`
  and whatever output was buffered. No orphaned processes.
- **Output**: `stdout` and `stderr` are returned as separate fields (a plain
  subprocess has separate streams; no PTY merge).
- **Stdin**: dropped. The subprocess gets a closed/empty stdin. Callers use
  shell heredocs or files.
- **Output cap**: each of `stdout` and `stderr` is capped at 4 MiB. This is
  comfortably under the 16 MiB `WSReadLimit`. If a stream hits the cap, the
  reply sets `truncated: true`.
- **Handler rename**: `ProcessHandler` → `Handler`, `NewProcessHandler` →
  `NewHandler`. The type no longer manages processes; it dispatches exec,
  file, edit, and forward messages.

## Protocol changes (`internal/protocol/messages.go`)

Remove these message types, their `msgType()` methods, and their codec cases:
`Start`, `Stdin`, `Kill`, `CaptureScreen`, `List`, `Attach`, `Detach`,
`ProcessOutput`, `ProcessExit`.

`Event` is retained for `device_online` / `device_offline` (used by port
forwarding). Its `"exit"` kind and the `ProcessID` / `Code` fields used only
by process exit are no longer produced.

Add one message:

```go
type Exec struct {
    Type    string            `cbor:"type"`     // "exec"
    MsgID   string            `cbor:"msg_id"`
    Target  string            `cbor:"target,omitempty"`
    Cmd     []string          `cbor:"cmd"`      // ["sh", "-c", <cmd>]
    Cwd     string            `cbor:"cwd,omitempty"`
    Env     map[string]string `cbor:"env,omitempty"`
    Timeout int               `cbor:"timeout,omitempty"` // seconds; 0 → default 30
}
```

The result is returned as a standard `Reply`:

```
Reply{OK: true, Data: {
    stdout:    string,
    stderr:    string,
    exit_code: int,    // -1 when terminated by signal (e.g. the timeout kill)
    timed_out: bool,
    truncated: bool,
}}
```

A failure to start the command (bad cwd, command not found at the OS level)
returns `Reply{OK: false, Error: ...}`.

## Node changes (`internal/node/`)

Delete `process.go`, `pty.go`, `vt.go`, `bytebus.go`, `registry.go` and their
tests. Remove the `go-pty` and `vt10x` dependencies from `go.mod` / `go.sum`.

New `exec.go`:

- Run the command with `exec.CommandContext` and a context deadline derived
  from `Timeout` (default 30s when 0).
- Set `Cmd.Dir` from `Cwd` and `Cmd.Env` from the merged environment.
- Set the child as its own process group (`SysProcAttr.Setpgid: true`) so the
  whole group can be killed.
- Capture stdout and stderr into two separate capped buffers (4 MiB each; a
  capped writer that stops appending and records that it overflowed).
- On the context deadline, call `killGroup(pid)`, then collect buffered
  output and reply with `timed_out: true`.
- Derive `exit_code` from `*exec.ExitError` (it is `-1` when the process was
  terminated by a signal, including the timeout kill).

`procattr_*.go`: rename `childAttrPTY` → `childAttrExec`. It returns
`SysProcAttr{Setpgid: true}` on Unix (plus `Pdeathsig: SIGKILL` on Linux);
`nil`/no-op on Windows. `killGroup` is unchanged.

`handler.go`: rename `ProcessHandler` → `Handler` and `NewProcessHandler` →
`NewHandler`. Drop the `logDir` and `cap` constructor parameters and the
`registry` / `attachSubs` fields. Remove the `Start`, `Kill`, `Stdin`,
`CaptureScreen`, `List`, `Attach`, `Detach` dispatch cases and their handler
methods. Add a `case *protocol.Exec` that runs the command and sends the
`Reply`. `Shutdown` no longer iterates a process registry; it only shuts down
the forward handler.

Update callers of `NewProcessHandler` — `internal/cli/serve.go`,
`internal/cli/join.go` — for the renamed constructor and dropped parameters.

## Client changes (`internal/client/tools_exec.go`)

Remove the `start_process`, `list_processes`, `capture_screen`, `send_stdin`,
and `kill_process` tools.

Keep `list_devices`. Rewrite `exec`:

- Inputs: `device` (required), `cmd` (required), `cwd`, `env`, `timeout`.
  Drop `stdin` and `description`.
- Send a single `Exec` message; wait for one `Reply`.
- Return `{stdout, stderr, exit_code, timed_out, truncated}`.
- Keep a client-side safety timeout slightly longer than the requested
  timeout so a node that never replies still surfaces an error.

`RegisterStream` on the client RPC is no longer used by exec but is retained —
file transfer still streams.

## Hub changes (`internal/hub/client_ws.go`)

Remove the dispatch cases for `Start`, `Stdin`, `Kill`, `CaptureScreen`,
`List`, `Attach`, `Detach`. Add `case *protocol.Exec: cs.routeOneShot(...)`.

Sticky routing (`routeStream`, `RegisterNode`) is retained — file transfer
still depends on it.

## Documentation

Update `docs/design.md` and `README.md` to drop process management and the
PTY/terminal-emulator description, and to describe `exec` as a plain
synchronous subprocess.

## Testing

- Delete tests for the removed node files (process/pty/vt/bus/registry).
- Trim `internal/hub/registry_test.go`, `internal/hub/server_test.go`, and
  the `device_ws` tests of process/attach assertions.
- Rewrite the exec e2e to cover: successful command with stdout/stderr,
  non-zero exit code, and a command that exceeds its timeout (asserting the
  node kills it and returns `timed_out: true` with partial output).

## Out-of-scope / explicitly not done

- No background/async exec. Long-running work is the caller's responsibility
  via `tmux` (or similar) launched through `exec`.
- No process inspection or listing of any kind.
