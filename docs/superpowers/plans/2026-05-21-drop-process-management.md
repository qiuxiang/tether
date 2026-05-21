# Drop Process Management; Simplify exec Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the node's process registry and PTY/terminal-emulator stack, and reduce `exec` to a plain synchronous subprocess (run, wait, return stdout/stderr/exit code).

**Architecture:** Two phases. Phase 1 (Tasks 1–6) adds the new `Exec` request/reply path additively — the build stays green and the old process-management path keeps working. Phase 2 (Tasks 7–10) deletes the now-dead code. Each task ends with a green `go build ./...` and `go test ./...`.

**Tech Stack:** Go, CBOR (`fxamacker/cbor`), `coder/websocket`, `mark3labs/mcp-go`, `testify`.

**Spec:** `docs/superpowers/specs/2026-05-21-drop-process-management-design.md`

---

## File Structure

**Phase 1 — created/modified (additive):**
- `internal/protocol/messages.go` — add `Exec` message type.
- `internal/protocol/codec.go` — add `Exec` to encode/decode switches.
- `internal/protocol/codec_test.go` — add `Exec` round-trip test.
- `internal/node/exec.go` *(new)* — `runExec` + `cappedBuffer`, the plain-subprocess runner.
- `internal/node/exec_run_test.go` *(new)* — tests for `runExec`/`cappedBuffer`.
- `internal/node/procattr_{linux,darwin,windows}.go` — add `childAttrExec`.
- `internal/node/handler.go` — add `Exec` dispatch case + `handleExec`.
- `internal/node/handler_test.go` — add `TestHandleExec`.
- `internal/hub/client_ws.go` — add `Exec` routing case.
- `internal/client/tools_exec.go` — rewrite `exec` tool, remove 4 process tools.
- `e2e_test.go` — replace exec/capture-screen e2e tests.

**Phase 2 — deleted:**
- `internal/node/process.go`, `pty.go`, `vt.go`, `bytebus.go`, `registry.go` and their `_test.go` files; old `exec_test.go`.
- Process/PTY message types from `protocol`; old dispatch cases from node/hub; `go-pty` + `vt10x` deps.

---

## Task 1: Add the `Exec` protocol message

**Files:**
- Modify: `internal/protocol/messages.go`
- Modify: `internal/protocol/codec.go`
- Test: `internal/protocol/codec_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/protocol/codec_test.go`:

```go
func TestRoundTripExec(t *testing.T) {
	in := &Exec{
		MsgID:   "m1",
		Target:  "host-a",
		Cmd:     []string{"sh", "-c", "ls"},
		Cwd:     "/tmp",
		Env:     map[string]string{"A": "b"},
		Timeout: 10,
	}
	enc, err := Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := Decode(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := decoded.(*Exec)
	if !ok {
		t.Fatalf("decoded type = %T, want *Exec", decoded)
	}
	if got.MsgID != in.MsgID || got.Target != in.Target || got.Cwd != in.Cwd || got.Timeout != in.Timeout {
		t.Fatalf("scalar mismatch: %+v vs %+v", got, in)
	}
	if len(got.Cmd) != 3 || got.Cmd[2] != "ls" || got.Env["A"] != "b" {
		t.Fatalf("slice/map mismatch: %+v", got)
	}
	if got.Type != "exec" {
		t.Fatalf("Type = %q, want exec", got.Type)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/protocol/ -run TestRoundTripExec`
Expected: FAIL — compile error, `undefined: Exec`.

- [ ] **Step 3: Add the `Exec` struct**

In `internal/protocol/messages.go`, add after the `Start` struct:

```go
// Exec — client → hub → node. Runs Cmd as a plain subprocess, waits for it
// to exit (or until Timeout seconds elapse, default 30, after which the node
// kills the process group), and returns the result in a single Reply.
// Reply.Data: {stdout string, stderr string, exit_code int, timed_out bool,
// truncated bool}.
type Exec struct {
	Type    string            `cbor:"type"`
	MsgID   string            `cbor:"msg_id"`
	Target  string            `cbor:"target,omitempty"`
	Cmd     []string          `cbor:"cmd"`
	Cwd     string            `cbor:"cwd,omitempty"`
	Env     map[string]string `cbor:"env,omitempty"`
	Timeout int               `cbor:"timeout,omitempty"`
}
```

In the `msgType()` block of `messages.go`, add:

```go
func (m *Exec) msgType() string { return "exec" }
```

- [ ] **Step 4: Wire `Exec` into the codec**

In `internal/protocol/codec.go`, add a case to `setType`'s switch:

```go
	case *Exec:
		v.Type = m.msgType()
```

And a case to `Decode`'s switch:

```go
	case "exec":
		m = &Exec{}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/protocol/ -run TestRoundTripExec -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/protocol/messages.go internal/protocol/codec.go internal/protocol/codec_test.go
git commit -m "protocol: add exec message type"
```

---

## Task 2: Node — `childAttrExec` and the `runExec` subprocess runner

**Files:**
- Modify: `internal/node/procattr_linux.go`, `procattr_darwin.go`, `procattr_windows.go`
- Create: `internal/node/exec.go`
- Test: `internal/node/exec_run_test.go` *(new)*

Note: `mergeEnv` already exists in `internal/node/process.go` (same package) — `exec.go` reuses it. It is moved into `exec.go` in Task 7 when `process.go` is deleted.

- [ ] **Step 1: Write the failing tests**

Create `internal/node/exec_run_test.go`:

```go
package node

import (
	"context"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCappedBuffer(t *testing.T) {
	w := &cappedBuffer{cap: 4}
	n, err := w.Write([]byte("ab"))
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.False(t, w.truncated)

	n, err = w.Write([]byte("cdef"))
	require.NoError(t, err)
	assert.Equal(t, 4, n, "Write must report the full input length")
	assert.Equal(t, "abcd", w.buf.String())
	assert.True(t, w.truncated)
}

func TestRunExecCapturesOutputAndExit(t *testing.T) {
	res, err := runExec(context.Background(), &protocol.Exec{
		Cmd: []string{"sh", "-c", "echo out; echo err 1>&2; exit 7"},
	})
	require.NoError(t, err)
	assert.Contains(t, res.Stdout, "out")
	assert.Contains(t, res.Stderr, "err")
	assert.Equal(t, 7, res.ExitCode)
	assert.False(t, res.TimedOut)
	assert.False(t, res.Truncated)
}

func TestRunExecTimeoutKills(t *testing.T) {
	start := time.Now()
	res, err := runExec(context.Background(), &protocol.Exec{
		Cmd:     []string{"sh", "-c", "echo started; sleep 30"},
		Timeout: 1,
	})
	require.NoError(t, err)
	assert.True(t, res.TimedOut, "expected timed_out")
	assert.Contains(t, res.Stdout, "started")
	assert.Less(t, time.Since(start), 10*time.Second, "runExec must return shortly after the timeout")
}

func TestRunExecStartError(t *testing.T) {
	_, err := runExec(context.Background(), &protocol.Exec{
		Cmd: []string{"sh", "-c", "true"},
		Cwd: "/no/such/directory/exists",
	})
	require.Error(t, err, "a bad working directory must surface as a start error")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/node/ -run 'TestCappedBuffer|TestRunExec'`
Expected: FAIL — compile error, `undefined: cappedBuffer`, `undefined: runExec`.

- [ ] **Step 3: Add `childAttrExec` to the three procattr files**

In `internal/node/procattr_linux.go`, add:

```go
// childAttrExec returns SysProcAttr for plain exec children: a new process
// group so killGroup can signal the whole group, plus Pdeathsig so a child
// dies if the agent does.
func childAttrExec() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Setpgid:   true,
		Pdeathsig: syscall.SIGKILL,
	}
}
```

In `internal/node/procattr_darwin.go`, add:

```go
// childAttrExec returns SysProcAttr for plain exec children: a new process
// group so killGroup can signal the whole group.
func childAttrExec() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
```

In `internal/node/procattr_windows.go`, add:

```go
// childAttrExec is a no-op on Windows; killGroup is also a no-op there and
// exec.CommandContext's default kill applies.
func childAttrExec() *syscall.SysProcAttr { return nil }
```

- [ ] **Step 4: Create `internal/node/exec.go`**

```go
package node

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
)

const (
	// execOutputCap bounds each of stdout/stderr. 4 MiB each stays well under
	// the 16 MiB WSReadLimit once both are packed into one Reply.
	execOutputCap      = 4 << 20
	defaultExecTimeout = 30 * time.Second
)

// execResult is the outcome of running a command via runExec.
type execResult struct {
	Stdout    string
	Stderr    string
	ExitCode  int
	TimedOut  bool
	Truncated bool
}

// cappedBuffer is an io.Writer that retains at most cap bytes and records
// whether it had to drop any. Write always reports the full input length so
// the process's write never sees a short write.
type cappedBuffer struct {
	buf       bytes.Buffer
	cap       int
	truncated bool
}

func (w *cappedBuffer) Write(p []byte) (int, error) {
	room := w.cap - w.buf.Len()
	switch {
	case room <= 0:
		if len(p) > 0 {
			w.truncated = true
		}
	case len(p) <= room:
		w.buf.Write(p)
	default:
		w.buf.Write(p[:room])
		w.truncated = true
	}
	return len(p), nil
}

// runExec runs m.Cmd to completion or until the timeout, whichever comes
// first. On timeout the whole process group is killed and TimedOut is set.
// The returned error is non-nil only when the process failed to start at the
// OS level (e.g. a bad working directory).
func runExec(ctx context.Context, m *protocol.Exec) (execResult, error) {
	timeout := defaultExecTimeout
	if m.Timeout > 0 {
		timeout = time.Duration(m.Timeout) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c := exec.CommandContext(ctx, m.Cmd[0], m.Cmd[1:]...)
	c.Dir = m.Cwd
	c.Env = mergeEnv(m.Env)
	c.SysProcAttr = childAttrExec()

	stdout := &cappedBuffer{cap: execOutputCap}
	stderr := &cappedBuffer{cap: execOutputCap}
	c.Stdout = stdout
	c.Stderr = stderr

	if err := c.Start(); err != nil {
		return execResult{}, err
	}

	// On timeout, kill the whole process group so children don't outlive us.
	pgid := c.Process.Pid
	c.Cancel = func() error {
		killGroup(pgid)
		return nil
	}
	// If a grandchild keeps the output pipe open after the group is killed,
	// don't let Wait hang forever.
	c.WaitDelay = 5 * time.Second

	err := c.Wait()

	res := execResult{
		Stdout:    stdout.buf.String(),
		Stderr:    stderr.buf.String(),
		Truncated: stdout.truncated || stderr.truncated,
	}
	if ctx.Err() == context.DeadlineExceeded {
		res.TimedOut = true
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		res.ExitCode = exitErr.ExitCode()
	} else if err != nil {
		res.ExitCode = -1
	}
	return res, nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/node/ -run 'TestCappedBuffer|TestRunExec' -v`
Expected: PASS (4 tests).

- [ ] **Step 6: Verify the whole build still compiles**

Run: `go build ./...`
Expected: no output (success).

- [ ] **Step 7: Commit**

```bash
git add internal/node/exec.go internal/node/exec_run_test.go internal/node/procattr_linux.go internal/node/procattr_darwin.go internal/node/procattr_windows.go
git commit -m "node: add runExec plain-subprocess runner"
```

---

## Task 3: Node handler — dispatch `Exec`

**Files:**
- Modify: `internal/node/handler.go`
- Test: `internal/node/handler_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/node/handler_test.go`:

```go
func TestHandleExec(t *testing.T) {
	h := NewProcessHandler(t.TempDir(), 16)
	s := &captureSender{msgs: make(chan protocol.Message, 8)}
	h.Handle(context.Background(), s, &protocol.Exec{
		MsgID: "e1",
		Cmd:   []string{"sh", "-c", "echo hi; echo bad 1>&2; exit 2"},
	})
	r := awaitReply(t, s.msgs)
	if !r.OK {
		t.Fatalf("exec reply not OK: %+v", r)
	}
	if got, _ := r.Data["stdout"].(string); !strings.Contains(got, "hi") {
		t.Fatalf("stdout = %q, want it to contain hi", got)
	}
	if got, _ := r.Data["stderr"].(string); !strings.Contains(got, "bad") {
		t.Fatalf("stderr = %q, want it to contain bad", got)
	}
	if r.Data["exit_code"] != 2 {
		t.Fatalf("exit_code = %v, want 2", r.Data["exit_code"])
	}
}
```

If `handler_test.go` does not already import `strings`, add it to the import block.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/node/ -run TestHandleExec`
Expected: FAIL — `awaitReply` returns no `*protocol.Reply` (the `Exec` case does not exist yet, so nothing is sent), test times out / fatals.

- [ ] **Step 3: Add the `Exec` dispatch case and handler method**

In `internal/node/handler.go`, add a case to the `Handle` switch (alongside the other `case *protocol.*` entries):

```go
	case *protocol.Exec:
		go h.handleExec(send, m)
```

Add the handler method (place it near `handleStart`):

```go
func (h *ProcessHandler) handleExec(send Sender, m *protocol.Exec) {
	res, err := runExec(context.Background(), m)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"stdout":    res.Stdout,
		"stderr":    res.Stderr,
		"exit_code": res.ExitCode,
		"timed_out": res.TimedOut,
		"truncated": res.Truncated,
	}})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/node/ -run TestHandleExec -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/node/handler.go internal/node/handler_test.go
git commit -m "node: dispatch exec messages to runExec"
```

---

## Task 4: Hub — route `Exec`

**Files:**
- Modify: `internal/hub/client_ws.go`

`Exec` is a single request with a single `Reply`, so it uses the existing one-shot routing. No new test here — Task 6's e2e exercises hub routing end to end.

- [ ] **Step 1: Add the dispatch case**

In `internal/hub/client_ws.go`, inside `dispatch`'s switch, add (next to the other `routeOneShot` cases):

```go
	case *protocol.Exec:
		cs.routeOneShot(m.MsgID, m.Target, raw)
```

- [ ] **Step 2: Verify build and existing hub tests**

Run: `go build ./... && go test ./internal/hub/`
Expected: build succeeds; hub tests PASS.

- [ ] **Step 3: Commit**

```bash
git add internal/hub/client_ws.go
git commit -m "hub: route exec messages one-shot to the target node"
```

---

## Task 5: Client — rewrite the `exec` tool, remove process tools

**Files:**
- Modify: `internal/client/tools_exec.go`
- Test: `internal/client/tools_exec_test.go`

This replaces the `Start`→`Attach`→stream `exec`, and removes `start_process`,
`list_processes`, `capture_screen`, `send_stdin`, `kill_process`. The tools left
in `registerExecTools` are exactly two: `list_devices` and `exec`.

- [ ] **Step 1: Inspect the current test file**

Run: `cat internal/client/tools_exec_test.go`
Note which tests reference the removed tools or the `Start`/`Attach` flow — those are rewritten/removed in Step 4.

- [ ] **Step 2: Rewrite `registerExecTools`**

In `internal/client/tools_exec.go`, replace the entire body of `registerExecTools` so it registers only `list_devices` (unchanged from the current code) and the new `exec` below. Delete the `start_process`, `list_processes`, `capture_screen`, `send_stdin`, and `kill_process` registrations.

New `exec` tool (registered after `list_devices`):

```go
	m.AddTool(
		mcp.NewTool("exec",
			mcp.WithDescription("Run a command on a device as a plain subprocess (sh -c), wait for it to exit, and return its output. If the command does not exit within `timeout` seconds (default 30), the device kills its process group and returns timed_out=true with whatever output was captured. Returns {stdout, stderr, exit_code, timed_out, truncated}. For long-running or interactive work, run tmux through this tool."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("cmd", mcp.Required(), mcp.Description("Shell command (passed to sh -c)")),
			mcp.WithString("cwd"),
			mcp.WithObject("env"),
			mcp.WithNumber("timeout", mcp.Description("Seconds to wait before the device kills the command. Default 30.")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			cmd, _ := args["cmd"].(string)
			cwd, _ := args["cwd"].(string)
			envMap := extractStringMap(args["env"])

			timeoutSecs := 30
			if t, ok := args["timeout"].(float64); ok && t > 0 {
				timeoutSecs = int(t)
			}

			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.Exec{
				MsgID: id, Target: device,
				Cmd: []string{"sh", "-c", cmd}, Cwd: cwd, Env: envMap,
				Timeout: timeoutSecs,
			}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}

			// Client-side safety net: wait a bit longer than the node's own
			// timeout so a node that never replies still surfaces an error.
			safety := time.Duration(timeoutSecs)*time.Second + 15*time.Second
			select {
			case reply := <-ch:
				if !reply.OK {
					return mcp.NewToolResultError(reply.Error), nil
				}
				b, _ := json.Marshal(reply.Data)
				return mcp.NewToolResultText(string(b)), nil
			case <-time.After(safety):
				return mcp.NewToolResultError("timeout"), nil
			case <-ctx.Done():
				return mcp.NewToolResultError(ctx.Err().Error()), nil
			}
		},
	)
```

- [ ] **Step 3: Delete now-unused helpers and imports**

In `tools_exec.go`, delete the `resultExec` function (only the old streaming `exec` used it) and the `fetchDevices` function (only `list_processes` fan-out used it). Keep `extractStringMap` (still used by `exec`) and `NewMsgID`.

After deleting `fetchDevices`, the `errors` import is unused — remove it. Run `goimports`/`go build` to confirm the final import set; `defaultRPCTimeout` is still used by `list_devices`, keep it.

- [ ] **Step 4: Rewrite `tools_exec_test.go`**

Replace `internal/client/tools_exec_test.go` so it tests only the surviving tools. Remove any test of `start_process`/`list_processes`/`capture_screen`/`send_stdin`/`kill_process` and of the old `Start`/`Attach` `exec`. Keep or adapt the `list_devices` test. Add a test that the `exec` tool sends a `protocol.Exec` frame and returns the node's `Reply.Data` as JSON — model it on the existing test harness in this file (reuse whatever fake `Conn`/transport the file already uses; do not invent a new one).

If, after pruning, no meaningful client-side unit test remains for `exec` beyond the e2e coverage in Task 6, it is acceptable to keep just the `list_devices` test plus a `protocol.Exec` encoding assertion — but do not leave the file referencing deleted symbols.

- [ ] **Step 5: Run the client tests**

Run: `go test ./internal/client/`
Expected: PASS.

- [ ] **Step 6: Verify the whole build**

Run: `go build ./...`
Expected: success.

- [ ] **Step 7: Commit**

```bash
git add internal/client/tools_exec.go internal/client/tools_exec_test.go
git commit -m "client: rewrite exec as a plain subprocess; drop process tools"
```

---

## Task 6: E2E — replace exec tests

**Files:**
- Modify: `e2e_test.go`

Replace `TestE2EExec`, `TestE2EExecTimeoutRecovery`, and `TestE2ECaptureScreen` with the two tests below. The hub/node/client setup boilerplate is identical to the current `TestE2EExec` (lines ~28–50) — copy it verbatim, including `node.NewProcessHandler(t.TempDir(), 50)` (that call is renamed in Task 7).

- [ ] **Step 1: Replace the three tests**

Delete `TestE2EExec`, `TestE2EExecTimeoutRecovery`, and `TestE2ECaptureScreen` entirely. Add:

```go
func TestE2EExec(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host"})
	nc.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
	go nc.Run(ctx)
	require.Eventually(t, func() bool {
		_, ok := s.Registry().Get("e2e-host")
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	c := client.NewConn(client.Config{HubURL: cliURL, Token: "secret"})
	go c.Run(ctx)
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, c.WaitReady(cctx))
	ccancel()

	id := client.NewMsgID()
	ch := c.RPC().Register(id)
	defer c.RPC().Unregister(id)
	require.NoError(t, c.Send(&protocol.Exec{
		MsgID:  id,
		Target: "e2e-host",
		Cmd:    []string{"sh", "-c", "echo hello; echo oops 1>&2; exit 3"},
	}))

	var reply *protocol.Reply
	select {
	case reply = <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("exec reply timeout")
	}
	require.True(t, reply.OK, "exec reply: %+v", reply)
	assert.Contains(t, reply.Data["stdout"].(string), "hello")
	assert.Contains(t, reply.Data["stderr"].(string), "oops")
	assert.EqualValues(t, 3, reply.Data["exit_code"])
	assert.Equal(t, false, reply.Data["timed_out"])
}

func TestE2EExecTimeout(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host"})
	nc.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
	go nc.Run(ctx)
	require.Eventually(t, func() bool {
		_, ok := s.Registry().Get("e2e-host")
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	c := client.NewConn(client.Config{HubURL: cliURL, Token: "secret"})
	go c.Run(ctx)
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, c.WaitReady(cctx))
	ccancel()

	id := client.NewMsgID()
	ch := c.RPC().Register(id)
	defer c.RPC().Unregister(id)
	start := time.Now()
	require.NoError(t, c.Send(&protocol.Exec{
		MsgID:   id,
		Target:  "e2e-host",
		Cmd:     []string{"sh", "-c", "echo started; sleep 30"},
		Timeout: 1,
	}))

	var reply *protocol.Reply
	select {
	case reply = <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("exec reply timeout")
	}
	require.True(t, reply.OK, "exec reply: %+v", reply)
	assert.Equal(t, true, reply.Data["timed_out"])
	assert.Contains(t, reply.Data["stdout"].(string), "started")
	assert.Less(t, time.Since(start), 10*time.Second, "exec must return shortly after the node-side timeout")
}
```

- [ ] **Step 2: Run the e2e exec tests**

Run: `go test . -run 'TestE2EExec|TestE2EExecTimeout' -v`
Expected: PASS (both).

- [ ] **Step 3: Run the full e2e suite**

Run: `go test .`
Expected: PASS — the remaining e2e tests (file transfer, forwarding, remote edit) are unaffected.

- [ ] **Step 4: Commit**

```bash
git add e2e_test.go
git commit -m "e2e: cover plain-subprocess exec success and timeout"
```

---

## Task 7: Node cleanup — delete process/PTY stack, rename handler

**Files:**
- Delete: `internal/node/process.go`, `pty.go`, `vt.go`, `bytebus.go`, `registry.go`
- Delete: `internal/node/process_test.go`, `pty_test.go`, `vt_test.go`, `bytebus_test.go`, `registry_test.go`, `exec_test.go` (the old `Start`/`Attach` tests)
- Modify: `internal/node/handler.go`, `handler_test.go`, `exec.go`, `client_test.go`
- Modify: `internal/cli/join.go`
- Modify: `go.mod`, `go.sum`

After this task the `node` package no longer references `protocol.Start`, `Stdin`, `Kill`, `CaptureScreen`, `List`, `Attach`, `Detach`, `ProcessOutput`, or `ProcessExit`. Those types still exist in `protocol`, so the overall build stays green.

- [ ] **Step 1: Delete the dead node files**

```bash
git rm internal/node/process.go internal/node/pty.go internal/node/vt.go internal/node/bytebus.go internal/node/registry.go
git rm internal/node/process_test.go internal/node/pty_test.go internal/node/vt_test.go internal/node/bytebus_test.go internal/node/registry_test.go internal/node/exec_test.go
```

- [ ] **Step 2: Move `mergeEnv` into `exec.go`**

`mergeEnv` lived in the now-deleted `process.go`. Add it to `internal/node/exec.go` (it is used by `runExec`):

```go
func mergeEnv(extra map[string]string) []string {
	base := os.Environ()
	for k, v := range extra {
		base = append(base, fmt.Sprintf("%s=%s", k, v))
	}
	return base
}
```

Add `"fmt"` and `"os"` to `exec.go`'s import block.

- [ ] **Step 3: Strip the process machinery from `handler.go`**

In `internal/node/handler.go`:
- Rename the type `ProcessHandler` → `Handler` and the constructor `NewProcessHandler` → `NewHandler` everywhere in the file (including all method receivers).
- `NewHandler` takes no parameters. Drop the `logDir int`/`cap` parameters and the `registry` and `attachSubs` struct fields. The struct keeps only `fileHandler`, `editHandler`, `forwardHandler`.
- In `Handle`, delete the `case` entries for `*protocol.Start`, `*protocol.Kill`, `*protocol.Stdin`, `*protocol.CaptureScreen`, `*protocol.List`, `*protocol.Attach`, `*protocol.Detach`. Keep the `*protocol.Exec` case and everything else (file, edit, forward, `Event`, `Reply`).
- Delete the methods `handleStart`, `handleKill`, `handleStdin`, `handleCaptureScreen`, `handleAttach`, `handleDetach`, `registerAttach`, `unregisterAttach`, and the `attachRec` type.
- In `Shutdown`, delete the loop over `h.registry.List(...)` and the `killGroup` call; `Shutdown` now only calls `h.forwardHandler.Shutdown()`.

Verify `handler.go`'s imports afterward: `context` and `sync` may become unused — remove whatever `go build` reports.

- [ ] **Step 4: Fix `handler_test.go`**

In `internal/node/handler_test.go`:
- The `captureSender` type previously lived in the deleted `exec_test.go`. Add it to `handler_test.go`:

```go
type captureSender struct {
	msgs chan protocol.Message
}

func (c *captureSender) Send(m protocol.Message) error {
	c.msgs <- m
	return nil
}
```

- Delete `TestHandleCaptureScreen_NotFound`, `TestHandleList_IncludesLogPath`, and `TestHandleCaptureScreen_HappyPath` (they test removed functionality).
- Keep `TestHandleExec` and the `awaitReply` helper.
- Update the `TestHandleExec` handler construction from `NewProcessHandler(t.TempDir(), 16)` to `NewHandler()`.

- [ ] **Step 5: Fix remaining `node` references**

Run: `grep -rn "NewProcessHandler\|ProcessHandler\|childAttrPTY\|killGroup" internal/node/`
- `childAttrPTY` should now be referenced nowhere — if `procattr_*.go` still defines it, delete the `childAttrPTY` function from all three `procattr_*.go` files. `killGroup` is still used by `exec.go` — keep it.
- Fix any `client_test.go` (or other) references to `NewProcessHandler` → `NewHandler()`.

- [ ] **Step 6: Update `internal/cli/join.go`**

Change `node.NewProcessHandler(cfg.LogDir, 50)` to `node.NewHandler()`. If `cfg.LogDir` is now unreferenced anywhere, remove the `LogDir` field and its wiring from the node config (`internal/config/config.go` / `internal/client/config.go` — whichever defines it). Confirm with `grep -rn "LogDir" --include=*.go .` and only remove it if there are no remaining readers.

- [ ] **Step 7: Drop the `go-pty` and `vt10x` dependencies**

```bash
go mod tidy
```

Confirm `github.com/aymanbagabas/go-pty` and `github.com/hinshun/vt10x` are gone from `go.mod`.

- [ ] **Step 8: Build and test**

Run: `go build ./... && go test ./internal/node/ ./internal/cli/`
Expected: build succeeds; tests PASS.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "node: remove process registry and PTY stack; rename handler"
```

---

## Task 8: Hub cleanup — remove process dispatch and stream routing

**Files:**
- Modify: `internal/hub/client_ws.go`, `internal/hub/device_ws.go`
- Modify: hub test files that reference removed types

- [ ] **Step 1: Remove process cases from `client_ws.go`**

In `internal/hub/client_ws.go`'s `dispatch` switch, delete the `case` entries for `*protocol.Attach`, `*protocol.Detach`, `*protocol.Start`, `*protocol.Stdin`, `*protocol.Kill`, `*protocol.CaptureScreen`, and `*protocol.List`. Keep `*protocol.Exec`, `*protocol.ListDevices`, all the `File*` cases, and `*protocol.FileRelay`.

The sticky-routing helpers (`routeStream`, `routeFilePut`, `routeFileGet`, `RegisterNode`) stay — file transfer still uses them.

- [ ] **Step 2: Remove process stream routing from `device_ws.go`**

In `internal/hub/device_ws.go`, delete the handling of `*protocol.ProcessOutput` and `*protocol.ProcessExit` (around the current lines 236, 254–256). Node→client streaming is still needed for `*protocol.FileChunk`/`*protocol.FileAbort` — leave those paths intact. If a `ProcessExit`-specific route-teardown branch existed, ensure file streams still tear down via their existing `FileChunk{EOF}` path (do not remove that).

- [ ] **Step 3: Fix hub tests**

Run: `grep -rln "protocol\.\(Start\|Stdin\|Kill\|CaptureScreen\|Attach\|Detach\|ProcessOutput\|ProcessExit\)\|protocol\.List{" internal/hub/`
For each file reported (e.g. `registry_test.go`, `server_test.go`, `device_events_test.go`): remove or rewrite the tests that exercise the removed message types. Tests that assert generic routing can be re-pointed at `protocol.Exec`. Tests purely about process attach/detach/streaming are deleted. Do not weaken file-transfer or forwarding tests.

- [ ] **Step 4: Build and test**

Run: `go build ./... && go test ./internal/hub/`
Expected: build succeeds; tests PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "hub: drop process-management dispatch and stream routing"
```

---

## Task 9: Protocol cleanup — remove dead message types

**Files:**
- Modify: `internal/protocol/messages.go`, `codec.go`, `codec_test.go`
- Modify: `internal/client/rpc.go`, `internal/client/rpc_test.go`

At this point nothing outside `protocol` itself and the client RPC stream classifier references the old types. Remove the RPC references first, then the types.

- [ ] **Step 1: Clean up `rpc.go`**

In `internal/client/rpc.go`'s `Deliver` switch, delete the `case *protocol.ProcessOutput` and `case *protocol.ProcessExit` branches. Keep `*protocol.Reply`, `*protocol.FileChunk`, and `*protocol.FileAbort` — file transfer still streams. `RegisterStream`/`RegisterStreamRaw` stay.

- [ ] **Step 2: Fix `rpc_test.go`**

In `internal/client/rpc_test.go`, the stream-delivery test currently delivers `protocol.ProcessOutput` + `protocol.ProcessExit`. Rewrite it to exercise the surviving stream path with `protocol.FileChunk`: deliver two `FileChunk` frames (the second with `EOF: true`), assert both arrive on the stream channel and that the channel is closed after the `EOF` chunk. Remove all references to `ProcessOutput`/`ProcessExit`.

- [ ] **Step 3: Remove the message types from `messages.go`**

In `internal/protocol/messages.go`, delete the struct definitions for `Start`, `Stdin`, `Kill`, `CaptureScreen`, `List`, `Attach`, `Detach`, `ProcessOutput`, `ProcessExit`, and their corresponding `msgType()` methods. Keep `Exec`, `Hello`, `Reply`, `Event`, `ListDevices`, all `File*` types, the `*Req` file-edit types, and the `Forward*` types. `Event` stays as-is (still used for `device_online`/`device_offline`).

- [ ] **Step 4: Remove the codec cases**

In `internal/protocol/codec.go`, delete from `setType` the `case *Start/*Stdin/*Kill/*CaptureScreen/*List/*Attach/*Detach/*ProcessOutput/*ProcessExit` entries, and delete from `Decode` the matching `case "start"/"stdin"/"kill"/"capture_screen"/"list"/"attach"/"detach"/"process_output"/"process_exit"` entries.

- [ ] **Step 5: Fix `codec_test.go`**

In `internal/protocol/codec_test.go`, delete `TestEncodeDecodeProcessOutput`, `TestRoundTripStartWithTarget`, `TestRoundTripAttach`, `TestRoundTripProcessExit`, and `TestRoundTripCaptureScreen`. Keep `TestRoundTripExec` (added in Task 1) and all non-process round-trip tests.

- [ ] **Step 6: Build and test everything**

Run: `go build ./... && go test ./...`
Expected: build succeeds; the full suite PASSES.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "protocol: remove process-management message types"
```

---

## Task 10: Documentation

**Files:**
- Modify: `docs/design.md`, `README.md`

- [ ] **Step 1: Update `docs/design.md`**

Run: `grep -n "exec\|process\|pty\|PTY\|terminal\|capture_screen\|start_process\|list_processes\|kill_process\|send_stdin\|attach" docs/design.md`
For every hit: remove descriptions of process management (the five removed tools, the process registry) and of the PTY / `vt10x` terminal emulator / byte-bus / `Attach`/`Detach` streaming. Rewrite the `exec` description to: a plain synchronous subprocess (`sh -c`) that runs to completion or is killed by the node on timeout, returning `stdout`, `stderr`, `exit_code`, `timed_out`, `truncated`. Note that long-running/interactive work is done by running `tmux` through `exec`.

- [ ] **Step 2: Update `README.md`**

Run: `grep -n "exec\|process\|pty\|capture_screen\|start_process\|list_processes\|kill_process\|send_stdin" README.md`
Apply the same edits: drop the removed tools from any tool list, drop PTY/terminal-emulator wording, and describe `exec` as a plain subprocess. If there is an MCP-tools table/list, it should now show `list_devices`, `exec`, and the file/edit/forward tools only.

- [ ] **Step 3: Sanity-check for stale references**

Run: `grep -rn "start_process\|list_processes\|capture_screen\|send_stdin\|kill_process\|ProcessHandler" --include=*.md docs/ README.md`
Expected: no hits (other than the spec and this plan under `docs/superpowers/`).

- [ ] **Step 4: Commit**

```bash
git add docs/design.md README.md
git commit -m "docs: describe exec as a plain subprocess; drop process management"
```

---

## Final Verification

- [ ] Run `go build ./...` — succeeds.
- [ ] Run `go test ./...` — full suite passes.
- [ ] Run `go vet ./...` — no warnings introduced.
- [ ] Confirm `go.mod` no longer lists `go-pty` or `vt10x`.
- [ ] Confirm `grep -rn "ProcessHandler\|startPTY\|byteBus\|vt10x" internal/` returns nothing.

---

## Notes for the Executor

- **Pre-existing uncommitted changes.** The working tree already has modifications to `internal/hub/device_ws.go`, `registry.go`, `registry_test.go`, and `server_test.go`, plus untracked files (`cmd/`, `.mcp.json`, `config-hub.yaml`, built binaries). These are unrelated to this plan. Do not revert them and do not `git add` the untracked binaries/configs — stage only the files each task names.
- **`c.Cancel` / `c.WaitDelay`** on `exec.Cmd` require Go 1.20+. Confirm the `go` directive in `go.mod` is ≥ 1.20 (it should be — the codebase uses generics and modern libs). If older, raise it.
- The spec is the source of truth: `docs/superpowers/specs/2026-05-21-drop-process-management-design.md`.
