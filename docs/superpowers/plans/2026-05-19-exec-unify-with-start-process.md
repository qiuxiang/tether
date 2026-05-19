# Unify exec with start_process — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `exec` a thin wrapper over `start_process` so every command on a node is a tracked, observable process; add a freeform `description` annotation to both tools; drop `Name`/`timeout`/`Exec*` legacy.

**Architecture:** Replace `Exec*` protocol with `Start + Attach + ProcessOutput + ProcessExit + Detach`. Node-side every `Process` gets a byte-bus that buffers raw pty bytes and pushes deltas to live subscribers. `exec` MCP tool = client-side composition: `Start` → `Attach` → drain stream until exit or ctx cancel; on cancel the process keeps running and the tool returns a success result with `process_id` + `timed_out: true`. `description` replaces `name` on `Process` and is returned in `list_processes`. No internal timeout; only MCP ctx terminates the wait.

**Tech Stack:** Go, CBOR (fxamacker/cbor), coder/websocket, mark3labs/mcp-go, aymanbagabas/go-pty, hinshun/vt10x.

---

## File Structure

Modify:
- `internal/protocol/messages.go` — drop `Exec/ExecCancel/ExecOutput/ExecExit`; rename `Start.Name` → `Start.Description`; add `Attach/Detach/ProcessOutput/ProcessExit`.
- `internal/protocol/codec.go` — sync `setType` and `Decode` switches.
- `internal/node/process.go` — rename `Name` → `Description`; embed `*byteBus`; delete `runExecStream` wrapper.
- `internal/node/pty.go` — wire bus into the pty copy loop; delete `runExecStreamPTY`.
- `internal/node/handler.go` — add `Attach/Detach` handlers; delete `handleExec`/`handleExecCancel`/`execMu`/`execCancel`.
- `internal/node/registry.go` — `processSnapshot.Name` → `Description`.
- `internal/hub/client_ws.go` — drop `Exec`/`ExecCancel` cases; add `Attach`/`Detach`.
- `internal/hub/device_ws.go` — `msgID()` covers new node→client frames; replace `ExecExit` route unregister with `ProcessExit`.
- `internal/client/rpc.go` — `Deliver` handles `ProcessOutput`/`ProcessExit` instead of `ExecOutput`/`ExecExit`.
- `internal/client/tools_exec.go` — rewrite `exec` body; remove `timeout`/`name`; add `description`.

Create:
- `internal/node/bytebus.go` — append-only buffer + subscriber registry.
- `internal/node/bytebus_test.go`.

Test files updated:
- `internal/node/exec_test.go`, `internal/node/pty_test.go`, `internal/node/handler_test.go`, `internal/client/tools_exec_test.go`, `e2e_test.go`.

---

## Task 1: Protocol — add Attach/Detach/ProcessOutput/ProcessExit and rename Start.Name

**Files:**
- Modify: `internal/protocol/messages.go`
- Modify: `internal/protocol/codec.go`

- [ ] **Step 1: Edit `internal/protocol/messages.go`**

Replace the existing block between `Hub → Node` and `Node → Hub` plus the bottom Exec types and `msgType`/Decode entries.

Inside the file:

1. In the `Hub → Node` section, delete the `Exec` and `ExecCancel` struct declarations entirely.

2. Replace the `Start` struct with:

```go
type Start struct {
	Type        string            `cbor:"type"`
	MsgID       string            `cbor:"msg_id"`
	Target      string            `cbor:"target,omitempty"`
	ProcessID   string            `cbor:"process_id"`
	Cmd         []string          `cbor:"cmd"`
	Cwd         string            `cbor:"cwd,omitempty"`
	Env         map[string]string `cbor:"env,omitempty"`
	Description string            `cbor:"description,omitempty"`
}
```

3. After the `List` struct (still in the Hub → Node section), append:

```go
// Attach subscribes the originating client to a process's raw pty byte stream.
// The hub treats this as a sticky stream (same routing as the old Exec). Node
// replies with a one-shot Reply{ok} first (so failures surface as errors),
// then pushes ProcessOutput frames until the process exits (terminal
// ProcessExit) or the client sends Detach.
type Attach struct {
	Type       string `cbor:"type"`
	MsgID      string `cbor:"msg_id"`
	Target     string `cbor:"target,omitempty"`
	ProcessID  string `cbor:"process_id"`
	FromOffset int64  `cbor:"from_offset,omitempty"`
}

// Detach cancels a prior Attach. The process keeps running.
type Detach struct {
	Type      string `cbor:"type"`
	MsgID     string `cbor:"msg_id"`
	Target    string `cbor:"target,omitempty"`
	ProcessID string `cbor:"process_id"`
}
```

4. In the `Node → Hub` section, delete the `ExecOutput` and `ExecExit` struct declarations entirely.

5. After the `Event` struct (still in the Node → Hub section), append:

```go
// ProcessOutput is a chunk of raw pty bytes streamed to an Attach subscriber.
type ProcessOutput struct {
	Type   string `cbor:"type"`
	MsgID  string `cbor:"msg_id"`
	Offset int64  `cbor:"offset"`
	Data   []byte `cbor:"data"`
}

// ProcessExit terminates an Attach stream when the process has exited.
type ProcessExit struct {
	Type  string `cbor:"type"`
	MsgID string `cbor:"msg_id"`
	Code  int    `cbor:"code"`
}
```

6. At the bottom of the file, in the `msgType` block, delete the lines for `Exec`, `ExecCancel`, `ExecOutput`, `ExecExit`, and append:

```go
func (m *Attach) msgType() string        { return "attach" }
func (m *Detach) msgType() string        { return "detach" }
func (m *ProcessOutput) msgType() string { return "process_output" }
func (m *ProcessExit) msgType() string   { return "process_exit" }
```

- [ ] **Step 2: Edit `internal/protocol/codec.go`**

In `setType`'s type-switch: delete the `*Exec`, `*ExecCancel`, `*ExecOutput`, `*ExecExit` cases. Add the same shape cases for `*Attach`, `*Detach`, `*ProcessOutput`, `*ProcessExit`.

In `Decode`'s string switch: delete the `"exec"`, `"exec_cancel"`, `"exec_output"`, `"exec_exit"` cases. Add:

```go
case "attach":
    m = &Attach{}
case "detach":
    m = &Detach{}
case "process_output":
    m = &ProcessOutput{}
case "process_exit":
    m = &ProcessExit{}
```

- [ ] **Step 3: Verify protocol package builds (other packages will still be broken — that's expected)**

Run: `go build ./internal/protocol/...`
Expected: PASS (the protocol package itself is self-contained).

- [ ] **Step 4: Commit**

```bash
git add internal/protocol/messages.go internal/protocol/codec.go
git commit -m "protocol: replace exec frames with attach/detach/process_output/process_exit"
```

---

## Task 2: Process gets a description field and a byte bus

**Files:**
- Modify: `internal/node/process.go`
- Modify: `internal/node/registry.go`
- Create: `internal/node/bytebus.go`
- Create: `internal/node/bytebus_test.go`

- [ ] **Step 1: Write the failing bus test — `internal/node/bytebus_test.go`**

```go
package node

import (
	"testing"
	"time"
)

func TestByteBus_NewSubscriberGetsBacklog(t *testing.T) {
	b := newByteBus()
	b.Write([]byte("hello "))
	b.Write([]byte("world"))

	sub := b.Subscribe(0)
	defer b.Unsubscribe(sub)

	got := drain(t, sub, 11, 200*time.Millisecond)
	if string(got) != "hello world" {
		t.Fatalf("backlog got %q, want %q", got, "hello world")
	}
}

func TestByteBus_LiveAppendDelivered(t *testing.T) {
	b := newByteBus()
	sub := b.Subscribe(0)
	defer b.Unsubscribe(sub)

	b.Write([]byte("ab"))
	b.Write([]byte("cd"))

	got := drain(t, sub, 4, 200*time.Millisecond)
	if string(got) != "abcd" {
		t.Fatalf("live got %q, want %q", got, "abcd")
	}
}

func TestByteBus_FromOffsetSkipsBacklog(t *testing.T) {
	b := newByteBus()
	b.Write([]byte("ignore-this-"))
	sub := b.Subscribe(int64(len("ignore-this-")))
	defer b.Unsubscribe(sub)

	b.Write([]byte("keep"))

	got := drain(t, sub, 4, 200*time.Millisecond)
	if string(got) != "keep" {
		t.Fatalf("offset got %q, want %q", got, "keep")
	}
}

func TestByteBus_CloseEndsSubscribers(t *testing.T) {
	b := newByteBus()
	sub := b.Subscribe(0)
	b.Close()
	select {
	case _, ok := <-sub.Ch():
		if ok {
			// allow drained backlog frames; loop until channel closes
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("subscriber not closed after bus close")
	}
}

func drain(t *testing.T, sub *busSub, n int, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.After(timeout)
	var out []byte
	for len(out) < n {
		select {
		case chunk, ok := <-sub.Ch():
			if !ok {
				return out
			}
			out = append(out, chunk...)
		case <-deadline:
			return out
		}
	}
	return out
}
```

- [ ] **Step 2: Run it — expect compile failure**

Run: `go test ./internal/node/ -run TestByteBus -v`
Expected: build error (`newByteBus`, `busSub` undefined).

- [ ] **Step 3: Implement `internal/node/bytebus.go`**

```go
package node

import "sync"

// byteBus is an append-only byte buffer with live subscribers. Each new write
// appends to buf and is fanned out to every active busSub. New subscribers
// can ask for the existing backlog starting from a given offset; subsequent
// writes are delivered live.
//
// Bus.Close() ends every subscriber's channel; further Writes are no-ops.
// Unsubscribe removes a single subscriber without closing the bus.
type byteBus struct {
	mu     sync.Mutex
	buf    []byte
	subs   map[*busSub]struct{}
	closed bool
}

type busSub struct {
	ch chan []byte
}

func (s *busSub) Ch() <-chan []byte { return s.ch }

func newByteBus() *byteBus {
	return &byteBus{subs: make(map[*busSub]struct{})}
}

// Write appends p to the buffer and fans out a copy to every active subscriber.
// Returns len(p), nil to satisfy io.Writer-shaped callers.
func (b *byteBus) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return len(p), nil
	}
	b.buf = append(b.buf, cp...)
	subs := make([]*busSub, 0, len(b.subs))
	for s := range b.subs {
		subs = append(subs, s)
	}
	b.mu.Unlock()
	for _, s := range subs {
		// Non-blocking-ish: if a slow subscriber backs up we still want to
		// deliver because bytes are append-only and dropping silently would
		// corrupt the agent's view. Use a generous buffer (Subscribe) and
		// accept that a stuck consumer blocks Write here.
		s.ch <- cp
	}
	return len(p), nil
}

// Subscribe returns a subscriber that first receives any buffered bytes from
// fromOffset onward, then receives every subsequent Write.
//
// fromOffset is clamped: negative → 0, beyond buffer end → buffer end.
func (b *byteBus) Subscribe(fromOffset int64) *busSub {
	sub := &busSub{ch: make(chan []byte, 64)}
	b.mu.Lock()
	if fromOffset < 0 {
		fromOffset = 0
	}
	if fromOffset > int64(len(b.buf)) {
		fromOffset = int64(len(b.buf))
	}
	backlog := b.buf[fromOffset:]
	if len(backlog) > 0 {
		cp := make([]byte, len(backlog))
		copy(cp, backlog)
		// Buffered channel sized to absorb a single backlog chunk.
		select {
		case sub.ch <- cp:
		default:
			// Should not happen for a fresh channel; fall through.
			go func() { sub.ch <- cp }()
		}
	}
	if b.closed {
		close(sub.ch)
		b.mu.Unlock()
		return sub
	}
	b.subs[sub] = struct{}{}
	b.mu.Unlock()
	return sub
}

// Unsubscribe removes sub and closes its channel. Safe to call once.
func (b *byteBus) Unsubscribe(sub *busSub) {
	b.mu.Lock()
	if _, ok := b.subs[sub]; ok {
		delete(b.subs, sub)
		close(sub.ch)
	}
	b.mu.Unlock()
}

// Close ends the bus: subscribers' channels are closed, future Writes drop.
func (b *byteBus) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	for s := range b.subs {
		close(s.ch)
	}
	b.subs = nil
	b.mu.Unlock()
}

// Len returns the current buffer length (bytes written so far).
func (b *byteBus) Len() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return int64(len(b.buf))
}
```

- [ ] **Step 4: Run tests, expect PASS**

Run: `go test ./internal/node/ -run TestByteBus -v`
Expected: all four PASS.

- [ ] **Step 5: Edit `internal/node/process.go`**

Replace the existing `Process` struct definition with:

```go
type Process struct {
	ID           string
	Description  string
	Cmd          []string
	Status       string // "running" | "exited"
	StartedAt    time.Time
	LastActiveAt time.Time
	ExitCode     *int
	LogPath      string
	Pid          int

	// Runtime handles; nil after exit.
	mu     sync.Mutex
	stdin  chan<- []byte // nil if not started or already closed
	cancel func()

	vt   vt10x.Terminal
	vtMu sync.Mutex

	// bus is the live raw-byte stream used by Attach subscribers. Always
	// non-nil for a running process; Close()d after exit so subscribers
	// see EOF naturally.
	bus *byteBus
}
```

In the same file delete the `runExecStream` function entirely (it wraps the soon-to-be-deleted PTY exec helper).

- [ ] **Step 6: Edit `internal/node/registry.go`**

In `processSnapshot`, rename field `Name` → `Description`.

In both `List` (the slice-of-Process variant) and `ListSnapshots`, change the `Name: p.Name,` assignment line in `ListSnapshots` to `Description: p.Description,`.

- [ ] **Step 7: Verify node package compiles (other call sites will still be broken — that's expected)**

Run: `go build ./internal/node/...`
Expected: errors referencing `runExecStream`/`runExecStreamPTY`/`handleExec` will remain — that's expected and will be fixed in Task 3. Confirm there are no NEW errors mentioning `byteBus`, `busSub`, or `Description`.

- [ ] **Step 8: Commit**

```bash
git add internal/node/bytebus.go internal/node/bytebus_test.go internal/node/process.go internal/node/registry.go
git commit -m "node: add byteBus for live pty stream subscribers; rename Process.Name to Description"
```

---

## Task 3: Wire byteBus into the pty copy loop; delete runExecStreamPTY

**Files:**
- Modify: `internal/node/pty.go`

- [ ] **Step 1: Edit `internal/node/pty.go`**

Delete the entire `runExecStreamPTY` function (lines `27..84` in current revision, ending at `return code, nil`).

In `startPTY`, after creating the vt (`proc.vt = vt10x.New(vt10x.WithSize(vtCols, vtRows))`), add:

```go
	proc.bus = newByteBus()
```

Replace the PTY → log + VT copy goroutine. Current code:

```go
	var copyDone sync.WaitGroup
	copyDone.Add(1)
	go func() {
		defer copyDone.Done()
		io.Copy(io.MultiWriter(logFile, &vtSink{p: proc}), p)
	}()
```

becomes:

```go
	var copyDone sync.WaitGroup
	copyDone.Add(1)
	go func() {
		defer copyDone.Done()
		io.Copy(io.MultiWriter(logFile, &vtSink{p: proc}, proc.bus), p)
	}()
```

In the exit goroutine, after `copyDone.Wait()` and `p.Close()` (and `logFile.Close()`), close the bus so subscribers receive EOF:

```go
		closeSlave(p)
		copyDone.Wait()
		p.Close()
		logFile.Close()
		proc.bus.Close()
		cancel()
		onExit(code)
```

(Insert `proc.bus.Close()` between `logFile.Close()` and `cancel()`.)

- [ ] **Step 2: Add an attach test — append to `internal/node/pty_test.go`**

(File is small — read it first to find a good insertion point near the existing PTY tests.)

```go
func TestStartPTY_BusReceivesOutputAndClosesOnExit(t *testing.T) {
	dir := t.TempDir()
	p := &Process{ID: "p1", Cmd: []string{"sh", "-c", "printf hello"}}
	exited := make(chan int, 1)
	if err := p.Start(context.Background(), dir, nil, "", func(code int) { exited <- code }); err != nil {
		t.Fatal(err)
	}

	sub := p.bus.Subscribe(0)
	var got []byte
	deadline := time.After(2 * time.Second)
	for {
		select {
		case chunk, ok := <-sub.Ch():
			if !ok {
				if !bytes.Contains(got, []byte("hello")) {
					t.Fatalf("bus output missing 'hello': %q", got)
				}
				<-exited
				return
			}
			got = append(got, chunk...)
		case <-deadline:
			t.Fatalf("timeout, got=%q", got)
		}
	}
}
```

Add the imports if missing: `"bytes"`, `"context"`, `"time"`, `"testing"`.

- [ ] **Step 3: Run the new test (expect FAIL until handler.go fix lands? Actually it will pass — pty.go is now self-consistent)**

Run: `go test ./internal/node/ -run TestStartPTY_BusReceives -v`
Expected: build still failing because `handler.go` references deleted symbols. Skip this run; proceed.

- [ ] **Step 4: Commit**

```bash
git add internal/node/pty.go internal/node/pty_test.go
git commit -m "node: stream pty bytes through byteBus and close it on process exit"
```

---

## Task 4: Node handler — drop exec path, add Attach/Detach

**Files:**
- Modify: `internal/node/handler.go`

- [ ] **Step 1: Edit `internal/node/handler.go`**

a. Remove the `execMu` and `execCancel` fields from `ProcessHandler`. Constructor `NewProcessHandler` no longer initializes them.

b. In `Handle`'s type switch, delete the `*protocol.Exec` and `*protocol.ExecCancel` cases. Add:

```go
	case *protocol.Attach:
		go h.handleAttach(send, m)
	case *protocol.Detach:
		h.handleDetach(m)
```

c. In `handleStart`, change the `Process` initializer to pass description:

```go
	p := &Process{ID: m.ProcessID, Description: m.Description, Cmd: m.Cmd}
```

d. Delete the `handleExec` and `handleExecCancel` functions entirely.

e. Add new handlers (place after `handleStdin`):

```go
// detachReg tracks active Attach subscribers by msg_id so Detach can find them.
// Kept on the ProcessHandler under its own mutex to mirror the prior execMu pattern.

func (h *ProcessHandler) handleAttach(send Sender, m *protocol.Attach) {
	p, ok := h.registry.Get(m.ProcessID)
	if !ok {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "process not found"})
		return
	}
	if p.bus == nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "process has no output stream"})
		return
	}
	sub := p.bus.Subscribe(m.FromOffset)
	h.registerAttach(m.MsgID, p, sub)
	defer h.unregisterAttach(m.MsgID)

	// Initial ok-reply so client knows the subscription is live and any first
	// ProcessOutput is genuine, not a routing artifact.
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true})

	offset := m.FromOffset
	if offset < 0 {
		offset = 0
	}
	for chunk := range sub.Ch() {
		send.Send(&protocol.ProcessOutput{MsgID: m.MsgID, Offset: offset, Data: chunk})
		offset += int64(len(chunk))
	}
	// Bus closed → process exited. Send terminal ProcessExit so the client's
	// stream is unblocked.
	code := 0
	p.mu.Lock()
	if p.ExitCode != nil {
		code = *p.ExitCode
	}
	p.mu.Unlock()
	send.Send(&protocol.ProcessExit{MsgID: m.MsgID, Code: code})
}

func (h *ProcessHandler) handleDetach(m *protocol.Detach) {
	h.mu.Lock()
	rec, ok := h.attachSubs[m.MsgID]
	if ok {
		delete(h.attachSubs, m.MsgID)
	}
	h.mu.Unlock()
	if ok && rec.proc != nil && rec.sub != nil {
		rec.proc.bus.Unsubscribe(rec.sub)
	}
}

type attachRec struct {
	proc *Process
	sub  *busSub
}

func (h *ProcessHandler) registerAttach(msgID string, p *Process, sub *busSub) {
	h.mu.Lock()
	if h.attachSubs == nil {
		h.attachSubs = make(map[string]attachRec)
	}
	h.attachSubs[msgID] = attachRec{proc: p, sub: sub}
	h.mu.Unlock()
}

func (h *ProcessHandler) unregisterAttach(msgID string) {
	h.mu.Lock()
	delete(h.attachSubs, msgID)
	h.mu.Unlock()
}
```

f. Add `attachSubs map[string]attachRec` to the `ProcessHandler` struct. Drop `execMu` and `execCancel` fields and their initialization in `NewProcessHandler`.

g. In `handleList`'s item builder, change `"name": snap.Name,` to `"description": snap.Description,`.

- [ ] **Step 2: Build the node package — expect PASS**

Run: `go build ./internal/node/...`
Expected: PASS.

- [ ] **Step 3: Run node tests — note which fail (test fixtures referencing exec or Name will fail; we'll fix them in Task 8)**

Run: `go test ./internal/node/ -run TestByteBus -v`
Expected: PASS (byteBus tests are self-contained).

Run: `go test ./internal/node/ -run TestStartPTY_BusReceives -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/node/handler.go
git commit -m "node: handle Attach/Detach, drop Exec dispatch path"
```

---

## Task 5: Hub routing — Attach/Detach in client_ws, ProcessOutput/ProcessExit in device_ws

**Files:**
- Modify: `internal/hub/client_ws.go`
- Modify: `internal/hub/device_ws.go`

- [ ] **Step 1: Edit `internal/hub/client_ws.go`**

In `dispatch`'s type switch:
- Delete the `*protocol.Exec` and `*protocol.ExecCancel` cases.
- Add:

```go
	case *protocol.Attach:
		cs.routeStream(m.MsgID, m.Target, raw)
	case *protocol.Detach:
		cs.forwardFireAndForget(m.Target, raw)
```

(Detach is fire-and-forget — the node side just unsubscribes; no reply expected.)

- [ ] **Step 2: Edit `internal/hub/device_ws.go`**

In `msgID()`:
- Delete the `*protocol.ExecOutput` and `*protocol.ExecExit` cases.
- Add cases for `*protocol.ProcessOutput` and `*protocol.ProcessExit` that return `v.MsgID`.

In `run`'s post-`ForwardToClient` switch (around line 232), replace:

```go
	case *protocol.ExecExit:
		s.router.Unregister(id)
```

with:

```go
	case *protocol.ProcessExit:
		s.router.Unregister(id)
```

- [ ] **Step 3: Build the hub package**

Run: `go build ./internal/hub/...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/hub/client_ws.go internal/hub/device_ws.go
git commit -m "hub: route attach/detach + process_output/process_exit; drop exec frames"
```

---

## Task 6: Client RPC delivery + tool rewrites

**Files:**
- Modify: `internal/client/rpc.go`
- Modify: `internal/client/tools_exec.go`

- [ ] **Step 1: Edit `internal/client/rpc.go`**

In `Deliver`'s type switch:
- Delete the `*protocol.ExecOutput` and `*protocol.ExecExit` cases.
- Add:

```go
	case *protocol.ProcessOutput:
		r.mu.Lock()
		ch, ok := r.streams[m.MsgID]
		r.mu.Unlock()
		if ok {
			ch <- m
		}
	case *protocol.ProcessExit:
		r.mu.Lock()
		ch, ok := r.streams[m.MsgID]
		r.mu.Unlock()
		if ok {
			ch <- m
			close(ch)
			r.Unregister(m.MsgID)
		}
```

- [ ] **Step 2: Edit `internal/client/tools_exec.go` — rewrite `exec` and `start_process`**

Replace the entire `exec` tool registration (the block currently bound to `mcp.NewTool("exec", ...)`) with:

```go
	m.AddTool(
		mcp.NewTool("exec",
			mcp.WithDescription("Run a command on a device, wait for it to exit, return merged pty output. If the MCP call is cancelled while the command is still running, returns success with timed_out=true and a process_id so the caller can re-attach or inspect via list_processes/capture_screen/kill_process. The command keeps running on the device in that case."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("cmd", mcp.Required(), mcp.Description("Shell command (passed to sh -c)")),
			mcp.WithString("cwd"),
			mcp.WithObject("env"),
			mcp.WithString("stdin"),
			mcp.WithString("description", mcp.Description("Free-form annotation so you can find this command later via list_processes when timed out.")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			cmd, _ := args["cmd"].(string)
			cwd, _ := args["cwd"].(string)
			stdin, _ := args["stdin"].(string)
			desc, _ := args["description"].(string)
			envMap := extractStringMap(args["env"])

			pid := NewMsgID()

			// 1) Start.
			startID := NewMsgID()
			startCh := c.rpc.Register(startID)
			if err := c.Send(&protocol.Start{
				MsgID: startID, Target: device, ProcessID: pid,
				Cmd: []string{"sh", "-c", cmd}, Cwd: cwd, Env: envMap,
				Description: desc,
			}); err != nil {
				c.rpc.Unregister(startID)
				return mcp.NewToolResultError(err.Error()), nil
			}
			select {
			case reply := <-startCh:
				c.rpc.Unregister(startID)
				if !reply.OK {
					return mcp.NewToolResultError(reply.Error), nil
				}
			case <-ctx.Done():
				c.rpc.Unregister(startID)
				return mcp.NewToolResultError(ctx.Err().Error()), nil
			}

			// 2) Optional initial stdin (fire-and-forget; node accepts after Start).
			if len(stdin) > 0 {
				_ = c.Send(&protocol.Stdin{Target: device, ProcessID: pid, Data: []byte(stdin)})
			}

			// 3) Attach.
			attachID := NewMsgID()
			ch := c.rpc.RegisterStream(attachID)
			defer c.rpc.Unregister(attachID)
			if err := c.Send(&protocol.Attach{MsgID: attachID, Target: device, ProcessID: pid}); err != nil {
				return resultExec(nil, 0, pid, false, err.Error()), nil
			}

			var output []byte
			gotInitialReply := false
			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						// Stream closed without ProcessExit — treat as ended.
						return resultExec(output, 0, pid, false, ""), nil
					}
					switch v := msg.(type) {
					case *protocol.Reply:
						gotInitialReply = true
						if !v.OK {
							return mcp.NewToolResultError(v.Error), nil
						}
					case *protocol.ProcessOutput:
						output = append(output, v.Data...)
					case *protocol.ProcessExit:
						return resultExec(output, v.Code, pid, false, ""), nil
					}
					_ = gotInitialReply
				case <-ctx.Done():
					_ = c.Send(&protocol.Detach{Target: device, ProcessID: pid})
					return resultExec(output, 0, pid, true, ""), nil
				}
			}
		},
	)
```

Replace the `start_process` tool registration with:

```go
	m.AddTool(
		mcp.NewTool("start_process",
			mcp.WithDescription("Start a long-running background process. Returns process_id."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("cmd", mcp.Required()),
			mcp.WithString("cwd"),
			mcp.WithObject("env"),
			mcp.WithString("description", mcp.Description("Free-form annotation for later identification via list_processes.")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			cmdStr, _ := args["cmd"].(string)
			cwd, _ := args["cwd"].(string)
			desc, _ := args["description"].(string)
			envMap := extractStringMap(args["env"])
			pid := NewMsgID()
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.Start{
				MsgID: id, Target: device, ProcessID: pid,
				Cmd: []string{"sh", "-c", cmdStr}, Cwd: cwd, Env: envMap,
				Description: desc,
			}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			select {
			case reply := <-ch:
				if !reply.OK {
					return mcp.NewToolResultError(reply.Error), nil
				}
				b, _ := json.Marshal(map[string]any{"process_id": pid})
				return mcp.NewToolResultText(string(b)), nil
			case <-ctx.Done():
				return mcp.NewToolResultError(ctx.Err().Error()), nil
			}
		},
	)
```

Replace `resultExec` with:

```go
func resultExec(output []byte, code int, processID string, timedOut bool, errStr string) *mcp.CallToolResult {
	payload := map[string]any{
		"output":     string(output),
		"exit_code":  code,
		"process_id": processID,
		"timed_out":  timedOut,
	}
	if errStr != "" {
		payload["error"] = errStr
	}
	b, _ := json.Marshal(payload)
	return mcp.NewToolResultText(string(b))
}
```

Delete the `defaultRPCTimeout` constant if it is no longer referenced (search the file — it's used by `list_devices`, `list_processes`, `capture_screen`, `kill_process`; if so keep it).

Remove the unused `errors` and `time` imports only if unused after edits — verify with the build step.

- [ ] **Step 3: Build the client package**

Run: `go build ./internal/client/...`
Expected: PASS.

- [ ] **Step 4: Build everything**

Run: `go build ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/client/rpc.go internal/client/tools_exec.go
git commit -m "client: rewrite exec as start+attach; add description to exec/start_process; drop timeout/name"
```

---

## Task 7: Update existing node tests for renamed fields and removed exec path

**Files:**
- Modify: `internal/node/exec_test.go`
- Modify: `internal/node/handler_test.go` (only if exec is referenced)
- Modify: `internal/node/registry_test.go` (only if Name is referenced)
- Modify: `internal/node/process_test.go` (only if Name is referenced)

- [ ] **Step 1: Survey the failing tests**

Run: `go test ./internal/node/ 2>&1 | head -80`
Expected: compile errors referencing `protocol.Exec`, `protocol.ExecOutput`, `protocol.ExecExit`, `protocol.ExecCancel`, `runExecStream`, or `Process.Name`.

- [ ] **Step 2: For each test that exercises the old exec path**

Either rewrite it to drive `Start` + `Attach` against `ProcessHandler`, or delete it if the new e2e (Task 9) gives equivalent coverage. The legacy `internal/node/exec_test.go` is the most likely victim — delete the file if all tests are obsolete.

- [ ] **Step 3: Rename references**

Search the test files for `\.Name\b` referring to `Process.Name`. Replace with `.Description`. Replace `name:` keys in test fixtures that build a `Process` literal.

- [ ] **Step 4: Build + run**

Run: `go test ./internal/node/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/node/
git commit -m "node: update tests for Process.Description and removed exec path"
```

---

## Task 8: Update client tests

**Files:**
- Modify: `internal/client/tools_exec_test.go`

- [ ] **Step 1: Survey**

Run: `go test ./internal/client/ 2>&1 | head -80`
Expected: compile or test failures referencing `protocol.Exec*`, the old `exec` return shape (`stdout`/`stderr` keys), or the removed `timeout` parameter.

- [ ] **Step 2: Rewrite assertions**

For tests that previously asserted on `stdout` / `stderr` separately, switch to `output` (single string field). For tests that exercised `exec` timeout behavior, rewrite to drive cancellation via `ctx` and assert `timed_out: true` + presence of `process_id`. For tests that asserted on the `name` field of `start_process`, rename to `description`.

- [ ] **Step 3: Run**

Run: `go test ./internal/client/...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/client/
git commit -m "client: update tests for unified exec/start_process shape"
```

---

## Task 9: E2E — exec round-trip + timeout-recovery via attach

**Files:**
- Modify: `e2e_test.go`

- [ ] **Step 1: Survey**

Run: `grep -n 'Exec\|exec_output\|exec_exit\|timeout' e2e_test.go | head -40`

Identify the existing exec test (single-shot success path).

- [ ] **Step 2: Rewrite the success-path test**

Test should:
1. Bring up hub + node + client (existing helper).
2. Call MCP `exec` with `device, cmd=echo hi, description="greet"`.
3. Assert the returned JSON has `output` containing `"hi"`, `exit_code=0`, `timed_out=false`, a non-empty `process_id`.
4. Call MCP `list_processes`. Assert the returned list contains an entry with `description="greet"`, `status="exited"`, `process_id` matching.

- [ ] **Step 3: Add a timeout-recovery test**

Test should:
1. Use a `context.WithTimeout(ctx, 500*time.Millisecond)` when invoking the MCP `exec` tool.
2. `cmd=sleep 5, description="stuck"`.
3. Assert ctx-cancel path returns `timed_out=true`, non-empty `process_id`, no error result.
4. Call `list_processes` and assert there is an entry with `status="running"` and matching `process_id`.
5. Call `capture_screen` with that `process_id`; assert it returns successfully (lines may be empty for `sleep`, but `total_lines` ≥ 0).
6. Call `kill_process` with that `process_id`; assert success.
7. Wait briefly (≤2s); call `list_processes` again; assert the entry's `status == "exited"`.

Place the test alongside the existing exec test, following the same fixture / helper style.

- [ ] **Step 4: Run the e2e suite**

Run: `go test ./... -run E2E -count=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add e2e_test.go
git commit -m "e2e: cover unified exec success and ctx-cancel recovery via list_processes"
```

---

## Task 10: Final verification

- [ ] **Step 1: Full build**

Run: `go build ./...`
Expected: PASS.

- [ ] **Step 2: Full test suite with race detector**

Run: `go test -race ./...`
Expected: PASS, no data races.

- [ ] **Step 3: Sanity check the binary**

Run: `go run . hub --help` (or however the binary is normally invoked — check `Makefile`).
Expected: no panic on startup; help text appears.

- [ ] **Step 4: Confirm no orphan references**

Run: `grep -rn 'ExecOutput\|ExecExit\|ExecCancel\|protocol.Exec\b\|runExecStream\|execMu\|execCancel\|Process.Name\b\|"name"' --include="*.go" .`
Expected: only legitimate hits (e.g. `Process.Name` may appear in unrelated structs; `"name"` will appear in MCP tool definitions like `list_devices`). Any reference to the old exec protocol is a bug.

- [ ] **Step 5: Confirm and ship**

No commit; this task is just verification. If everything passes, the branch is ready for review.
