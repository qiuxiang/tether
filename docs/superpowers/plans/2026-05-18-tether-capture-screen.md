# Capture Screen Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace tether's raw-byte `get_output` with a tmux-style `capture_screen` MCP tool that returns rendered terminal state (ANSI sequences resolved, CR overwrites applied, colors stripped). Historical raw bytes remain accessible through the existing log file via `file_transfer`.

**Architecture:** Each `Process` owns a fixed-size `vt10x.Terminal` (200 cols × 10000 rows). PTY and pipe writes are tee'd into the VT via `io.MultiWriter`. A new `CaptureScreen` method walks the VT cells under a mutex and returns plain-text lines plus cursor and total-line counts. `GetOutput` (tool, protocol message, handler, `Process.ReadOutput`) is removed; `list_processes` exposes the on-disk log path so callers can retrieve full byte history via `file_transfer`.

**Tech Stack:** Go, `github.com/hinshun/vt10x` (terminal emulator), existing tether protocol/codec, existing MCP client framework in `internal/client`.

**Spec:** `docs/superpowers/specs/2026-05-18-tether-capture-screen-design.md`

---

## File Map

**Create:**
- `internal/node/vt.go` — `vtSink` writer adapter + `Process.CaptureScreen` method
- `internal/node/vt_test.go` — unit tests for VT rendering + capture
- `internal/client/tools_capture.go` — MCP `capture_screen` tool

**Modify:**
- `go.mod`, `go.sum` — add `vt10x`
- `internal/node/process.go` — `Process` gains `vt`/`vtMu` fields; `Start` constructs VT; remove `ReadOutput`
- `internal/node/pty.go` — wrap PTY copy with `io.MultiWriter`
- `internal/node/handler.go` — add `handleCaptureScreen`, remove `handleGetOutput`, include `log_path` in `handleList`
- `internal/node/handler_test.go` — replace `GetOutput` tests with `CaptureScreen` tests; add `log_path` assertion
- `internal/node/process_test.go` — drop `ReadOutput` cases
- `internal/protocol/messages.go` — add `CaptureScreen`, remove `GetOutput`
- `internal/protocol/codec.go` — register `CaptureScreen`, deregister `GetOutput`
- `internal/protocol/codec_test.go` — codec round-trip
- `internal/client/tools_exec.go` — remove `get_output` tool registration (lines around 227–260)
- `cmd/probe/main.go` — switch smoke-test calls from `get_output` to `capture_screen`
- `e2e_test.go` — replace `get_output` flow with `capture_screen`
- `README.md` (or `docs/`) — document new tool and historical-log workflow

---

## Task 1: Add vt10x dependency

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Add dependency**

```bash
go get github.com/hinshun/vt10x@latest
go mod tidy
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./...
```
Expected: build succeeds.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "deps: add hinshun/vt10x for terminal rendering"
```

---

## Task 2: vtSink adapter + CaptureScreen method (no plumbing yet)

Build the rendering core in isolation, fed directly via `vt.Write` in tests. Plumbing into Process.Start comes in later tasks.

**Files:**
- Create: `internal/node/vt.go`
- Create: `internal/node/vt_test.go`
- Modify: `internal/node/process.go` (add fields only)

- [ ] **Step 1: Write the failing test** — `internal/node/vt_test.go`

```go
package node

import (
	"strings"
	"testing"

	"github.com/hinshun/vt10x"
)

func newTestProcess() *Process {
	p := &Process{ID: "test"}
	p.vt = vt10x.New(vt10x.WithSize(vtCols, vtRows))
	return p
}

func TestCaptureScreen_PlainText(t *testing.T) {
	p := newTestProcess()
	p.vt.Write([]byte("hello\r\nworld\r\n"))

	lines, _, _, total := p.CaptureScreen(nil, nil)
	if total != 2 {
		t.Fatalf("total_lines: got %d, want 2", total)
	}
	if len(lines) != 2 || lines[0] != "hello" || lines[1] != "world" {
		t.Fatalf("lines: got %q", lines)
	}
}

func TestCaptureScreen_StripsANSIColor(t *testing.T) {
	p := newTestProcess()
	p.vt.Write([]byte("\x1b[31mred\x1b[0m\r\n"))

	lines, _, _, _ := p.CaptureScreen(nil, nil)
	if len(lines) != 1 || lines[0] != "red" {
		t.Fatalf("got %q", lines)
	}
	if strings.ContainsAny(lines[0], "\x1b[") {
		t.Fatalf("escape leaked: %q", lines[0])
	}
}

func TestCaptureScreen_CarriageReturnOverwrite(t *testing.T) {
	p := newTestProcess()
	p.vt.Write([]byte("foo\rbar\r\n"))

	lines, _, _, _ := p.CaptureScreen(nil, nil)
	if len(lines) != 1 || lines[0] != "bar" {
		t.Fatalf("got %q, want [bar]", lines)
	}
}

func TestCaptureScreen_LineRange(t *testing.T) {
	p := newTestProcess()
	var b strings.Builder
	for i := 0; i < 100; i++ {
		b.WriteString("line")
		b.WriteString(itoa(i))
		b.WriteString("\r\n")
	}
	p.vt.Write([]byte(b.String()))

	last10Start, last10End := -10, -1
	lines, _, _, total := p.CaptureScreen(&last10Start, &last10End)
	if total != 100 {
		t.Fatalf("total: %d", total)
	}
	if len(lines) != 10 || lines[0] != "line90" || lines[9] != "line99" {
		t.Fatalf("got %q", lines)
	}

	mid1, mid2 := 50, 59
	lines, _, _, _ = p.CaptureScreen(&mid1, &mid2)
	if len(lines) != 10 || lines[0] != "line50" || lines[9] != "line59" {
		t.Fatalf("got %q", lines)
	}

	oob := -9999
	lines, _, _, _ = p.CaptureScreen(&oob, nil)
	if len(lines) != 100 || lines[0] != "line0" {
		t.Fatalf("clamp failed: len=%d first=%q", len(lines), lines[0])
	}
}

func TestCaptureScreen_Empty(t *testing.T) {
	p := newTestProcess()
	lines, row, col, total := p.CaptureScreen(nil, nil)
	if len(lines) != 0 || total != 0 || row != 0 || col != 0 {
		t.Fatalf("empty: lines=%q total=%d row=%d col=%d", lines, total, row, col)
	}
}

func TestCaptureScreen_ClearScreen(t *testing.T) {
	p := newTestProcess()
	p.vt.Write([]byte("line1\r\nline2\r\n\x1b[2J\x1b[H"))

	lines, _, _, total := p.CaptureScreen(nil, nil)
	for _, l := range lines {
		if strings.TrimSpace(l) != "" {
			t.Fatalf("after clear: got nonempty %q (total=%d)", l, total)
		}
	}
}

// itoa avoids importing strconv just for tests.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
```

- [ ] **Step 2: Run tests — expect compile failure**

```bash
go test ./internal/node/ -run TestCaptureScreen
```
Expected: build fails because `vtCols`, `vtRows`, `Process.vt`, and `Process.CaptureScreen` are not defined.

- [ ] **Step 3: Add VT fields to Process** — edit `internal/node/process.go`

Find the `Process` struct definition. Add fields:

```go
import (
	// ... existing ...
	"github.com/hinshun/vt10x"
)

type Process struct {
	// ... existing fields ...

	vt   vt10x.Terminal
	vtMu sync.Mutex
}
```

- [ ] **Step 4: Create `internal/node/vt.go`**

```go
package node

import (
	"io"
	"strings"
	"unicode"

	"github.com/hinshun/vt10x"
)

const (
	vtCols = 200
	vtRows = 10000
)

// vtSink is an io.Writer that forwards bytes into a Process's VT under vtMu.
// Used inside io.MultiWriter so PTY/pipe copy loops stay simple.
type vtSink struct {
	p *Process
}

func (s *vtSink) Write(b []byte) (int, error) {
	s.p.vtMu.Lock()
	defer s.p.vtMu.Unlock()
	if s.p.vt == nil {
		return len(b), nil
	}
	return s.p.vt.Write(b)
}

// CaptureScreen returns rendered lines in [startLine, endLine] (inclusive),
// tmux-style: negative indices count from the end, nil means "extreme"
// (top for start, bottom for end). Out-of-range values are clamped. Colors
// and display attributes are stripped; trailing whitespace per line is trimmed.
//
// `total` is the highest line index that has received any content plus 1.
// `cursorRow` and `cursorCol` are the VT cursor position (cursorRow is the
// absolute row index inside the VT, same coordinate space as start/end).
func (p *Process) CaptureScreen(startLine, endLine *int) (lines []string, cursorRow, cursorCol, total int) {
	p.vtMu.Lock()
	defer p.vtMu.Unlock()
	if p.vt == nil {
		return nil, 0, 0, 0
	}

	cols, rows := p.vt.Size()
	total = highestNonEmptyRow(p.vt, cols, rows) + 1
	// Special case: if cursor sits below the last non-empty row, count it.
	cur := p.vt.Cursor()
	if cur.Y+1 > total {
		total = cur.Y + 1
	}
	if total < 0 {
		total = 0
	}

	start, end := resolveRange(startLine, endLine, total)
	if start > end || total == 0 {
		return []string{}, cur.Y, cur.X, total
	}

	lines = make([]string, 0, end-start+1)
	for y := start; y <= end; y++ {
		lines = append(lines, renderLine(p.vt, cols, y))
	}
	return lines, cur.Y, cur.X, total
}

// resolveRange converts tmux-style indices (nil/negative) into [0, total)
// inclusive [start, end]. Returns start > end when the range is empty.
func resolveRange(startLine, endLine *int, total int) (int, int) {
	start := 0
	if startLine != nil {
		start = *startLine
		if start < 0 {
			start = total + start
		}
	}
	end := total - 1
	if endLine != nil {
		end = *endLine
		if end < 0 {
			end = total + end
		}
	}
	if start < 0 {
		start = 0
	}
	if end > total-1 {
		end = total - 1
	}
	return start, end
}

func renderLine(vt vt10x.Terminal, cols, row int) string {
	var b strings.Builder
	b.Grow(cols)
	for x := 0; x < cols; x++ {
		g := vt.Cell(x, row)
		if g.Char == 0 {
			b.WriteRune(' ')
		} else {
			b.WriteRune(g.Char)
		}
	}
	return strings.TrimRightFunc(b.String(), unicode.IsSpace)
}

func highestNonEmptyRow(vt vt10x.Terminal, cols, rows int) int {
	for y := rows - 1; y >= 0; y-- {
		for x := 0; x < cols; x++ {
			g := vt.Cell(x, y)
			if g.Char != 0 && g.Char != ' ' {
				return y
			}
		}
	}
	return -1
}

// Compile-time guard so a refactor doesn't accidentally break the io.Writer
// contract relied on by io.MultiWriter.
var _ io.Writer = (*vtSink)(nil)
```

- [ ] **Step 5: Run tests — verify pass**

```bash
go test ./internal/node/ -run TestCaptureScreen -v
```
Expected: all `TestCaptureScreen_*` PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/node/vt.go internal/node/vt_test.go internal/node/process.go
git commit -m "node: vtSink + CaptureScreen rendering core"
```

---

## Task 3: Construct VT in Process.Start (non-PTY path)

**Files:**
- Modify: `internal/node/process.go`

Find `Process.Start` (around line 50–70 — handles the non-PTY branch). The pipe path sets `cmd.Stdout = logFile; cmd.Stderr = logFile`. We change it so writes go to both the log file and the VT, and we construct the VT before either is wired.

- [ ] **Step 1: Write the failing test** — append to `internal/node/process_test.go`

```go
func TestStart_NonPTY_FeedsVT(t *testing.T) {
	dir := t.TempDir()
	p := &Process{ID: "vt-pipe", Cmd: []string{"sh", "-c", "printf 'hello\\nworld\\n'"}}
	done := make(chan struct{})
	if err := p.Start(context.Background(), dir, nil, "", false, func(code int) { close(done) }); err != nil {
		t.Fatal(err)
	}
	<-done

	lines, _, _, total := p.CaptureScreen(nil, nil)
	if total != 2 || len(lines) != 2 || lines[0] != "hello" || lines[1] != "world" {
		t.Fatalf("got lines=%q total=%d", lines, total)
	}
}
```

- [ ] **Step 2: Run — expect failure**

```bash
go test ./internal/node/ -run TestStart_NonPTY_FeedsVT
```
Expected: FAIL (VT is nil; `CaptureScreen` returns empty).

- [ ] **Step 3: Construct VT and wire MultiWriter in non-PTY path** — edit `Process.Start` in `internal/node/process.go`. Find the section that sets `cmd.Stdout = logFile; cmd.Stderr = logFile`. Replace with:

```go
p.vt = vt10x.New(vt10x.WithSize(vtCols, vtRows))
sink := &vtSink{p: p}
w := io.MultiWriter(logFile, sink)
cmd.Stdout = w
cmd.Stderr = w
```

Add `"io"` to imports if not already present.

- [ ] **Step 4: Run — verify pass**

```bash
go test ./internal/node/ -run TestStart_NonPTY_FeedsVT -v
```
Expected: PASS.

- [ ] **Step 5: Run full node tests**

```bash
go test ./internal/node/ -v
```
Expected: all pre-existing tests still PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/node/process.go internal/node/process_test.go
git commit -m "node: feed pipe output into VT alongside log file"
```

---

## Task 4: Wire VT into PTY path

**Files:**
- Modify: `internal/node/pty.go`

`startPTY` currently does `io.Copy(logFile, p)` on line ~138. Change to write through both. Also construct VT before launching command.

- [ ] **Step 1: Write the failing test** — append to `internal/node/pty_test.go`

```go
func TestStartPTY_FeedsVT(t *testing.T) {
	dir := t.TempDir()
	p := &Process{ID: "vt-pty", Cmd: []string{"sh", "-c", "printf 'foo\\rbar\\n'"}}
	done := make(chan struct{})
	if err := p.Start(context.Background(), dir, nil, "", true, func(code int) { close(done) }); err != nil {
		t.Fatal(err)
	}
	<-done

	lines, _, _, total := p.CaptureScreen(nil, nil)
	if total != 1 || len(lines) != 1 || lines[0] != "bar" {
		t.Fatalf("got lines=%q total=%d (CR overwrite should leave 'bar')", lines, total)
	}
}
```

- [ ] **Step 2: Run — expect failure**

```bash
go test ./internal/node/ -run TestStartPTY_FeedsVT
```
Expected: FAIL.

- [ ] **Step 3: Construct VT and wire MultiWriter in PTY path** — edit `internal/node/pty.go`. In `startPTY`, before the `c.Start()` call, add VT construction:

```go
proc.vt = vt10x.New(vt10x.WithSize(vtCols, vtRows))
```

Then find the copy goroutine (`go func() { defer copyDone.Done(); io.Copy(logFile, p) }()`) and change `io.Copy(logFile, p)` to:

```go
io.Copy(io.MultiWriter(logFile, &vtSink{p: proc}), p)
```

Add the `vt10x` import if not already present.

- [ ] **Step 4: Run — verify pass**

```bash
go test ./internal/node/ -run TestStartPTY_FeedsVT -v
```
Expected: PASS.

- [ ] **Step 5: Race check**

```bash
go test ./internal/node/ -race
```
Expected: no race detected.

- [ ] **Step 6: Commit**

```bash
git add internal/node/pty.go internal/node/pty_test.go
git commit -m "node: feed PTY output into VT alongside log file"
```

---

## Task 5: CaptureScreen protocol message + codec

**Files:**
- Modify: `internal/protocol/messages.go`
- Modify: `internal/protocol/codec.go`
- Modify: `internal/protocol/codec_test.go`

Mirror the structure of existing messages (e.g. `GetOutput` — copy its shape).

- [ ] **Step 1: Find GetOutput's definition** — for reference

```bash
grep -n "GetOutput" internal/protocol/messages.go internal/protocol/codec.go
```

- [ ] **Step 2: Write the failing codec test** — append to `internal/protocol/codec_test.go`

```go
func TestCodec_CaptureScreen_RoundTrip(t *testing.T) {
	start, end := -10, -1
	msgs := []*CaptureScreen{
		{MsgID: "m1", ProcessID: "p1", StartLine: &start, EndLine: &end},
		{MsgID: "m2", ProcessID: "p2", StartLine: nil, EndLine: nil},
	}
	for _, m := range msgs {
		raw, err := Encode(m)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, err := Decode(raw)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		got, ok := decoded.(*CaptureScreen)
		if !ok {
			t.Fatalf("decoded type: %T", decoded)
		}
		if got.MsgID != m.MsgID || got.ProcessID != m.ProcessID {
			t.Fatalf("mismatch: %+v vs %+v", got, m)
		}
		if (got.StartLine == nil) != (m.StartLine == nil) || (got.EndLine == nil) != (m.EndLine == nil) {
			t.Fatalf("nil mismatch")
		}
		if m.StartLine != nil && *got.StartLine != *m.StartLine {
			t.Fatalf("StartLine: %d vs %d", *got.StartLine, *m.StartLine)
		}
		if m.EndLine != nil && *got.EndLine != *m.EndLine {
			t.Fatalf("EndLine: %d vs %d", *got.EndLine, *m.EndLine)
		}
	}
}
```

- [ ] **Step 3: Run — expect failure**

```bash
go test ./internal/protocol/ -run TestCodec_CaptureScreen
```
Expected: FAIL (`CaptureScreen` type missing).

- [ ] **Step 4: Add type in `internal/protocol/messages.go`** — alongside other message types:

```go
// CaptureScreen requests the rendered terminal screen of a process.
// StartLine/EndLine use tmux semantics: negative indices count from the end,
// nil means "extreme" (start = top of scrollback, end = current last line).
type CaptureScreen struct {
	MsgID     string `json:"msg_id"`
	Target    string `json:"target"`
	ProcessID string `json:"process_id"`
	StartLine *int   `json:"start_line,omitempty"`
	EndLine   *int   `json:"end_line,omitempty"`
}
```

Read the `GetOutput` definition first (it has Target, MsgID, ProcessID) and copy its exact method set (`GetMsgID`/`Kind`/`GetTarget` or whatever the codebase uses) onto `CaptureScreen`. If `GetOutput` has additional interface methods you don't see here, add the same on `CaptureScreen`.

- [ ] **Step 5: Register in `internal/protocol/codec.go`** — find the registration table where other message types are decoded (e.g. switch on Kind). Add a case for `"capture_screen"` that decodes into `*CaptureScreen`.

- [ ] **Step 6: Run — verify pass**

```bash
go test ./internal/protocol/ -v
```
Expected: PASS, including pre-existing tests.

- [ ] **Step 7: Commit**

```bash
git add internal/protocol/
git commit -m "protocol: add CaptureScreen message"
```

---

## Task 6: Node handler for CaptureScreen

**Files:**
- Modify: `internal/node/handler.go`
- Modify: `internal/node/handler_test.go` (create if missing)

- [ ] **Step 1: Check for an existing handler_test.go**

```bash
ls internal/node/handler_test.go 2>/dev/null || echo "missing"
```

- [ ] **Step 2: Write the failing test** — `internal/node/handler_test.go` (create or append)

```go
package node

import (
	"context"
	"testing"

	"github.com/qiuxiang/tether/internal/protocol"
)

type captureSender struct{ last protocol.Message }

func (s *captureSender) Send(m protocol.Message) { s.last = m }

func TestHandleCaptureScreen_NotFound(t *testing.T) {
	h := NewProcessHandler(t.TempDir(), 16)
	s := &captureSender{}
	h.Handle(context.Background(), s, &protocol.CaptureScreen{MsgID: "x", ProcessID: "missing"})
	r, ok := s.last.(*protocol.Reply)
	if !ok || r.OK {
		t.Fatalf("expected OK=false reply, got %+v", s.last)
	}
}

func TestHandleCaptureScreen_HappyPath(t *testing.T) {
	dir := t.TempDir()
	h := NewProcessHandler(dir, 16)
	p := &Process{ID: "ok", Cmd: []string{"sh", "-c", "printf 'a\\nb\\n'"}}
	done := make(chan struct{})
	if err := p.Start(context.Background(), dir, nil, "", false, func(int) { close(done) }); err != nil {
		t.Fatal(err)
	}
	h.registry.Add(p)
	<-done

	s := &captureSender{}
	h.Handle(context.Background(), s, &protocol.CaptureScreen{MsgID: "m", ProcessID: "ok"})
	r, ok := s.last.(*protocol.Reply)
	if !ok || !r.OK {
		t.Fatalf("expected OK reply, got %+v", s.last)
	}
	lines, _ := r.Data["lines"].([]string)
	if len(lines) != 2 || lines[0] != "a" || lines[1] != "b" {
		t.Fatalf("lines=%q", lines)
	}
	if _, ok := r.Data["cursor"]; !ok {
		t.Fatalf("missing cursor in Data: %+v", r.Data)
	}
	if cols, _ := r.Data["cols"].(int); cols != vtCols {
		t.Fatalf("cols=%v", r.Data["cols"])
	}
}
```

- [ ] **Step 3: Run — expect failure**

```bash
go test ./internal/node/ -run TestHandleCaptureScreen
```
Expected: FAIL (handler not implemented).

- [ ] **Step 4: Implement handler** — edit `internal/node/handler.go`. In the `Handle` switch, add the case (alongside existing ones):

```go
case *protocol.CaptureScreen:
    h.handleCaptureScreen(send, m)
```

Add the method (paste below `handleGetOutput` for now — it gets deleted in Task 9):

```go
func (h *ProcessHandler) handleCaptureScreen(send Sender, m *protocol.CaptureScreen) {
	p, ok := h.registry.Get(m.ProcessID)
	if !ok {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "process not found"})
		return
	}
	lines, row, col, total := p.CaptureScreen(m.StartLine, m.EndLine)
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"lines":       lines,
		"cursor":      map[string]any{"row": row, "col": col},
		"cols":        vtCols,
		"total_lines": total,
	}})
}
```

- [ ] **Step 5: Run — verify pass**

```bash
go test ./internal/node/ -run TestHandleCaptureScreen -v
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/node/handler.go internal/node/handler_test.go
git commit -m "node: handle CaptureScreen requests"
```

---

## Task 7: Include log_path in list_processes

**Files:**
- Modify: `internal/node/handler.go` (function `handleList`)
- Modify: `internal/node/handler_test.go` or `internal/node/registry_test.go`

- [ ] **Step 1: Write the failing test** — append to `internal/node/handler_test.go`

```go
func TestHandleList_IncludesLogPath(t *testing.T) {
	dir := t.TempDir()
	h := NewProcessHandler(dir, 16)
	p := &Process{ID: "lp", Cmd: []string{"true"}}
	done := make(chan struct{})
	if err := p.Start(context.Background(), dir, nil, "", false, func(int) { close(done) }); err != nil {
		t.Fatal(err)
	}
	h.registry.Add(p)
	<-done

	s := &captureSender{}
	h.Handle(context.Background(), s, &protocol.List{MsgID: "m", Limit: 10})
	r := s.last.(*protocol.Reply)
	items := r.Data["processes"].([]map[string]any)
	if len(items) != 1 {
		t.Fatalf("items=%d", len(items))
	}
	lp, ok := items[0]["log_path"].(string)
	if !ok || lp == "" {
		t.Fatalf("log_path missing or empty: %+v", items[0])
	}
	if lp != p.LogPath {
		t.Fatalf("log_path %q != Process.LogPath %q", lp, p.LogPath)
	}
}
```

- [ ] **Step 2: Run — expect failure**

```bash
go test ./internal/node/ -run TestHandleList_IncludesLogPath
```
Expected: FAIL.

- [ ] **Step 3: Modify `handleList`** in `internal/node/handler.go`. Inside the `for _, snap := range list` loop, add `"log_path": snap.LogPath,` to the `entry` map. If `snap` doesn't have `LogPath` yet, also extend `ListSnapshots` and the snapshot struct in `internal/node/registry.go` to carry it (read `p.LogPath` under `p.mu`).

- [ ] **Step 4: Run — verify pass**

```bash
go test ./internal/node/ -run TestHandleList -v
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/node/handler.go internal/node/registry.go internal/node/handler_test.go
git commit -m "node: surface log_path in list response"
```

---

## Task 8: MCP client tool capture_screen

**Files:**
- Create: `internal/client/tools_capture.go`

Pattern after the existing `get_output` registration in `internal/client/tools_exec.go` (around lines 227–260). Don't remove `get_output` yet — that's Task 9.

- [ ] **Step 1: Inspect the existing pattern**

```bash
sed -n '220,265p' internal/client/tools_exec.go
```

- [ ] **Step 2: Create `internal/client/tools_capture.go`**

```go
package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/qiuxiang/tether/internal/protocol"
)

// registerCaptureScreen adds the capture_screen MCP tool to the server.
// Pattern mirrors get_output in tools_exec.go but with line-range parameters
// and a structured (non-base64) response.
func (c *Client) registerCaptureScreen(srv *mcp.Server) {
	tool := mcp.NewTool("capture_screen",
		mcp.WithDescription("Return the rendered terminal screen of a process (ANSI sequences resolved, colors stripped). Tmux-style line ranges. Use this for live state. For full historical bytes beyond scrollback (10000 lines), use list_processes + file_transfer on log_path."),
		mcp.WithString("device", mcp.Required(), mcp.Description("Target device hostname.")),
		mcp.WithString("process_id", mcp.Required(), mcp.Description("Process ID returned by start_process.")),
		mcp.WithNumber("start_line", mcp.Description("Inclusive start line. Negative counts from end. Omitted = top of scrollback.")),
		mcp.WithNumber("end_line", mcp.Description("Inclusive end line. Negative counts from end. Omitted = current last line.")),
	)

	srv.AddTool(tool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		device, _ := req.Params.Arguments["device"].(string)
		pid, _ := req.Params.Arguments["process_id"].(string)
		if device == "" || pid == "" {
			return mcp.NewToolResultError("device and process_id are required"), nil
		}

		msg := &protocol.CaptureScreen{
			MsgID:     newMsgID(),
			Target:    device,
			ProcessID: pid,
		}
		if v, ok := req.Params.Arguments["start_line"].(float64); ok {
			n := int(v)
			msg.StartLine = &n
		}
		if v, ok := req.Params.Arguments["end_line"].(float64); ok {
			n := int(v)
			msg.EndLine = &n
		}

		reply, err := c.Call(ctx, msg)
		if err != nil {
			return nil, fmt.Errorf("capture_screen: %w", err)
		}
		if !reply.OK {
			return mcp.NewToolResultError(reply.Error), nil
		}
		out, _ := json.Marshal(reply.Data)
		return mcp.NewToolResultText(string(out)), nil
	})
}
```

NOTE: if the existing client uses a different helper name than `newMsgID` / `c.Call`, or routes messages via a different mechanism (e.g. `c.Send` + waiting on a reply channel), match that pattern by reading `tools_exec.go` first.

`CaptureScreen.Target` was already added in Task 5; pass it through here.

- [ ] **Step 3: Register the tool** — find where other tools are registered (likely in `internal/client/server.go` or wherever `registerExec`, `registerGetOutput`, etc. are called). Add:

```go
c.registerCaptureScreen(srv)
```

- [ ] **Step 4: Build**

```bash
go build ./...
```
Expected: success.

- [ ] **Step 5: Manual smoke test** — start hub+node locally and invoke via MCP probe:

```bash
go run ./cmd/probe -tool capture_screen -device <node> -process_id <pid>
```

(Use whatever invocation the probe supports; if `cmd/probe/main.go` doesn't support arbitrary tools, skip this step — Task 10 e2e covers it.)

- [ ] **Step 6: Commit**

```bash
git add internal/client/tools_capture.go internal/client/  # plus any registration file modified
git commit -m "client: register capture_screen MCP tool"
```

---

## Task 9: Remove get_output

Now that `capture_screen` works, delete the old path.

**Files:**
- Modify: `internal/protocol/messages.go` — remove `GetOutput` type
- Modify: `internal/protocol/codec.go` — deregister
- Modify: `internal/protocol/codec_test.go` — drop GetOutput round-trip test if present
- Modify: `internal/node/handler.go` — remove `case *protocol.GetOutput:` and `handleGetOutput`
- Modify: `internal/node/process.go` — remove `ReadOutput`
- Modify: `internal/node/process_test.go` — remove tests calling `ReadOutput`
- Modify: `internal/client/tools_exec.go` — remove the `get_output` block (lines around 227–260)
- Modify: `cmd/probe/main.go` — switch lines 81 and 86 to `capture_screen`

- [ ] **Step 1: Verify all call sites are known**

```bash
grep -rn "GetOutput\|get_output\|ReadOutput" --include='*.go'
```
Cross-check against the file list above.

- [ ] **Step 2: Delete the protocol type** — in `internal/protocol/messages.go` remove the `GetOutput` struct and its methods.

- [ ] **Step 3: Deregister from codec** — in `internal/protocol/codec.go` remove the `"get_output"` switch case (or registration entry).

- [ ] **Step 4: Delete handler bits** — in `internal/node/handler.go` remove the `case *protocol.GetOutput:` switch arm and the `handleGetOutput` function.

- [ ] **Step 5: Delete `Process.ReadOutput`** — in `internal/node/process.go` remove the method (around lines 227–259) and its `io`/`os`/`fmt` imports if newly unused.

- [ ] **Step 6: Update probe** — `cmd/probe/main.go` lines 81 and 86:

```go
call(ctx, c, "capture_screen", map[string]any{"device": "mac", "process_id": pid})
// ... and again at line 86
call(ctx, c, "capture_screen", map[string]any{"device": "mac", "process_id": pid})
```

- [ ] **Step 7: Remove client tool registration** — delete the block in `internal/client/tools_exec.go` around lines 227–260 that registers `get_output`.

- [ ] **Step 8: Remove or migrate tests** — delete `process_test.go` cases that called `ReadOutput`; delete codec tests for `GetOutput` if any.

- [ ] **Step 9: Build and run all tests**

```bash
go build ./...
go test ./...
```
Expected: build clean, all tests pass.

- [ ] **Step 10: Commit**

```bash
# IMPORTANT: do NOT use `git add -A`. There are untracked local files
# (.mcp.json, cmd/probe/main.go, config-hub.yaml, tether-darwin-arm64)
# that the user has intentionally kept out of version control.
# Stage only the modified tracked files:
git add internal/protocol/messages.go internal/protocol/codec.go \
        internal/protocol/codec_test.go \
        internal/node/handler.go internal/node/process.go \
        internal/node/process_test.go \
        internal/client/tools_exec.go
# Modify cmd/probe/main.go locally for the implementer's own smoke testing
# but DO NOT git add it — it is untracked and must stay that way.
git commit -m "node,protocol,client: remove get_output in favor of capture_screen"
```

---

## Task 10: End-to-end test

**Files:**
- Modify: `e2e_test.go`

- [ ] **Step 1: Inspect the existing e2e test** — find a test that previously exercised `get_output` and use it as a template:

```bash
grep -n "get_output\|GetOutput\|capture_screen" e2e_test.go
```

If none, look for a representative e2e using `start_process` + reply assertions. Pattern after that.

- [ ] **Step 2: Write the e2e test** — append to `e2e_test.go` (adapt to the file's existing helper functions):

```go
func TestE2E_CaptureScreen(t *testing.T) {
	hub, node, client := startTestStack(t) // existing helper — name may differ
	defer hub.Close()

	pid := startEcho(t, client, node.Hostname, "printf 'foo\\nbar\\n'") // existing helper or inline
	waitExit(t, client, node.Hostname, pid)

	reply := callTool(t, client, "capture_screen", map[string]any{
		"device":     node.Hostname,
		"process_id": pid,
	})
	if !reply.OK {
		t.Fatalf("reply: %+v", reply)
	}
	lines := reply.Data["lines"].([]string)
	if len(lines) != 2 || lines[0] != "foo" || lines[1] != "bar" {
		t.Fatalf("lines=%q", lines)
	}

	listReply := callTool(t, client, "list_processes", map[string]any{"device": node.Hostname})
	procs := listReply.Data["processes"].([]map[string]any)
	var lp string
	for _, p := range procs {
		if p["process_id"] == pid {
			lp, _ = p["log_path"].(string)
		}
	}
	if lp == "" {
		t.Fatalf("log_path missing for %s", pid)
	}
	if _, err := os.Stat(lp); err != nil {
		t.Fatalf("log_path does not exist: %v", err)
	}
}
```

(Helper names like `startTestStack`, `startEcho`, `waitExit`, `callTool` are placeholders — replace with whatever this repo's e2e test file currently uses.)

- [ ] **Step 3: Run e2e**

```bash
go test -run TestE2E_CaptureScreen ./... -v
```
Expected: PASS.

- [ ] **Step 4: Run full test suite**

```bash
go test ./... -race
```
Expected: all green, no races.

- [ ] **Step 5: Commit**

```bash
git add e2e_test.go
git commit -m "e2e: cover capture_screen + log_path"
```

---

## Task 11: Docs update

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Document `capture_screen`** — add a section explaining the tool, parameters, semantics, and the historical-log workflow (`list_processes` → `log_path` → `file_transfer`). Remove any existing `get_output` references.

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document capture_screen, remove get_output references"
```

---

## Self-review checklist (run after all tasks complete)

- [ ] `grep -rn "GetOutput\|get_output\|ReadOutput" --include='*.go'` returns no hits.
- [ ] `go test ./... -race` is fully green.
- [ ] `go vet ./...` clean.
- [ ] Spec is satisfied: each spec section has a corresponding task above.
- [ ] No "TODO"/"TBD" left in production code.

---

## Notes for the implementer

- **vt10x quirk**: `vt10x.New(WithSize(...))` creates a fixed-size virtual screen — no built-in scrollback. We use 10000 rows as effectively the entire scrollback. Lines that scroll past row 10000 inside the VT will rotate; this is acceptable because `file_transfer` on `log_path` still has the full byte history.
- **vt10x locking**: the library has its own `Lock()/Unlock()` on the View interface. We do not rely on it; `Process.vtMu` is sufficient because we never expose the raw VT outside `Process` methods. If you choose to use `vt.Lock()` for reads, drop `vtMu` for those calls — but keep them paired.
- **`Cell(x, y)` with `Char == 0`**: empty cells return zero-value glyph. We treat that as space when rendering and as "no content" when computing `highestNonEmptyRow`.
- **PTY winsize**: the existing code doesn't set winsize explicitly, so programs see the default (typically 80×24 from the kernel). Programs will line-wrap at column 80 in their own output, but we render at 200 cols. That's fine — wrapped lines still appear correctly because the VT processes the actual bytes the program writes (which include the program's own wrap newlines if it does software wrapping). If you find a program's output mangled, consider extending `startPTY` to set winsize to 200×something via `pty.Setsize` — but treat that as a follow-up, not part of this plan.
