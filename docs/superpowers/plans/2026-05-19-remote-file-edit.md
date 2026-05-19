# Remote File Edit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three MCP tools (`read_file`, `write_file`, `edit_file`) so an AI can edit files on a remote tether node directly, without round-tripping through `file_transfer`.

**Architecture:** Three new request/response message types in `internal/protocol`. A node-side handler module that performs the OS work with atomic writes (temp file + fsync + rename). Three new MCP tool handlers on the client side that parse `node:/path`, send RPCs over the existing hub-routed `msg_id` pattern, and translate errors back to MCP. No changes to hub routing — it already forwards by `target`/`msg_id` for arbitrary message types.

**Tech Stack:** Go 1.x, fxamacker/cbor v2 (existing codec), mark3labs/mcp-go (existing MCP server), stretchr/testify (existing test framework).

**Spec:** `docs/superpowers/specs/2026-05-19-remote-file-edit-design.md`

---

## File Structure

**Create:**
- `internal/node/edit.go` — three handlers (`handleReadFile`, `handleWriteFile`, `handleEditFile`), atomic-write helper.
- `internal/node/edit_test.go` — unit tests for the three handlers.
- `internal/client/tools_edit.go` — three MCP tool registrations and their RPC handlers.
- `internal/client/tools_edit_test.go` — client-side RPC mock tests.

**Modify:**
- `internal/protocol/messages.go` — add `ReadFileReq` / `WriteFileReq` / `EditFileReq` (no Resp structs: replies use the existing `Reply` with `Data map[string]any`, matching how `CaptureScreen` and `List` already work).
- `internal/protocol/codec.go` — register the three new types in `setType` and `Decode`.
- `internal/protocol/codec_test.go` — add round-trip cases for the new types.
- `internal/node/handler.go` — dispatch the three new types in the `Handle` switch.
- `internal/client/mcp_server.go` — call `registerEditTools(s.mcp, c)`.
- `e2e_test.go` — add an end-to-end round-trip test.
- `README.md` — bump tool count from 8 to 11; add sections for the three new tools.

---

## Task 1: Protocol message types

**Files:**
- Modify: `internal/protocol/messages.go`
- Modify: `internal/protocol/codec.go`
- Test: `internal/protocol/codec_test.go`

- [ ] **Step 1: Write the failing codec test**

Append to `internal/protocol/codec_test.go`:

```go
func TestCodecRoundTripReadFile(t *testing.T) {
	in := &ReadFileReq{MsgID: "m1", Target: "n1", Path: "/etc/hosts", Offset: 10, Limit: 50}
	b, err := Encode(in)
	require.NoError(t, err)
	out, err := Decode(b)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestCodecRoundTripWriteFile(t *testing.T) {
	in := &WriteFileReq{MsgID: "m2", Target: "n1", Path: "/tmp/x", Content: []byte("hello"), Overwrite: true, CreateDirs: false}
	b, err := Encode(in)
	require.NoError(t, err)
	out, err := Decode(b)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestCodecRoundTripEditFile(t *testing.T) {
	in := &EditFileReq{MsgID: "m3", Target: "n1", Path: "/tmp/x", OldString: []byte("foo"), NewString: []byte("bar"), ReplaceAll: true}
	b, err := Encode(in)
	require.NoError(t, err)
	out, err := Decode(b)
	require.NoError(t, err)
	require.Equal(t, in, out)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/protocol/ -run RoundTrip -v`
Expected: FAIL with "undefined: ReadFileReq" etc.

- [ ] **Step 3: Add the three structs to `internal/protocol/messages.go`**

Append after `FileLocalCopy` (around line 178):

```go
// ReadFileReq — client → hub → node. Reads a slice of a file's lines.
// Reply.Data: {lines: [][]byte, total_lines int, truncated bool, sha256 string, binary bool}.
type ReadFileReq struct {
	Type   string `cbor:"type"`
	MsgID  string `cbor:"msg_id"`
	Target string `cbor:"target,omitempty"`
	Path   string `cbor:"path"`
	Offset int    `cbor:"offset,omitempty"`
	Limit  int    `cbor:"limit,omitempty"`
}

// WriteFileReq — client → hub → node. Atomic write (temp + fsync + rename).
// Reply.Data: {bytes int64, sha256 string}.
type WriteFileReq struct {
	Type       string `cbor:"type"`
	MsgID      string `cbor:"msg_id"`
	Target     string `cbor:"target,omitempty"`
	Path       string `cbor:"path"`
	Content    []byte `cbor:"content"`
	Overwrite  bool   `cbor:"overwrite,omitempty"`
	CreateDirs bool   `cbor:"create_dirs,omitempty"`
}

// EditFileReq — client → hub → node. Replaces OldString with NewString.
// When ReplaceAll is false, OldString must occur exactly once.
// Reply.Data: {replacements int, sha256 string}.
type EditFileReq struct {
	Type       string `cbor:"type"`
	MsgID      string `cbor:"msg_id"`
	Target     string `cbor:"target,omitempty"`
	Path       string `cbor:"path"`
	OldString  []byte `cbor:"old_string"`
	NewString  []byte `cbor:"new_string"`
	ReplaceAll bool   `cbor:"replace_all,omitempty"`
}
```

And register their `msgType()` methods at the bottom (alongside the other receivers):

```go
func (m *ReadFileReq) msgType() string  { return "read_file" }
func (m *WriteFileReq) msgType() string { return "write_file" }
func (m *EditFileReq) msgType() string  { return "edit_file" }
```

- [ ] **Step 4: Wire into codec**

In `internal/protocol/codec.go`, add to the `setType` switch (alongside `*FileLocalCopy`):

```go
		case *ReadFileReq:
			v.Type = m.msgType()
		case *WriteFileReq:
			v.Type = m.msgType()
		case *EditFileReq:
			v.Type = m.msgType()
```

And in `Decode`'s switch (alongside `"file_local_copy"`):

```go
		case "read_file":
			m = &ReadFileReq{}
		case "write_file":
			m = &WriteFileReq{}
		case "edit_file":
			m = &EditFileReq{}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/protocol/ -v`
Expected: PASS (all existing + 3 new).

- [ ] **Step 6: Commit**

```bash
git add internal/protocol/messages.go internal/protocol/codec.go internal/protocol/codec_test.go
git commit -m "protocol: add read_file/write_file/edit_file message types"
```

---

## Task 2: Node — read_file handler

**Files:**
- Create: `internal/node/edit.go`
- Modify: `internal/node/handler.go`
- Test: `internal/node/edit_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/node/edit_test.go`:

```go
package node

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/require"
)

// captureSender records every Send for assertion.
type captureSender struct{ out []protocol.Message }

func (c *captureSender) Send(m protocol.Message) error { c.out = append(c.out, m); return nil }

func lastReply(t *testing.T, s *captureSender) *protocol.Reply {
	t.Helper()
	require.NotEmpty(t, s.out)
	r, ok := s.out[len(s.out)-1].(*protocol.Reply)
	require.True(t, ok, "last message was not a Reply: %T", s.out[len(s.out)-1])
	return r
}

func TestReadFileWholeFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(p, []byte("line1\nline2\nline3\n"), 0o644))

	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.ReadFileReq{MsgID: "1", Path: p, Offset: 0, Limit: 100})

	r := lastReply(t, s)
	require.True(t, r.OK, "error: %s", r.Error)
	require.Equal(t, []string{"line1", "line2", "line3"}, toLines(r.Data["lines"]))
	require.Equal(t, 3, r.Data["total_lines"])
	require.Equal(t, false, r.Data["truncated"])
	want := sha256.Sum256([]byte("line1\nline2\nline3\n"))
	require.Equal(t, hex.EncodeToString(want[:]), r.Data["sha256"])
}

func TestReadFileOffsetLimit(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(p, []byte("a\nb\nc\nd\ne\n"), 0o644))
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.ReadFileReq{MsgID: "1", Path: p, Offset: 1, Limit: 2})
	r := lastReply(t, s)
	require.True(t, r.OK)
	require.Equal(t, []string{"b", "c"}, toLines(r.Data["lines"]))
	require.Equal(t, 5, r.Data["total_lines"])
	require.Equal(t, true, r.Data["truncated"])
}

func TestReadFileMissing(t *testing.T) {
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.ReadFileReq{MsgID: "1", Path: "/nonexistent/x"})
	r := lastReply(t, s)
	require.False(t, r.OK)
	require.Equal(t, "not_found", r.Error)
}

func TestReadFileTooLarge(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "big")
	require.NoError(t, os.WriteFile(p, make([]byte, editMaxBytes+1), 0o644))
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.ReadFileReq{MsgID: "1", Path: p})
	r := lastReply(t, s)
	require.False(t, r.OK)
	require.Equal(t, "too_large", r.Error)
}

func TestReadFileBinary(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "bin")
	require.NoError(t, os.WriteFile(p, []byte{0xff, 0xfe, 0x00, 0x7f}, 0o644))
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.ReadFileReq{MsgID: "1", Path: p})
	r := lastReply(t, s)
	require.True(t, r.OK)
	require.Equal(t, true, r.Data["binary"])
}

// toLines coerces either []string or []any-of-string from cbor round-trip.
func toLines(v any) []string {
	if s, ok := v.([]string); ok {
		return s
	}
	a := v.([]any)
	out := make([]string, len(a))
	for i, x := range a {
		out[i] = x.(string)
	}
	return out
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/node/ -run TestReadFile -v`
Expected: FAIL with "undefined: NewEditHandler".

- [ ] **Step 3: Create `internal/node/edit.go`**

```go
package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/qiuxiang/tether/internal/protocol"
)

// editMaxBytes caps file size for read_file/write_file/edit_file to bound
// node memory. Files bigger than this must use file_transfer.
const editMaxBytes = 10 * 1024 * 1024 // 10 MiB

type EditHandler struct{}

func NewEditHandler() *EditHandler { return &EditHandler{} }

func (h *EditHandler) Handle(send Sender, msg protocol.Message) {
	switch m := msg.(type) {
	case *protocol.ReadFileReq:
		h.handleRead(send, m)
	case *protocol.WriteFileReq:
		h.handleWrite(send, m)
	case *protocol.EditFileReq:
		h.handleEdit(send, m)
	}
}

func (h *EditHandler) handleRead(send Sender, m *protocol.ReadFileReq) {
	path, err := expandPath(m.Path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	info, err := os.Lstat(path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: classifyErr(err)})
		return
	}
	if info.IsDir() {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "is_directory"})
		return
	}
	if info.Size() > editMaxBytes {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "too_large"})
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: classifyErr(err)})
		return
	}
	sum := sha256.Sum256(data)
	binary := !utf8.Valid(data)

	allLines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(data) == 0 {
		allLines = nil
	}
	total := len(allLines)

	limit := m.Limit
	if limit <= 0 {
		limit = 2000
	}
	offset := m.Offset
	if offset < 0 {
		offset = 0
	}
	end := offset + limit
	if end > total {
		end = total
	}
	var window []string
	if offset < total {
		window = allLines[offset:end]
	}
	truncated := end < total

	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"lines":       window,
		"total_lines": total,
		"truncated":   truncated,
		"sha256":      hex.EncodeToString(sum[:]),
		"binary":      binary,
	}})
}

// classifyErr maps common os errors to the spec's error codes.
func classifyErr(err error) string {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "not_found"
	case errors.Is(err, fs.ErrPermission):
		return "permission_denied"
	default:
		return err.Error()
	}
}

// Placeholders so future tasks don't leave unused imports.
var _ = bytes.Equal
var _ = filepath.Dir
```

- [ ] **Step 4: Wire into `handler.go`**

In `internal/node/handler.go`, add to `ProcessHandler`:

```go
type ProcessHandler struct {
	registry       *ProcessRegistry
	logDir         string
	mu             sync.Mutex
	attachSubs     map[string]attachRec
	fileHandler    *FileHandler
	editHandler    *EditHandler
	forwardHandler *ForwardHandler
}
```

In `NewProcessHandler`:

```go
		fileHandler:    NewFileHandler(),
		editHandler:    NewEditHandler(),
		forwardHandler: NewForwardHandler(),
```

In the `Handle` switch, add a case alongside `FilePutOpen`:

```go
		case *protocol.ReadFileReq, *protocol.WriteFileReq, *protocol.EditFileReq:
			h.editHandler.Handle(send, msg)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/node/ -run TestReadFile -v`
Expected: PASS (5 subtests).

- [ ] **Step 6: Commit**

```bash
git add internal/node/edit.go internal/node/edit_test.go internal/node/handler.go
git commit -m "node: add read_file handler with sha256 + binary detection"
```

---

## Task 3: Node — write_file handler (atomic)

**Files:**
- Modify: `internal/node/edit.go`
- Modify: `internal/node/edit_test.go`

- [ ] **Step 1: Append failing tests to `internal/node/edit_test.go`**

```go
func TestWriteFileNew(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out.txt")
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.WriteFileReq{MsgID: "1", Path: p, Content: []byte("hello")})
	r := lastReply(t, s)
	require.True(t, r.OK, "err: %s", r.Error)
	got, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))
	// No leftover temp files
	entries, _ := os.ReadDir(dir)
	require.Len(t, entries, 1)
}

func TestWriteFileNoOverwrite(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out.txt")
	require.NoError(t, os.WriteFile(p, []byte("old"), 0o644))
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.WriteFileReq{MsgID: "1", Path: p, Content: []byte("new")})
	r := lastReply(t, s)
	require.False(t, r.OK)
	require.Equal(t, "exists", r.Error)
	got, _ := os.ReadFile(p)
	require.Equal(t, "old", string(got))
}

func TestWriteFileOverwrite(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out.txt")
	require.NoError(t, os.WriteFile(p, []byte("old"), 0o644))
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.WriteFileReq{MsgID: "1", Path: p, Content: []byte("new"), Overwrite: true})
	r := lastReply(t, s)
	require.True(t, r.OK)
	got, _ := os.ReadFile(p)
	require.Equal(t, "new", string(got))
}

func TestWriteFileMissingParent(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "sub", "out.txt")
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.WriteFileReq{MsgID: "1", Path: p, Content: []byte("x")})
	r := lastReply(t, s)
	require.False(t, r.OK)
}

func TestWriteFileCreateDirs(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "sub", "out.txt")
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.WriteFileReq{MsgID: "1", Path: p, Content: []byte("x"), CreateDirs: true})
	r := lastReply(t, s)
	require.True(t, r.OK, "err: %s", r.Error)
	got, _ := os.ReadFile(p)
	require.Equal(t, "x", string(got))
}

func TestWriteFileTooLarge(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "big")
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.WriteFileReq{MsgID: "1", Path: p, Content: make([]byte, editMaxBytes+1)})
	r := lastReply(t, s)
	require.False(t, r.OK)
	require.Equal(t, "too_large", r.Error)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/node/ -run TestWriteFile -v`
Expected: FAIL — `handleWrite` is a no-op so far.

- [ ] **Step 3: Implement `handleWrite` and the atomic-write helper in `internal/node/edit.go`**

Add to `edit.go`:

```go
func (h *EditHandler) handleWrite(send Sender, m *protocol.WriteFileReq) {
	path, err := expandPath(m.Path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	if int64(len(m.Content)) > editMaxBytes {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "too_large"})
		return
	}
	bytes, sum, err := atomicWrite(path, m.Content, m.Overwrite, m.CreateDirs)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"bytes":  bytes,
		"sha256": sum,
	}})
}

// atomicWrite writes data to path via a same-directory temp file, fsyncs,
// then renames over the destination. Returns (bytes, sha256-hex, err).
// err is already a spec error-code string (e.g. "exists", "not_found",
// "permission_denied", "is_directory") when possible.
func atomicWrite(path string, data []byte, overwrite, createDirs bool) (int64, string, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.IsDir() {
			return 0, "", errors.New("is_directory")
		}
		if !overwrite {
			return 0, "", errors.New("exists")
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return 0, "", errors.New(classifyErr(err))
	}

	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return 0, "", errors.New(classifyErr(err))
		}
		if !createDirs {
			return 0, "", errors.New("not_found")
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return 0, "", errors.New(classifyErr(err))
		}
	}

	tmp, err := os.CreateTemp(dir, ".tether-edit-*.tmp")
	if err != nil {
		return 0, "", errors.New(classifyErr(err))
	}
	tmpPath := tmp.Name()
	cleanup := func() { os.Remove(tmpPath) }

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		cleanup()
		return 0, "", err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return 0, "", err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return 0, "", err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return 0, "", errors.New(classifyErr(err))
	}
	sum := sha256.Sum256(data)
	return int64(len(data)), hex.EncodeToString(sum[:]), nil
}
```

Remove the placeholder `_ = bytes.Equal` and `_ = filepath.Dir` lines from Task 2 — those imports are now used.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/node/ -run TestWriteFile -v`
Expected: PASS (6 subtests).

- [ ] **Step 5: Commit**

```bash
git add internal/node/edit.go internal/node/edit_test.go
git commit -m "node: add write_file handler with atomic temp+rename"
```

---

## Task 4: Node — edit_file handler

**Files:**
- Modify: `internal/node/edit.go`
- Modify: `internal/node/edit_test.go`

- [ ] **Step 1: Append failing tests**

```go
func TestEditFileUnique(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(p, []byte("alpha\nbeta\ngamma\n"), 0o644))
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.EditFileReq{MsgID: "1", Path: p, OldString: []byte("beta"), NewString: []byte("BETA")})
	r := lastReply(t, s)
	require.True(t, r.OK, "err: %s", r.Error)
	require.Equal(t, 1, r.Data["replacements"])
	got, _ := os.ReadFile(p)
	require.Equal(t, "alpha\nBETA\ngamma\n", string(got))
}

func TestEditFileNotUnique(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(p, []byte("foo foo\n"), 0o644))
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.EditFileReq{MsgID: "1", Path: p, OldString: []byte("foo"), NewString: []byte("bar")})
	r := lastReply(t, s)
	require.False(t, r.OK)
	require.Equal(t, "not_unique", r.Error)
	got, _ := os.ReadFile(p)
	require.Equal(t, "foo foo\n", string(got))
}

func TestEditFileReplaceAll(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(p, []byte("foo foo foo\n"), 0o644))
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.EditFileReq{MsgID: "1", Path: p, OldString: []byte("foo"), NewString: []byte("bar"), ReplaceAll: true})
	r := lastReply(t, s)
	require.True(t, r.OK)
	require.Equal(t, 3, r.Data["replacements"])
	got, _ := os.ReadFile(p)
	require.Equal(t, "bar bar bar\n", string(got))
}

func TestEditFileNotFound(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(p, []byte("alpha\n"), 0o644))
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.EditFileReq{MsgID: "1", Path: p, OldString: []byte("beta"), NewString: []byte("x")})
	r := lastReply(t, s)
	require.False(t, r.OK)
	require.Equal(t, "not_found", r.Error)
}

func TestEditFileMissing(t *testing.T) {
	h := NewEditHandler()
	s := &captureSender{}
	h.Handle(s, &protocol.EditFileReq{MsgID: "1", Path: "/nonexistent/x", OldString: []byte("a"), NewString: []byte("b")})
	r := lastReply(t, s)
	require.False(t, r.OK)
	require.Equal(t, "not_found", r.Error)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/node/ -run TestEditFile -v`
Expected: FAIL.

- [ ] **Step 3: Implement `handleEdit` in `internal/node/edit.go`**

Append:

```go
func (h *EditHandler) handleEdit(send Sender, m *protocol.EditFileReq) {
	path, err := expandPath(m.Path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	info, err := os.Lstat(path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: classifyErr(err)})
		return
	}
	if info.IsDir() {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "is_directory"})
		return
	}
	if info.Size() > editMaxBytes {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "too_large"})
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: classifyErr(err)})
		return
	}

	count := bytes.Count(data, m.OldString)
	if count == 0 {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "not_found"})
		return
	}
	if count > 1 && !m.ReplaceAll {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "not_unique"})
		return
	}

	var out []byte
	var replaced int
	if m.ReplaceAll {
		out = bytes.ReplaceAll(data, m.OldString, m.NewString)
		replaced = count
	} else {
		out = bytes.Replace(data, m.OldString, m.NewString, 1)
		replaced = 1
	}
	if int64(len(out)) > editMaxBytes {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "too_large"})
		return
	}
	_, sum, werr := atomicWrite(path, out, true, false)
	if werr != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: werr.Error()})
		return
	}
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"replacements": replaced,
		"sha256":       sum,
	}})
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/node/ -v`
Expected: PASS (all 5 read + 6 write + 5 edit subtests, plus pre-existing tests).

- [ ] **Step 5: Commit**

```bash
git add internal/node/edit.go internal/node/edit_test.go
git commit -m "node: add edit_file handler with uniqueness check"
```

---

## Task 5: Client — MCP tool registrations

**Files:**
- Create: `internal/client/tools_edit.go`
- Modify: `internal/client/mcp_server.go`
- Test: `internal/client/tools_edit_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/client/tools_edit_test.go`:

```go
package client

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestReadFileToolHappyPath(t *testing.T) {
	c := newFakeConn(t)
	defer c.Close()

	go func() {
		// Wait for the Send, then reply.
		msg := <-c.sent
		req := msg.(*protocol.ReadFileReq)
		require.Equal(t, "node1", req.Target)
		require.Equal(t, "/etc/hosts", req.Path)
		c.injectReply(&protocol.Reply{MsgID: req.MsgID, OK: true, Data: map[string]any{
			"lines":       []string{"127.0.0.1 localhost"},
			"total_lines": 1,
			"truncated":   false,
			"sha256":      "abc",
			"binary":      false,
		}})
	}()

	res := callReadFileTool(t, c.Conn, "node1:/etc/hosts")
	require.False(t, res.IsError)
	body := unwrapText(t, res)
	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &got))
	require.Equal(t, "abc", got["sha256"])
}

func TestReadFileToolRejectsLocalPath(t *testing.T) {
	c := newFakeConn(t)
	defer c.Close()
	res := callReadFileTool(t, c.Conn, "/etc/hosts")
	require.True(t, res.IsError)
}

func TestWriteFileToolHappyPath(t *testing.T) {
	c := newFakeConn(t)
	defer c.Close()
	go func() {
		msg := <-c.sent
		req := msg.(*protocol.WriteFileReq)
		c.injectReply(&protocol.Reply{MsgID: req.MsgID, OK: true, Data: map[string]any{
			"bytes": int64(5), "sha256": "deadbeef",
		}})
	}()
	res := callWriteFileTool(t, c.Conn, "node1:/tmp/x", "hello", false)
	require.False(t, res.IsError, "body: %s", unwrapText(t, res))
}

func TestEditFileToolNotUnique(t *testing.T) {
	c := newFakeConn(t)
	defer c.Close()
	go func() {
		msg := <-c.sent
		req := msg.(*protocol.EditFileReq)
		c.injectReply(&protocol.Reply{MsgID: req.MsgID, OK: false, Error: "not_unique"})
	}()
	res := callEditFileTool(t, c.Conn, "node1:/tmp/x", "foo", "bar", false)
	require.True(t, res.IsError)
}

// Test helpers below assume a `newFakeConn` helper exists. If the codebase
// already uses a fake-conn pattern in another *_test.go (e.g. rpc_test.go or
// tools_exec_test.go), reuse it; otherwise add it to client_test_helpers.go.

func callReadFileTool(t *testing.T, c *Conn, path string) *mcp.CallToolResult {
	t.Helper()
	return invokeTool(t, c, "read_file", map[string]any{"path": path})
}

func callWriteFileTool(t *testing.T, c *Conn, path, content string, overwrite bool) *mcp.CallToolResult {
	t.Helper()
	return invokeTool(t, c, "write_file", map[string]any{"path": path, "content": content, "overwrite": overwrite})
}

func callEditFileTool(t *testing.T, c *Conn, path, oldS, newS string, all bool) *mcp.CallToolResult {
	t.Helper()
	return invokeTool(t, c, "edit_file", map[string]any{"path": path, "old_string": oldS, "new_string": newS, "replace_all": all})
}

func invokeTool(t *testing.T, c *Conn, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv := NewMCPServer(c)
	_ = srv
	// Find the handler by name and invoke it. Reuse whatever invocation helper
	// existing tests use (see tools_exec_test.go). If none exists, add one in
	// client_test_helpers.go that walks srv.mcp's tool registry.
	return invokeRegisteredTool(t, ctx, c, name, args)
}

func unwrapText(t *testing.T, r *mcp.CallToolResult) string {
	t.Helper()
	require.NotEmpty(t, r.Content)
	tc, ok := r.Content[0].(mcp.TextContent)
	require.True(t, ok)
	return tc.Text
}
```

Note: `invokeRegisteredTool`, `newFakeConn`, and `injectReply` must follow the patterns already used in `tools_exec_test.go` and `rpc_test.go`. Before writing the implementation, **read those existing test files** and use their exact helper names — do not invent new ones. If equivalent helpers don't exist, add them to a shared `client_test_helpers.go` rather than duplicating per file.

- [ ] **Step 2: Read existing helpers**

Run: `grep -n "func " internal/client/tools_exec_test.go internal/client/tools_file_test.go internal/client/rpc_test.go`

Adapt the test code in Step 1 to match the actual helper names found. If the existing tests inline the fake-conn setup, follow the same inline pattern instead of factoring it out.

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/client/ -run "TestReadFileTool|TestWriteFileTool|TestEditFileTool" -v`
Expected: FAIL — `read_file`/`write_file`/`edit_file` tools not registered.

- [ ] **Step 4: Create `internal/client/tools_edit.go`**

```go
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/qiuxiang/tether/internal/protocol"
)

const editRPCTimeout = 30 * time.Second

func registerEditTools(m *server.MCPServer, c *Conn) {
	m.AddTool(
		mcp.NewTool("read_file",
			mcp.WithDescription("Read a slice of a file on a remote node. Path must be 'node:/abs/path' or 'node:~/path'. Returns {lines, total_lines, truncated, sha256, binary}. Max file size 10 MB — use file_transfer for larger files."),
			mcp.WithString("path", mcp.Required()),
			mcp.WithNumber("offset"),
			mcp.WithNumber("limit"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			pathStr, _ := args["path"].(string)
			offset, _ := args["offset"].(float64)
			limit, _ := args["limit"].(float64)
			node, path, err := mustNodePath(pathStr)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.ReadFileReq{
				MsgID: id, Target: node, Path: path,
				Offset: int(offset), Limit: int(limit),
			}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return awaitReply(ctx, ch)
		},
	)

	m.AddTool(
		mcp.NewTool("write_file",
			mcp.WithDescription("Atomically write a file on a remote node. Path must be 'node:/abs/path' or 'node:~/path'. Default refuses to overwrite. Max 10 MB."),
			mcp.WithString("path", mcp.Required()),
			mcp.WithString("content", mcp.Required()),
			mcp.WithBoolean("overwrite"),
			mcp.WithBoolean("create_dirs"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			pathStr, _ := args["path"].(string)
			content, _ := args["content"].(string)
			overwrite, _ := args["overwrite"].(bool)
			createDirs, _ := args["create_dirs"].(bool)
			node, path, err := mustNodePath(pathStr)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.WriteFileReq{
				MsgID: id, Target: node, Path: path,
				Content: []byte(content), Overwrite: overwrite, CreateDirs: createDirs,
			}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return awaitReply(ctx, ch)
		},
	)

	m.AddTool(
		mcp.NewTool("edit_file",
			mcp.WithDescription("Replace old_string with new_string in a file on a remote node. Path must be 'node:/abs/path'. With replace_all=false (default), old_string must occur exactly once. Max 10 MB."),
			mcp.WithString("path", mcp.Required()),
			mcp.WithString("old_string", mcp.Required()),
			mcp.WithString("new_string", mcp.Required()),
			mcp.WithBoolean("replace_all"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			pathStr, _ := args["path"].(string)
			oldS, _ := args["old_string"].(string)
			newS, _ := args["new_string"].(string)
			replaceAll, _ := args["replace_all"].(bool)
			node, path, err := mustNodePath(pathStr)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.EditFileReq{
				MsgID: id, Target: node, Path: path,
				OldString: []byte(oldS), NewString: []byte(newS), ReplaceAll: replaceAll,
			}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return awaitReply(ctx, ch)
		},
	)
}

// mustNodePath enforces the node: prefix; local paths are rejected.
// Reuses parsePath from tools_file.go.
func mustNodePath(s string) (node, path string, err error) {
	ps, perr := parsePath(s)
	if perr != nil {
		return "", "", perr
	}
	if ps.Node == "" {
		return "", "", fmt.Errorf("path must be in 'node:/abs/path' form; got %q", s)
	}
	return ps.Node, ps.Path, nil
}

func awaitReply(ctx context.Context, ch chan *protocol.Reply) (*mcp.CallToolResult, error) {
	select {
	case r := <-ch:
		if !r.OK {
			return mcp.NewToolResultError(r.Error), nil
		}
		b, _ := json.Marshal(r.Data)
		return mcp.NewToolResultText(string(b)), nil
	case <-ctx.Done():
		return mcp.NewToolResultError(ctx.Err().Error()), nil
	case <-time.After(editRPCTimeout):
		return mcp.NewToolResultError("rpc_timeout"), nil
	}
}
```

- [ ] **Step 5: Wire into `mcp_server.go`**

Edit `internal/client/mcp_server.go`'s `NewMCPServer`:

```go
func NewMCPServer(c *Conn) *Server {
	s := &Server{conn: c, mcp: server.NewMCPServer("tether", "0.1.0")}
	registerExecTools(s.mcp, c)
	registerFileTool(s.mcp, c)
	registerEditTools(s.mcp, c)
	return s
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/client/ -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/client/tools_edit.go internal/client/tools_edit_test.go internal/client/mcp_server.go
git commit -m "client: register read_file/write_file/edit_file MCP tools"
```

---

## Task 6: End-to-end round-trip test

**Files:**
- Modify: `e2e_test.go`

- [ ] **Step 1: Read the existing file-transfer e2e block (lines 300-360 area) to copy its fixture pattern**

Run: `sed -n '290,360p' e2e_test.go`

- [ ] **Step 2: Append the new e2e test**

Append to `e2e_test.go`:

```go
func TestE2ERemoteFileEdit(t *testing.T) {
	hubAddr, nodeName, c := setupHubAndNode(t) // use the same fixture name as the existing file_transfer e2e
	_ = hubAddr
	_ = nodeName

	dir := t.TempDir()
	target := filepath.Join(dir, "file.txt")

	// 1. write_file
	id1 := protocol.NewMsgID()
	ch := c.rpc.Register(id1)
	require.NoError(t, c.Send(&protocol.WriteFileReq{
		MsgID: id1, Target: nodeName, Path: target,
		Content: []byte("alpha\nbeta\ngamma\n"),
	}))
	r1 := <-ch
	c.rpc.Unregister(id1)
	require.True(t, r1.OK, "write err: %s", r1.Error)

	// 2. read_file
	id2 := protocol.NewMsgID()
	ch = c.rpc.Register(id2)
	require.NoError(t, c.Send(&protocol.ReadFileReq{MsgID: id2, Target: nodeName, Path: target}))
	r2 := <-ch
	c.rpc.Unregister(id2)
	require.True(t, r2.OK)
	require.Equal(t, 3, r2.Data["total_lines"])

	// 3. edit_file
	id3 := protocol.NewMsgID()
	ch = c.rpc.Register(id3)
	require.NoError(t, c.Send(&protocol.EditFileReq{
		MsgID: id3, Target: nodeName, Path: target,
		OldString: []byte("beta"), NewString: []byte("BETA"),
	}))
	r3 := <-ch
	c.rpc.Unregister(id3)
	require.True(t, r3.OK)
	require.Equal(t, 1, r3.Data["replacements"])

	// 4. Verify on disk
	got, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "alpha\nBETA\ngamma\n", string(got))
}
```

**Important:** Before pasting verbatim, read the existing `TestE2EFileTransfer` (or whatever the file_transfer e2e is named — find it via `grep -n "FilePutOpen" e2e_test.go`) and adapt:
- The fixture helper name (`setupHubAndNode` is a placeholder — use the existing one).
- The `NewMsgID` package — if the existing test uses `client.NewMsgID`, use that.
- `c` is whatever `Conn` variable the surrounding test uses.

- [ ] **Step 3: Run e2e test**

Run: `go test . -run TestE2ERemoteFileEdit -v`
Expected: PASS.

- [ ] **Step 4: Run full test suite**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add e2e_test.go
git commit -m "e2e: cover remote file write/read/edit round-trip"
```

---

## Task 7: Documentation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update tool count and add sections**

In `README.md`, find the line that lists the 8 tools:

```
8 tools become available: `list_devices`, `exec`, `start_process`, `list_processes`, `capture_screen`, `send_stdin`, `kill_process`, `file_transfer`.
```

Replace with:

```
11 tools become available: `list_devices`, `exec`, `start_process`, `list_processes`, `capture_screen`, `send_stdin`, `kill_process`, `file_transfer`, `read_file`, `write_file`, `edit_file`.
```

Then add a new section after `### file_transfer` (and before `### Port forwarding`):

````markdown
### read_file / write_file / edit_file

In-place file editing on a node, mirroring Claude Code's built-in Read / Write / Edit tools but with `node:` paths.

```
read_file(path, offset?=0, limit?=2000)
  → {lines, total_lines, truncated, sha256, binary}

write_file(path, content, overwrite?=false, create_dirs?=false)
  → {bytes, sha256}

edit_file(path, old_string, new_string, replace_all?=false)
  → {replacements, sha256}
```

All paths must be `node:/abs/path` or `node:~/path` — local files are handled by Claude Code's built-in tools. Writes are atomic (temp file + fsync + rename in the same directory). `edit_file` requires `old_string` to occur exactly once unless `replace_all=true`. The 10 MB per-file limit applies to all three — use `file_transfer` for anything larger.
````

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document read_file/write_file/edit_file MCP tools"
```

---

## Self-Review Checklist (run after the plan is fully drafted; already done — keeping the trail)

1. **Spec coverage:**
   - Tool surface (read/write/edit, signatures, node: prefix) → Tasks 5, 7
   - Atomic write semantics → Task 3
   - `edit_file` uniqueness → Task 4
   - 10 MB cap → Tasks 2, 3, 4
   - Binary fallback hint → Task 2
   - Protocol messages → Task 1
   - Node handler + dispatch → Tasks 2, 3, 4
   - Client tool registration → Task 5
   - Tests (unit + e2e) → Tasks 2-6
   - README → Task 7
   - No path allowlist, no CAS, no recursion → enforced by absence
2. **Placeholder scan:** No "TBD"/"TODO"/"handle edge cases"; every code step has full code. Task 5 Step 1 references existing test helpers — the step explicitly tells the implementer to read them first rather than inventing names.
3. **Type consistency:** `ReadFileReq`/`WriteFileReq`/`EditFileReq` used identically across all tasks. `editMaxBytes` defined in Task 2, reused in Tasks 3 and 4. `atomicWrite` defined in Task 3, reused in Task 4. `mustNodePath` defined in Task 5, used only within Task 5. `parsePath` from `tools_file.go` reused (no new path parser).
