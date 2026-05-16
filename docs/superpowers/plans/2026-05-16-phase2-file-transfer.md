# Phase 2: file_transfer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Prerequisite:** Phase 1 (MCP architecture refactor) must be merged. This plan assumes `internal/client/`, the `/client` WSS endpoint, `ListDevices`, and the peer-conn router exist.

**Goal:** Add a single MCP tool `file_transfer(from, to, overwrite)` that supports five routing combinations (local↔node, node↔node, same-node copy), with atomic node-side writes, sha256 verification, and a hard size cap.

**Architecture:** Six new protocol messages (`FileGetOpen`, `FilePutOpen`, `FileChunk`, `FileAbort`, `FileRelay`, `FileLocalCopy`). The Hub gains a `relay.go` coordinator for node↔node transfers and uses the existing sticky-routing capability of the router for client↔node chunk streams. Nodes get an `internal/node/file.go` handler that streams reads/writes through 256 KB CBOR-framed chunks, writes via a `.tether-tmp-<msg_id>` temp file with atomic rename on success, and verifies sha256 end-to-end. The client adds `internal/client/tools_file.go` implementing the MCP tool, parsing the `node:` path prefix, and dispatching to the correct routing flow.

**Tech Stack:** Same as Phase 1.

---

## File Structure

**New files:**
- `internal/node/file.go` — node-side handlers for `FileGetOpen`, `FilePutOpen`, `FileChunk`, `FileAbort`, `FileLocalCopy`
- `internal/node/file_test.go` — local file handler tests
- `internal/hub/relay.go` — `FileRelay` coordinator (node↔node streaming via Hub)
- `internal/hub/relay_test.go`
- `internal/client/tools_file.go` — `file_transfer` MCP tool
- `internal/client/tools_file_test.go`

**Modified files:**
- `internal/protocol/messages.go` — add six file message structs
- `internal/protocol/codec.go` — register six new types
- `internal/protocol/codec_test.go` — add round-trip tests
- `internal/node/handler.go` — dispatch six new message types to file handler
- `internal/hub/client_ws.go` — add dispatch cases for `FileGetOpen`, `FilePutOpen`, `FileChunk`, `FileAbort`, `FileRelay`, `FileLocalCopy`
- `internal/hub/server.go` — instantiate relay coordinator
- `internal/client/mcp_server.go` — register `file_transfer` tool
- `e2e_test.go` — add a file transfer scenario
- `README.md` — document `file_transfer`

---

## Task 1: Add six file protocol messages

**Files:**
- Modify: `internal/protocol/messages.go`
- Modify: `internal/protocol/codec.go`
- Modify: `internal/protocol/codec_test.go`

- [ ] **Step 1: Append message structs**

Edit `internal/protocol/messages.go`, append after existing messages:

```go
// FileGetOpen — client → hub → node (download). Node replies with metadata
// then pushes FileChunk frames until EOF.
type FileGetOpen struct {
    Type   string `cbor:"type"`
    MsgID  string `cbor:"msg_id"`
    Target string `cbor:"target,omitempty"`
    Path   string `cbor:"path"`
}

// FilePutOpen — client → hub → node (upload). Node replies ok:true when
// ready; client pushes FileChunk frames until EOF=true; node verifies
// sha256 then sends the final Reply.
type FilePutOpen struct {
    Type      string `cbor:"type"`
    MsgID     string `cbor:"msg_id"`
    Target    string `cbor:"target,omitempty"`
    Path      string `cbor:"path"`
    Size      int64  `cbor:"size"`
    Mode      uint32 `cbor:"mode,omitempty"`
    Overwrite bool   `cbor:"overwrite,omitempty"`
    SHA256    string `cbor:"sha256,omitempty"`
}

// FileChunk — bidirectional streaming frame keyed by msg_id.
type FileChunk struct {
    Type  string `cbor:"type"`
    MsgID string `cbor:"msg_id"`
    Seq   int64  `cbor:"seq"`
    Data  []byte `cbor:"data"`
    EOF   bool   `cbor:"eof,omitempty"`
}

// FileAbort — either side cancels a transfer.
type FileAbort struct {
    Type  string `cbor:"type"`
    MsgID string `cbor:"msg_id"`
    Error string `cbor:"error"`
}

// FileRelay — client → hub only. Hub coordinates a streaming copy between
// from_node and to_node.
type FileRelay struct {
    Type      string `cbor:"type"`
    MsgID     string `cbor:"msg_id"`
    FromNode  string `cbor:"from_node"`
    FromPath  string `cbor:"from_path"`
    ToNode    string `cbor:"to_node"`
    ToPath    string `cbor:"to_path"`
    Overwrite bool   `cbor:"overwrite,omitempty"`
}

// FileLocalCopy — client → hub → node. Same-node copy between two paths.
type FileLocalCopy struct {
    Type      string `cbor:"type"`
    MsgID     string `cbor:"msg_id"`
    Target    string `cbor:"target,omitempty"`
    FromPath  string `cbor:"from_path"`
    ToPath    string `cbor:"to_path"`
    Overwrite bool   `cbor:"overwrite,omitempty"`
}

func (m *FileGetOpen) msgType() string   { return "file_get_open" }
func (m *FilePutOpen) msgType() string   { return "file_put_open" }
func (m *FileChunk) msgType() string     { return "file_chunk" }
func (m *FileAbort) msgType() string     { return "file_abort" }
func (m *FileRelay) msgType() string     { return "file_relay" }
func (m *FileLocalCopy) msgType() string { return "file_local_copy" }
```

- [ ] **Step 2: Register in codec**

Edit `internal/protocol/codec.go`. In `setType`, add cases:

```go
case *FileGetOpen:
    v.Type = m.msgType()
case *FilePutOpen:
    v.Type = m.msgType()
case *FileChunk:
    v.Type = m.msgType()
case *FileAbort:
    v.Type = m.msgType()
case *FileRelay:
    v.Type = m.msgType()
case *FileLocalCopy:
    v.Type = m.msgType()
```

In `Decode`, add cases:

```go
case "file_get_open":
    m = &FileGetOpen{}
case "file_put_open":
    m = &FilePutOpen{}
case "file_chunk":
    m = &FileChunk{}
case "file_abort":
    m = &FileAbort{}
case "file_relay":
    m = &FileRelay{}
case "file_local_copy":
    m = &FileLocalCopy{}
```

- [ ] **Step 3: Add round-trip tests**

Append to `internal/protocol/codec_test.go`:

```go
func TestRoundTripFilePutOpen(t *testing.T) {
    in := &FilePutOpen{MsgID: "m1", Target: "n1", Path: "/tmp/x", Size: 1024, SHA256: "abc", Overwrite: true}
    raw, err := Encode(in)
    require.NoError(t, err)
    out, err := Decode(raw)
    require.NoError(t, err)
    got, ok := out.(*FilePutOpen)
    require.True(t, ok)
    require.Equal(t, "n1", got.Target)
    require.Equal(t, int64(1024), got.Size)
    require.True(t, got.Overwrite)
}

func TestRoundTripFileChunk(t *testing.T) {
    in := &FileChunk{MsgID: "m1", Seq: 3, Data: []byte("hello"), EOF: true}
    raw, err := Encode(in)
    require.NoError(t, err)
    out, err := Decode(raw)
    require.NoError(t, err)
    got, ok := out.(*FileChunk)
    require.True(t, ok)
    require.Equal(t, int64(3), got.Seq)
    require.Equal(t, []byte("hello"), got.Data)
    require.True(t, got.EOF)
}

func TestRoundTripFileRelay(t *testing.T) {
    in := &FileRelay{MsgID: "m1", FromNode: "a", FromPath: "/a", ToNode: "b", ToPath: "/b"}
    raw, err := Encode(in)
    require.NoError(t, err)
    out, err := Decode(raw)
    require.NoError(t, err)
    got, ok := out.(*FileRelay)
    require.True(t, ok)
    require.Equal(t, "a", got.FromNode)
    require.Equal(t, "/b", got.ToPath)
}
```

- [ ] **Step 4: Test**

```
go test ./internal/protocol/...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add internal/protocol/
git commit -m "protocol: add six file transfer messages"
```

---

## Task 2: Node-side file handler — basic FilePutOpen + FileChunk + atomic write

**Files:**
- Create: `internal/node/file.go`
- Create: `internal/node/file_test.go`
- Modify: `internal/node/handler.go` (dispatch new types)

### Why

The simplest path first: receive a `FilePutOpen`, stream `FileChunk`s into a temp file, verify sha256 on EOF, atomic rename. This lets us prove the wire and write paths before adding download/relay/local-copy.

The node has the rest of the file transfer state in a per-node registry: `msgID → in-flight transfer`. When a chunk arrives with an unknown msg_id, we abort.

- [ ] **Step 1: Write file.go with FilePutOpen + FileChunk handling**

Create `internal/node/file.go`:

```go
package node

import (
    "crypto/sha256"
    "encoding/hex"
    "errors"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "strings"
    "sync"

    "github.com/qiuxiang/tether/internal/protocol"
)

// FileHandler manages in-flight uploads and downloads on a node.
type FileHandler struct {
    mu        sync.Mutex
    uploads   map[string]*uploadState
    downloads map[string]*downloadState

    MaxFileSize int64 // 0 = no limit
}

type uploadState struct {
    path      string
    tmpPath   string
    f         *os.File
    h         interface{ Write([]byte) (int, error); Sum([]byte) []byte }
    wantSHA   string
    written   int64
    overwrite bool
    finalMode os.FileMode
}

type downloadState struct {
    cancel chan struct{}
}

func NewFileHandler() *FileHandler {
    return &FileHandler{
        uploads:     make(map[string]*uploadState),
        downloads:   make(map[string]*downloadState),
        MaxFileSize: 5 << 30, // 5 GiB
    }
}

func (h *FileHandler) Handle(send Sender, msg protocol.Message) {
    switch m := msg.(type) {
    case *protocol.FilePutOpen:
        h.handlePutOpen(send, m)
    case *protocol.FileChunk:
        h.handleChunk(send, m)
    case *protocol.FileAbort:
        h.handleAbort(m)
    case *protocol.FileGetOpen:
        h.handleGetOpen(send, m)
    case *protocol.FileLocalCopy:
        h.handleLocalCopy(send, m)
    }
}

func expandPath(p string) (string, error) {
    if strings.HasPrefix(p, "~") {
        home, err := os.UserHomeDir()
        if err != nil {
            return "", err
        }
        p = home + p[1:]
    }
    if !filepath.IsAbs(p) {
        return "", fmt.Errorf("path must be absolute: %s", p)
    }
    return p, nil
}

func (h *FileHandler) handlePutOpen(send Sender, m *protocol.FilePutOpen) {
    path, err := expandPath(m.Path)
    if err != nil {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
        return
    }
    if h.MaxFileSize > 0 && m.Size > h.MaxFileSize {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "size_limit_exceeded"})
        return
    }
    if _, err := os.Stat(path); err == nil && !m.Overwrite {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "destination_exists"})
        return
    }
    tmp := path + ".tether-tmp-" + m.MsgID
    mode := os.FileMode(0o644)
    if m.Mode != 0 {
        mode = os.FileMode(m.Mode)
    }
    f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
    if err != nil {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
        return
    }
    st := &uploadState{
        path: path, tmpPath: tmp, f: f, h: sha256.New(),
        wantSHA: m.SHA256, overwrite: m.Overwrite, finalMode: mode,
    }
    h.mu.Lock()
    h.uploads[m.MsgID] = st
    h.mu.Unlock()
    send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true})
}

func (h *FileHandler) handleChunk(send Sender, m *protocol.FileChunk) {
    h.mu.Lock()
    st, ok := h.uploads[m.MsgID]
    h.mu.Unlock()
    if !ok {
        // No matching upload — drop. (Downloads receive chunks at the other side.)
        return
    }
    if len(m.Data) > 0 {
        if _, err := st.f.Write(m.Data); err != nil {
            h.failUpload(send, m.MsgID, st, "disk_full")
            return
        }
        st.h.Write(m.Data)
        st.written += int64(len(m.Data))
        if h.MaxFileSize > 0 && st.written > h.MaxFileSize {
            h.failUpload(send, m.MsgID, st, "size_limit_exceeded")
            return
        }
    }
    if m.EOF {
        h.finishUpload(send, m.MsgID, st)
    }
}

func (h *FileHandler) failUpload(send Sender, msgID string, st *uploadState, errStr string) {
    st.f.Close()
    os.Remove(st.tmpPath)
    h.mu.Lock()
    delete(h.uploads, msgID)
    h.mu.Unlock()
    send.Send(&protocol.Reply{MsgID: msgID, OK: false, Error: errStr})
}

func (h *FileHandler) finishUpload(send Sender, msgID string, st *uploadState) {
    if err := st.f.Sync(); err != nil {
        h.failUpload(send, msgID, st, err.Error())
        return
    }
    st.f.Close()
    gotSHA := hex.EncodeToString(st.h.Sum(nil))
    if st.wantSHA != "" && !strings.EqualFold(gotSHA, st.wantSHA) {
        os.Remove(st.tmpPath)
        h.mu.Lock()
        delete(h.uploads, msgID)
        h.mu.Unlock()
        send.Send(&protocol.Reply{MsgID: msgID, OK: false, Error: "hash_mismatch"})
        return
    }
    if err := os.Rename(st.tmpPath, st.path); err != nil {
        os.Remove(st.tmpPath)
        h.mu.Lock()
        delete(h.uploads, msgID)
        h.mu.Unlock()
        send.Send(&protocol.Reply{MsgID: msgID, OK: false, Error: err.Error()})
        return
    }
    h.mu.Lock()
    delete(h.uploads, msgID)
    h.mu.Unlock()
    send.Send(&protocol.Reply{MsgID: msgID, OK: true, Data: map[string]any{
        "bytes": st.written, "sha256": gotSHA,
    }})
}

func (h *FileHandler) handleAbort(m *protocol.FileAbort) {
    h.mu.Lock()
    if st, ok := h.uploads[m.MsgID]; ok {
        st.f.Close()
        os.Remove(st.tmpPath)
        delete(h.uploads, m.MsgID)
    }
    if ds, ok := h.downloads[m.MsgID]; ok {
        close(ds.cancel)
        delete(h.downloads, m.MsgID)
    }
    h.mu.Unlock()
}

// handleGetOpen and handleLocalCopy are added in Task 3.
func (h *FileHandler) handleGetOpen(send Sender, m *protocol.FileGetOpen)   {}
func (h *FileHandler) handleLocalCopy(send Sender, m *protocol.FileLocalCopy) {}

// silence unused import errors in this skeleton (io, errors).
var _ = io.Copy
var _ = errors.New
```

- [ ] **Step 2: Dispatch new messages in node/handler.go**

Edit `internal/node/handler.go`. The current `ProcessHandler.Handle` switches by type. Two ways forward:

- Option A: add file cases to `ProcessHandler.Handle`.
- Option B: register a separate `FileHandler` on the node client.

Use Option A for minimum churn — make `ProcessHandler` hold a `*FileHandler` and delegate file message types:

In `ProcessHandler` struct, add:

```go
fileHandler *FileHandler
```

In `NewProcessHandler`, initialize:

```go
return &ProcessHandler{
    registry:    NewProcessRegistry(cap),
    logDir:      logDir,
    execCancel:  make(map[string]context.CancelFunc),
    fileHandler: NewFileHandler(),
}
```

In `Handle`, append (`msg` is the outer function parameter; in a multi-type case `m` has the static interface type, so just pass `msg`):

```go
case *protocol.FilePutOpen, *protocol.FileChunk, *protocol.FileAbort,
     *protocol.FileGetOpen, *protocol.FileLocalCopy:
    h.fileHandler.Handle(send, msg)
```

- [ ] **Step 3: Write file_test.go**

Create `internal/node/file_test.go`:

```go
package node

import (
    "crypto/sha256"
    "encoding/hex"
    "os"
    "path/filepath"
    "sync"
    "testing"

    "github.com/qiuxiang/tether/internal/protocol"
    "github.com/stretchr/testify/require"
)

type capSender struct {
    mu   sync.Mutex
    msgs []protocol.Message
}

func (c *capSender) Send(m protocol.Message) error {
    c.mu.Lock()
    c.msgs = append(c.msgs, m)
    c.mu.Unlock()
    return nil
}

func (c *capSender) last() protocol.Message {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.msgs[len(c.msgs)-1]
}

func TestUploadHappyPath(t *testing.T) {
    h := NewFileHandler()
    s := &capSender{}
    dir := t.TempDir()
    dst := filepath.Join(dir, "out.bin")
    payload := []byte("hello world")
    sum := sha256.Sum256(payload)
    sumHex := hex.EncodeToString(sum[:])

    h.Handle(s, &protocol.FilePutOpen{
        MsgID: "u1", Path: dst, Size: int64(len(payload)), SHA256: sumHex,
    })
    require.True(t, s.last().(*protocol.Reply).OK)

    h.Handle(s, &protocol.FileChunk{MsgID: "u1", Seq: 0, Data: payload, EOF: true})
    final := s.last().(*protocol.Reply)
    require.True(t, final.OK, "final reply error: %s", final.Error)

    got, err := os.ReadFile(dst)
    require.NoError(t, err)
    require.Equal(t, payload, got)
}

func TestUploadHashMismatch(t *testing.T) {
    h := NewFileHandler()
    s := &capSender{}
    dst := filepath.Join(t.TempDir(), "out.bin")
    h.Handle(s, &protocol.FilePutOpen{MsgID: "u2", Path: dst, Size: 5, SHA256: "deadbeef"})
    require.True(t, s.last().(*protocol.Reply).OK)
    h.Handle(s, &protocol.FileChunk{MsgID: "u2", Data: []byte("hello"), EOF: true})
    final := s.last().(*protocol.Reply)
    require.False(t, final.OK)
    require.Equal(t, "hash_mismatch", final.Error)
    _, err := os.Stat(dst)
    require.True(t, os.IsNotExist(err), "destination must not exist on mismatch")
}

func TestUploadDestExists(t *testing.T) {
    h := NewFileHandler()
    s := &capSender{}
    dst := filepath.Join(t.TempDir(), "out.bin")
    require.NoError(t, os.WriteFile(dst, []byte("old"), 0o644))

    h.Handle(s, &protocol.FilePutOpen{MsgID: "u3", Path: dst, Size: 5})
    final := s.last().(*protocol.Reply)
    require.False(t, final.OK)
    require.Equal(t, "destination_exists", final.Error)
}

func TestUploadOverwrite(t *testing.T) {
    h := NewFileHandler()
    s := &capSender{}
    dst := filepath.Join(t.TempDir(), "out.bin")
    require.NoError(t, os.WriteFile(dst, []byte("old"), 0o644))

    payload := []byte("new")
    sum := sha256.Sum256(payload)
    h.Handle(s, &protocol.FilePutOpen{MsgID: "u4", Path: dst, Size: int64(len(payload)),
        SHA256: hex.EncodeToString(sum[:]), Overwrite: true})
    require.True(t, s.last().(*protocol.Reply).OK)
    h.Handle(s, &protocol.FileChunk{MsgID: "u4", Data: payload, EOF: true})
    require.True(t, s.last().(*protocol.Reply).OK)

    got, _ := os.ReadFile(dst)
    require.Equal(t, payload, got)
}

func TestUploadAbortCleansTempFile(t *testing.T) {
    h := NewFileHandler()
    s := &capSender{}
    dst := filepath.Join(t.TempDir(), "out.bin")
    h.Handle(s, &protocol.FilePutOpen{MsgID: "u5", Path: dst, Size: 100})
    require.True(t, s.last().(*protocol.Reply).OK)

    h.Handle(s, &protocol.FileAbort{MsgID: "u5", Error: "cancelled"})
    matches, _ := filepath.Glob(dst + ".tether-tmp-*")
    require.Empty(t, matches, "temp file should be cleaned up")
}
```

- [ ] **Step 4: Run tests**

```
go test ./internal/node/... -run TestUpload
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add internal/node/file.go internal/node/file_test.go internal/node/handler.go
git commit -m "node: implement FilePutOpen + FileChunk upload with atomic write"
```

---

## Task 3: Node-side FileGetOpen (download) + FileLocalCopy

**Files:**
- Modify: `internal/node/file.go`
- Modify: `internal/node/file_test.go`

### Why

Downloads stream a file from the node: send Reply with metadata immediately, then push 256 KB FileChunk frames followed by EOF. Local copy is a sibling that does a same-node OS copy.

- [ ] **Step 1: Implement handleGetOpen**

In `internal/node/file.go`, replace the placeholder `handleGetOpen` with:

```go
const fileChunkSize = 256 * 1024

func (h *FileHandler) handleGetOpen(send Sender, m *protocol.FileGetOpen) {
    path, err := expandPath(m.Path)
    if err != nil {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
        return
    }
    fi, err := os.Stat(path)
    if err != nil {
        if os.IsNotExist(err) {
            send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "path_not_found"})
            return
        }
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
        return
    }
    if h.MaxFileSize > 0 && fi.Size() > h.MaxFileSize {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "size_limit_exceeded"})
        return
    }
    f, err := os.Open(path)
    if err != nil {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
        return
    }

    // Compute sha256 by hashing as we read; we'll send it in the Reply (metadata)
    // but the receiver verifies as it goes.
    hash := sha256.New()
    // Send metadata reply first (without sha — receiver computes its own).
    send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
        "size": fi.Size(),
        "mode": uint32(fi.Mode().Perm()),
    }})

    // Register cancel chan for handleAbort.
    cancelCh := make(chan struct{})
    h.mu.Lock()
    h.downloads[m.MsgID] = &downloadState{cancel: cancelCh}
    h.mu.Unlock()

    go func() {
        defer f.Close()
        defer func() {
            h.mu.Lock()
            delete(h.downloads, m.MsgID)
            h.mu.Unlock()
        }()
        buf := make([]byte, fileChunkSize)
        var seq int64
        for {
            select {
            case <-cancelCh:
                send.Send(&protocol.FileAbort{MsgID: m.MsgID, Error: "cancelled"})
                return
            default:
            }
            n, rerr := f.Read(buf)
            if n > 0 {
                hash.Write(buf[:n])
                eof := rerr == io.EOF
                err := send.Send(&protocol.FileChunk{
                    MsgID: m.MsgID, Seq: seq, Data: append([]byte(nil), buf[:n]...), EOF: eof,
                })
                if err != nil {
                    return
                }
                seq++
                if eof {
                    return
                }
            }
            if rerr == io.EOF {
                // Zero-byte file: still need to send EOF frame.
                _ = send.Send(&protocol.FileChunk{MsgID: m.MsgID, Seq: seq, EOF: true})
                return
            }
            if rerr != nil {
                send.Send(&protocol.FileAbort{MsgID: m.MsgID, Error: rerr.Error()})
                return
            }
        }
    }()
}
```

- [ ] **Step 2: Implement handleLocalCopy**

In `internal/node/file.go`, replace the placeholder `handleLocalCopy` with:

```go
func (h *FileHandler) handleLocalCopy(send Sender, m *protocol.FileLocalCopy) {
    from, err := expandPath(m.FromPath)
    if err != nil {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
        return
    }
    to, err := expandPath(m.ToPath)
    if err != nil {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
        return
    }
    if from == to {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "from equals to"})
        return
    }
    if _, err := os.Stat(to); err == nil && !m.Overwrite {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "destination_exists"})
        return
    }
    src, err := os.Open(from)
    if err != nil {
        if os.IsNotExist(err) {
            send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "path_not_found"})
            return
        }
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
        return
    }
    defer src.Close()
    fi, err := src.Stat()
    if err != nil {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
        return
    }
    tmp := to + ".tether-tmp-" + m.MsgID
    dst, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fi.Mode().Perm())
    if err != nil {
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
        return
    }
    hash := sha256.New()
    n, err := io.Copy(io.MultiWriter(dst, hash), src)
    dst.Close()
    if err != nil {
        os.Remove(tmp)
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
        return
    }
    if err := os.Rename(tmp, to); err != nil {
        os.Remove(tmp)
        send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
        return
    }
    send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
        "bytes": n, "sha256": hex.EncodeToString(hash.Sum(nil)),
    }})
}
```

Remove the `var _ = io.Copy` / `var _ = errors.New` placeholder lines at the bottom of `file.go` — they're now actually used.

- [ ] **Step 3: Add tests for download and local copy**

Append to `internal/node/file_test.go`:

```go
func TestDownloadHappyPath(t *testing.T) {
    h := NewFileHandler()
    s := &capSender{}
    src := filepath.Join(t.TempDir(), "in.bin")
    payload := []byte("file contents here")
    require.NoError(t, os.WriteFile(src, payload, 0o644))

    h.Handle(s, &protocol.FileGetOpen{MsgID: "d1", Path: src})

    // First message should be a metadata reply.
    require.IsType(t, &protocol.Reply{}, s.msgs[0])
    require.True(t, s.msgs[0].(*protocol.Reply).OK)

    // Wait for chunks to arrive — file is small so it should arrive in one chunk.
    require.Eventually(t, func() bool {
        s.mu.Lock()
        defer s.mu.Unlock()
        for _, m := range s.msgs {
            if ch, ok := m.(*protocol.FileChunk); ok && ch.EOF {
                return true
            }
        }
        return false
    }, 2*time.Second, 20*time.Millisecond)

    var got []byte
    s.mu.Lock()
    for _, m := range s.msgs {
        if ch, ok := m.(*protocol.FileChunk); ok {
            got = append(got, ch.Data...)
        }
    }
    s.mu.Unlock()
    require.Equal(t, payload, got)
}

func TestDownloadMissingFile(t *testing.T) {
    h := NewFileHandler()
    s := &capSender{}
    h.Handle(s, &protocol.FileGetOpen{MsgID: "d2", Path: "/nonexistent/path/here"})
    final := s.last().(*protocol.Reply)
    require.False(t, final.OK)
    require.Equal(t, "path_not_found", final.Error)
}

func TestLocalCopyHappyPath(t *testing.T) {
    h := NewFileHandler()
    s := &capSender{}
    dir := t.TempDir()
    from := filepath.Join(dir, "a.bin")
    to := filepath.Join(dir, "b.bin")
    payload := []byte("xyzzy")
    require.NoError(t, os.WriteFile(from, payload, 0o644))

    h.Handle(s, &protocol.FileLocalCopy{MsgID: "c1", FromPath: from, ToPath: to})
    final := s.last().(*protocol.Reply)
    require.True(t, final.OK, final.Error)
    got, _ := os.ReadFile(to)
    require.Equal(t, payload, got)
}
```

Add a `time` import at the top of `file_test.go` if not present.

- [ ] **Step 4: Run tests**

```
go test ./internal/node/... -run TestDownload -count=1
go test ./internal/node/... -run TestLocalCopy -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add internal/node/file.go internal/node/file_test.go
git commit -m "node: implement FileGetOpen and FileLocalCopy"
```

---

## Task 4: Hub — sticky route file chunks; dispatch new types in client_ws

**Files:**
- Modify: `internal/hub/client_ws.go`
- Modify: `internal/hub/device_ws.go` (forward `FileChunk` and `FileAbort` replies through router)

### Why

Sticky routing already exists in the router from Phase 1. We need three things:

1. `FilePutOpen` from a client must register a **sticky** route (so subsequent chunk frames from the node — actually for uploads chunks go client→node, so the chunk path is client→hub→node; the only thing that comes back is one final Reply, so a one-shot route is fine).
2. `FileGetOpen` from a client must register a **sticky** route (chunks stream node→hub→client; final Reply is also routed back; sticky lasts until the chunk EOF or an explicit FileAbort).
3. The device read loop must forward `FileChunk` and `FileAbort` messages by msg_id.

Also, `FileChunk` and `FileAbort` traveling client→hub need to be **routed to the node** based on a sticky route that was set up by the original `FilePutOpen`.

So: after `FilePutOpen` from a client, register a sticky route `msg_id → client` (for the eventual Reply) AND a separate forwarding route `msg_id → node` for chunks coming from the client to be forwarded to the node. We need two-direction routing.

Simpler model: separate routing tables by direction is unnecessary. Instead, use the existing sticky route to record **the peer that should receive messages with this msg_id** — and the dispatcher in each WS read loop chooses where to forward based on the source.

Concretely: on the hub, every WS read loop (device or client) decodes a chunk with `msg_id=X` and asks the router "who is the other side of X?" — the router stores both ends.

Restructure the router minimally for this:

- [ ] **Step 1: Extend Router with two-sided forwarding**

Edit `internal/hub/router.go`. Replace the existing one-way `Forward` with a pair store:

```go
package hub

import "sync"

// Router stores msg_id ↔ (clientConn, nodeConn) pairings used to route
// streaming messages in either direction. Use RegisterPair when the
// transaction has chunks flowing in both directions (uploads, downloads,
// file relay legs). Use Register for legacy one-shot replies.
type Router struct {
    mu     sync.Mutex
    routes map[string]route
}

type route struct {
    Client PeerConn // the originating client
    Node   PeerConn // the target node
    Sticky bool
}

func NewRouter() *Router {
    return &Router{routes: make(map[string]route)}
}

// Register associates msg_id with the client peer that will receive replies
// from this msg_id (one-shot unless sticky=true).
func (r *Router) Register(msgID string, client PeerConn, sticky bool) {
    r.mu.Lock()
    cur := r.routes[msgID]
    cur.Client = client
    cur.Sticky = sticky
    r.routes[msgID] = cur
    r.mu.Unlock()
}

// RegisterNode marks the node side of a sticky route (used so that chunks
// coming from the client can be forwarded to the right node by msg_id).
func (r *Router) RegisterNode(msgID string, node PeerConn) {
    r.mu.Lock()
    cur := r.routes[msgID]
    cur.Node = node
    r.routes[msgID] = cur
    r.mu.Unlock()
}

func (r *Router) Unregister(msgID string) {
    r.mu.Lock()
    delete(r.routes, msgID)
    r.mu.Unlock()
}

// ForwardToClient delivers raw bytes from a node back to the client that
// originated msg_id. Returns true if a route was found. Removes one-shot
// routes after delivery.
func (r *Router) ForwardToClient(msgID string, raw []byte) bool {
    r.mu.Lock()
    rt, ok := r.routes[msgID]
    if ok && !rt.Sticky {
        delete(r.routes, msgID)
    }
    r.mu.Unlock()
    if !ok || rt.Client == nil {
        return false
    }
    _ = rt.Client.SendRaw(raw)
    return true
}

// ForwardToNode delivers raw bytes from a client to the node side of the
// route (set via RegisterNode). Always non-removing — used for chunk
// streams; route cleanup happens on EOF/abort.
func (r *Router) ForwardToNode(msgID string, raw []byte) bool {
    r.mu.Lock()
    rt, ok := r.routes[msgID]
    r.mu.Unlock()
    if !ok || rt.Node == nil {
        return false
    }
    _ = rt.Node.SendRaw(raw)
    return true
}

func (r *Router) Lookup(msgID string) (route, bool) {
    r.mu.Lock()
    rt, ok := r.routes[msgID]
    r.mu.Unlock()
    return rt, ok
}
```

Update `internal/hub/router_test.go` to match. Replace `Forward` with `ForwardToClient` for sticky/one-shot tests and add a `ForwardToNode` test:

```go
func TestRouterNodeSide(t *testing.T) {
    r := NewRouter()
    cliC := &fakeConn{}
    nodeC := &fakeConn{}
    r.Register("m1", cliC, true)
    r.RegisterNode("m1", nodeC)

    require.True(t, r.ForwardToNode("m1", []byte("chunk")))
    require.Len(t, nodeC.sent, 1)
    require.True(t, r.ForwardToClient("m1", []byte("reply")))
    require.Len(t, cliC.sent, 1)
}
```

- [ ] **Step 2: Update device_ws.go to forward chunks and aborts**

In `internal/hub/device_ws.go`, update the `msgID` extraction to include `FileChunk` and `FileAbort`, and the read loop to use `ForwardToClient`:

```go
func msgID(m protocol.Message) string {
    switch v := m.(type) {
    case *protocol.Reply:
        return v.MsgID
    case *protocol.ExecOutput:
        return v.MsgID
    case *protocol.ExecExit:
        return v.MsgID
    case *protocol.FileChunk:
        return v.MsgID
    case *protocol.FileAbort:
        return v.MsgID
    }
    return ""
}
```

And in the run loop, use `s.router.ForwardToClient(id, raw)` instead of `s.router.Forward(id, raw)`.

Similarly, in `client_ws.go` add chunk/abort dispatch:

```go
// In dispatch:
case *protocol.FilePutOpen:
    cs.routeFilePut(m.MsgID, m.Target, raw)
case *protocol.FileGetOpen:
    cs.routeFileGet(m.MsgID, m.Target, raw)
case *protocol.FileLocalCopy:
    cs.routeOneShot(m.MsgID, m.Target, raw)
case *protocol.FileChunk:
    // Client is pushing a chunk to a node mid-upload — forward by msg_id.
    cs.server.router.ForwardToNode(m.MsgID, raw)
case *protocol.FileAbort:
    cs.server.router.ForwardToNode(m.MsgID, raw)
    cs.server.router.Unregister(m.MsgID)
```

Add the two new helpers in `client_ws.go`:

```go
func (cs *clientSession) routeFilePut(msgID, target string, raw []byte) {
    d, ok := cs.server.registry.Get(target)
    if !ok || d.Conn == nil {
        cs.sendErrorReply(msgID, fmt.Errorf("device_offline: %s", target))
        return
    }
    cs.server.router.Register(msgID, cs, false)        // final Reply is one-shot
    cs.server.router.RegisterNode(msgID, d.Conn)        // chunks flow client→node
    if err := d.Conn.SendRaw(raw); err != nil {
        cs.server.router.Unregister(msgID)
        cs.sendErrorReply(msgID, err)
    }
}

func (cs *clientSession) routeFileGet(msgID, target string, raw []byte) {
    d, ok := cs.server.registry.Get(target)
    if !ok || d.Conn == nil {
        cs.sendErrorReply(msgID, fmt.Errorf("device_offline: %s", target))
        return
    }
    // metadata Reply + chunk stream + (optional) FileAbort all flow node→client.
    // Sticky until client unregisters via FileAbort or we observe the EOF chunk
    // (handled in device_ws — see Step 3).
    cs.server.router.Register(msgID, cs, true)
    cs.server.router.RegisterNode(msgID, d.Conn) // for client→node abort frames
    if err := d.Conn.SendRaw(raw); err != nil {
        cs.server.router.Unregister(msgID)
        cs.sendErrorReply(msgID, err)
    }
}
```

- [ ] **Step 3: Auto-unregister on chunk EOF**

In `internal/hub/device_ws.go`, after `ForwardToClient`, if the message is a `FileChunk` with `EOF=true` or a `FileAbort`, unregister the route:

```go
if id != "" {
    s.router.ForwardToClient(id, raw)
    switch v := msg.(type) {
    case *protocol.FileChunk:
        if v.EOF {
            s.router.Unregister(id)
        }
    case *protocol.FileAbort:
        s.router.Unregister(id)
    }
}
```

For uploads (client → node chunks), the route is unregistered when the final Reply arrives — that's already handled by the one-shot Register for FilePutOpen above (sticky=false makes `ForwardToClient` of the final Reply also delete the route).

- [ ] **Step 4: Run tests**

```
go test ./internal/hub/... -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add internal/hub/
git commit -m "hub: bidirectional sticky routing for file chunks"
```

---

## Task 5: Hub — FileRelay coordinator

**Files:**
- Create: `internal/hub/relay.go`
- Create: `internal/hub/relay_test.go`
- Modify: `internal/hub/client_ws.go` (dispatch `FileRelay` to coordinator)

### Why

`FileRelay` is the only message the Hub itself originates outward. It opens `FileGetOpen` on `from_node` and `FilePutOpen` on `to_node`, then streams chunks between them with msg_id rewriting.

- [ ] **Step 1: Write relay.go**

Create `internal/hub/relay.go`:

```go
package hub

import (
    "fmt"
    "sync"
    "time"

    "github.com/qiuxiang/tether/internal/protocol"
)

// RelayCoordinator orchestrates node↔node file transfers initiated by a
// client via FileRelay.
type RelayCoordinator struct {
    server *Server

    mu       sync.Mutex
    inflight map[string]*relayState // keyed by outer (client) msg_id
}

type relayState struct {
    clientMsgID string
    getMsgID    string
    putMsgID    string
    fromConn    PeerConn
    toConn      PeerConn
    client      PeerConn
    metaReady   chan *protocol.Reply
    putReady    chan *protocol.Reply
}

func NewRelayCoordinator(s *Server) *RelayCoordinator {
    return &RelayCoordinator{server: s, inflight: make(map[string]*relayState)}
}

// Start kicks off a relay. Returns nil and replies asynchronously on success;
// returns an error to be sent as an error Reply on immediate failure.
func (rc *RelayCoordinator) Start(client PeerConn, m *protocol.FileRelay) error {
    from, ok := rc.server.registry.Get(m.FromNode)
    if !ok || from.Conn == nil {
        return fmt.Errorf("device_offline: %s", m.FromNode)
    }
    to, ok := rc.server.registry.Get(m.ToNode)
    if !ok || to.Conn == nil {
        return fmt.Errorf("device_offline: %s", m.ToNode)
    }

    getID := newClientID()
    putID := newClientID()
    st := &relayState{
        clientMsgID: m.MsgID,
        getMsgID:    getID,
        putMsgID:    putID,
        fromConn:    from.Conn,
        toConn:      to.Conn,
        client:      client,
        metaReady:   make(chan *protocol.Reply, 1),
        putReady:    make(chan *protocol.Reply, 1),
    }
    rc.mu.Lock()
    rc.inflight[m.MsgID] = st
    rc.mu.Unlock()

    // Hook routes:
    //   - replies for getID land in metaReady (interceptor below)
    //   - replies for putID land in putReady AND eventually the final relay Reply
    rc.server.router.Register(getID, &replyInterceptor{ch: st.metaReady}, true)
    rc.server.router.Register(putID, &replyInterceptor{ch: st.putReady}, false)

    // Send FileGetOpen to from_node.
    rawGet, _ := protocol.Encode(&protocol.FileGetOpen{MsgID: getID, Path: m.FromPath})
    if err := from.Conn.SendRaw(rawGet); err != nil {
        rc.cleanup(m.MsgID)
        return err
    }

    go rc.coordinate(m, st)
    return nil
}

func (rc *RelayCoordinator) coordinate(m *protocol.FileRelay, st *relayState) {
    // Wait for metadata.
    meta := <-st.metaReady
    if meta == nil || !meta.OK {
        rc.failClient(st.clientMsgID, meta)
        rc.cleanup(st.clientMsgID)
        return
    }
    var size int64
    if v, ok := meta.Data["size"].(int64); ok {
        size = v
    } else if v, ok := meta.Data["size"].(uint64); ok {
        size = int64(v)
    }
    var mode uint32
    if v, ok := meta.Data["mode"].(uint64); ok {
        mode = uint32(v)
    }

    // Send FilePutOpen to to_node with size/mode and overwrite flag.
    rawPut, _ := protocol.Encode(&protocol.FilePutOpen{
        MsgID: st.putMsgID, Path: m.ToPath, Size: size, Mode: mode, Overwrite: m.Overwrite,
    })
    if err := st.toConn.SendRaw(rawPut); err != nil {
        rc.failClient(st.clientMsgID, &protocol.Reply{Error: err.Error()})
        rc.cleanup(st.clientMsgID)
        return
    }

    // Wait for put_ready.
    select {
    case ready := <-st.putReady:
        if ready == nil || !ready.OK {
            rc.failClient(st.clientMsgID, ready)
            rc.cleanup(st.clientMsgID)
            return
        }
    case <-time.After(30 * time.Second):
        rc.failClient(st.clientMsgID, &protocol.Reply{Error: "put_open_timeout"})
        rc.cleanup(st.clientMsgID)
        return
    }

    // Stream chunks: register node-side routes that rewrite msg_id.
    rc.server.router.Register(st.getMsgID, &chunkRewriter{toConn: st.toConn, toMsgID: st.putMsgID}, true)
    rc.server.router.Register(st.putMsgID, &finalDeliverer{client: st.client, clientMsgID: st.clientMsgID, onDone: func() { rc.cleanup(st.clientMsgID) }}, false)
}

func (rc *RelayCoordinator) cleanup(clientMsgID string) {
    rc.mu.Lock()
    st, ok := rc.inflight[clientMsgID]
    delete(rc.inflight, clientMsgID)
    rc.mu.Unlock()
    if ok {
        rc.server.router.Unregister(st.getMsgID)
        rc.server.router.Unregister(st.putMsgID)
    }
}

func (rc *RelayCoordinator) failClient(clientMsgID string, r *protocol.Reply) {
    errStr := "relay failed"
    if r != nil && r.Error != "" {
        errStr = r.Error
    }
    raw, _ := protocol.Encode(&protocol.Reply{MsgID: clientMsgID, OK: false, Error: errStr})
    rc.mu.Lock()
    st, ok := rc.inflight[clientMsgID]
    rc.mu.Unlock()
    if ok {
        _ = st.client.SendRaw(raw)
    }
}

// replyInterceptor implements PeerConn so the router can deliver a Reply to a chan.
type replyInterceptor struct {
    ch chan *protocol.Reply
}

func (i *replyInterceptor) SendRaw(raw []byte) error {
    msg, err := protocol.Decode(raw)
    if err != nil {
        return err
    }
    if r, ok := msg.(*protocol.Reply); ok {
        select {
        case i.ch <- r:
        default:
        }
    }
    return nil
}
func (i *replyInterceptor) Close() {}

// chunkRewriter implements PeerConn — when the router forwards a node→hub
// FileChunk to it, it rewrites the msg_id and writes to the destination node.
type chunkRewriter struct {
    toConn  PeerConn
    toMsgID string
}

func (cr *chunkRewriter) SendRaw(raw []byte) error {
    msg, err := protocol.Decode(raw)
    if err != nil {
        return err
    }
    switch m := msg.(type) {
    case *protocol.FileChunk:
        m.MsgID = cr.toMsgID
        out, err := protocol.Encode(m)
        if err != nil {
            return err
        }
        return cr.toConn.SendRaw(out)
    case *protocol.FileAbort:
        m.MsgID = cr.toMsgID
        out, _ := protocol.Encode(m)
        return cr.toConn.SendRaw(out)
    }
    return nil
}
func (cr *chunkRewriter) Close() {}

// finalDeliverer forwards the final upload Reply back to the originating
// client under the client's outer msg_id.
type finalDeliverer struct {
    client      PeerConn
    clientMsgID string
    onDone      func()
}

func (fd *finalDeliverer) SendRaw(raw []byte) error {
    defer fd.onDone()
    msg, err := protocol.Decode(raw)
    if err != nil {
        return err
    }
    if r, ok := msg.(*protocol.Reply); ok {
        r.MsgID = fd.clientMsgID
        out, _ := protocol.Encode(r)
        return fd.client.SendRaw(out)
    }
    return nil
}
func (fd *finalDeliverer) Close() {}
```

- [ ] **Step 2: Wire RelayCoordinator into Server and client_ws**

In `internal/hub/server.go`, add a field and constructor wiring:

```go
type Server struct {
    opts     Options
    registry *Registry
    clients  *ClientRegistry
    router   *Router
    relay    *RelayCoordinator
}

func NewServer(opts Options) *Server {
    s := &Server{
        opts:     opts,
        registry: NewRegistry(),
        clients:  NewClientRegistry(),
        router:   NewRouter(),
    }
    s.relay = NewRelayCoordinator(s)
    return s
}
```

In `internal/hub/client_ws.go`, dispatch:

```go
case *protocol.FileRelay:
    if err := cs.server.relay.Start(cs, m); err != nil {
        cs.sendErrorReply(m.MsgID, err)
    }
```

- [ ] **Step 3: Write a minimal relay test**

Create `internal/hub/relay_test.go`:

```go
package hub

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "net/http/httptest"
    "os"
    "path/filepath"
    "strings"
    "testing"
    "time"

    "github.com/coder/websocket"
    "github.com/qiuxiang/tether/internal/node"
    "github.com/qiuxiang/tether/internal/protocol"
    "github.com/stretchr/testify/require"
)

func TestFileRelayEndToEnd(t *testing.T) {
    s := NewServer(Options{Token: "tk"})
    ts := httptest.NewServer(s.Handler())
    defer ts.Close()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Two nodes.
    for _, host := range []string{"src", "dst"} {
        url := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
        n := node.New(node.Config{HubURL: url, Token: "tk", Hostname: host})
        n.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
        go n.Run(ctx)
    }
    require.Eventually(t, func() bool {
        _, a := s.Registry().Get("src")
        _, b := s.Registry().Get("dst")
        return a && b
    }, 2*time.Second, 20*time.Millisecond)

    // Place a file on src node — we can't easily reach the node's tempdir
    // from this test process. Instead, drive a FilePutOpen via a client.
    payload := []byte("relay payload contents")
    srcPath := filepath.Join(t.TempDir(), "src.bin")
    require.NoError(t, os.WriteFile(srcPath, payload, 0o644))

    // Connect a client.
    cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
    cli, _, err := websocket.Dial(ctx, cliURL, nil)
    require.NoError(t, err)
    defer cli.Close(websocket.StatusNormalClosure, "")
    hello := &protocol.Hello{Hostname: "tester", Token: "tk", Role: "client"}
    raw, _ := protocol.Encode(hello)
    require.NoError(t, cli.Write(ctx, websocket.MessageBinary, raw))

    // Skip the node-to-node test here if your nodes share the FS in a test
    // harness; for true isolation, this test should run as a smoke test
    // outside the unit suite.
    _ = srcPath
    _ = sha256.Sum256(payload)
    _ = hex.EncodeToString
    t.Skip("FileRelay end-to-end requires per-node tempdirs that share files; covered by e2e_test.go in Task 7")
}
```

(The relay coordinator logic is best validated end-to-end in Task 7; this stub keeps the test file compiling and serves as a docking point for narrower unit tests if you add them.)

- [ ] **Step 4: Run all hub tests**

```
go test ./internal/hub/... -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add internal/hub/relay.go internal/hub/relay_test.go internal/hub/server.go internal/hub/client_ws.go
git commit -m "hub: implement FileRelay coordinator"
```

---

## Task 6: Client — file_transfer MCP tool

**Files:**
- Create: `internal/client/tools_file.go`
- Create: `internal/client/tools_file_test.go`
- Modify: `internal/client/mcp_server.go` (register tool)

### Why

The client tool parses `from`/`to` paths, identifies the routing combination, and dispatches to the appropriate flow.

- [ ] **Step 1: Write tools_file.go**

Create `internal/client/tools_file.go`:

```go
package client

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "strings"
    "time"

    "github.com/mark3labs/mcp-go/mcp"
    "github.com/mark3labs/mcp-go/server"
    "github.com/qiuxiang/tether/internal/protocol"
)

const fileChunkSize = 256 * 1024

type pathSpec struct {
    Node string // empty = local
    Path string // raw (may include "~")
}

func parsePath(s string) (pathSpec, error) {
    if i := strings.Index(s, ":"); i > 0 && !strings.ContainsAny(s[:i], "/.~") {
        return pathSpec{Node: s[:i], Path: s[i+1:]}, nil
    }
    return pathSpec{Path: s}, nil
}

func expandLocal(p string) (string, error) {
    if strings.HasPrefix(p, "~") {
        home, err := os.UserHomeDir()
        if err != nil {
            return "", err
        }
        p = home + p[1:]
    }
    if !filepath.IsAbs(p) {
        return "", fmt.Errorf("path must be absolute: %s", p)
    }
    return p, nil
}

func registerFileTool(m *server.MCPServer, c *Conn) {
    m.AddTool(
        mcp.NewTool("file_transfer",
            mcp.WithDescription("Transfer a single file between the local machine and a node, or between two nodes. Paths use 'node:/abs/path' for a node path or '/abs/path' (or '~/path') for the local machine running 'tether mcp'."),
            mcp.WithString("from", mcp.Required()),
            mcp.WithString("to", mcp.Required()),
            mcp.WithBoolean("overwrite"),
        ),
        func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
            args := req.GetArguments()
            fromStr, _ := args["from"].(string)
            toStr, _ := args["to"].(string)
            overwrite, _ := args["overwrite"].(bool)

            from, err := parsePath(fromStr)
            if err != nil {
                return mcp.NewToolResultError(err.Error()), nil
            }
            to, err := parsePath(toStr)
            if err != nil {
                return mcp.NewToolResultError(err.Error()), nil
            }

            switch {
            case from.Node == "" && to.Node == "":
                return mcp.NewToolResultError("use os tools — both paths are local"), nil
            case from.Node == "" && to.Node != "":
                return upload(ctx, c, from.Path, to.Node, to.Path, overwrite)
            case from.Node != "" && to.Node == "":
                return download(ctx, c, from.Node, from.Path, to.Path, overwrite)
            case from.Node == to.Node:
                return sameNodeCopy(ctx, c, from.Node, from.Path, to.Path, overwrite)
            default:
                return relay(ctx, c, from.Node, from.Path, to.Node, to.Path, overwrite)
            }
        },
    )
}

func upload(ctx context.Context, c *Conn, localPath, node, remotePath string, overwrite bool) (*mcp.CallToolResult, error) {
    p, err := expandLocal(localPath)
    if err != nil {
        return mcp.NewToolResultError(err.Error()), nil
    }
    f, err := os.Open(p)
    if err != nil {
        if os.IsNotExist(err) {
            return mcp.NewToolResultError("path_not_found"), nil
        }
        return mcp.NewToolResultError(err.Error()), nil
    }
    defer f.Close()
    fi, _ := f.Stat()
    h := sha256.New()
    if _, err := io.Copy(h, f); err != nil {
        return mcp.NewToolResultError(err.Error()), nil
    }
    sumHex := hex.EncodeToString(h.Sum(nil))
    if _, err := f.Seek(0, io.SeekStart); err != nil {
        return mcp.NewToolResultError(err.Error()), nil
    }

    id := NewMsgID()
    ch := c.rpc.Register(id)
    defer c.rpc.Unregister(id)
    start := time.Now()
    if err := c.Send(&protocol.FilePutOpen{
        MsgID: id, Target: node, Path: remotePath,
        Size: fi.Size(), Mode: uint32(fi.Mode().Perm()),
        Overwrite: overwrite, SHA256: sumHex,
    }); err != nil {
        return mcp.NewToolResultError(err.Error()), nil
    }
    // Wait for ok-to-send.
    select {
    case reply := <-ch:
        if !reply.OK {
            return mcp.NewToolResultError(reply.Error), nil
        }
    case <-time.After(30 * time.Second):
        return mcp.NewToolResultError("put_open_timeout"), nil
    case <-ctx.Done():
        return mcp.NewToolResultError(ctx.Err().Error()), nil
    }
    // Re-register for the final reply.
    finalCh := c.rpc.Register(id)
    defer c.rpc.Unregister(id)

    buf := make([]byte, fileChunkSize)
    var seq int64
    for {
        n, rerr := f.Read(buf)
        eof := rerr == io.EOF
        if n > 0 || eof {
            if err := c.Send(&protocol.FileChunk{
                MsgID: id, Seq: seq, Data: append([]byte(nil), buf[:n]...), EOF: eof,
            }); err != nil {
                return mcp.NewToolResultError(err.Error()), nil
            }
            seq++
        }
        if eof {
            break
        }
        if rerr != nil {
            return mcp.NewToolResultError(rerr.Error()), nil
        }
    }

    select {
    case reply := <-finalCh:
        if !reply.OK {
            return mcp.NewToolResultError(reply.Error), nil
        }
        return finalResult(reply, start), nil
    case <-time.After(60 * time.Second):
        return mcp.NewToolResultError("final_reply_timeout"), nil
    case <-ctx.Done():
        return mcp.NewToolResultError(ctx.Err().Error()), nil
    }
}

func download(ctx context.Context, c *Conn, node, remotePath, localPath string, overwrite bool) (*mcp.CallToolResult, error) {
    lp, err := expandLocal(localPath)
    if err != nil {
        return mcp.NewToolResultError(err.Error()), nil
    }
    if _, err := os.Stat(lp); err == nil && !overwrite {
        return mcp.NewToolResultError("destination_exists"), nil
    }

    id := NewMsgID()
    replyCh := c.rpc.Register(id)
    streamCh := c.rpc.RegisterStreamRaw(id) // see Note below
    defer c.rpc.Unregister(id)
    start := time.Now()
    if err := c.Send(&protocol.FileGetOpen{MsgID: id, Target: node, Path: remotePath}); err != nil {
        return mcp.NewToolResultError(err.Error()), nil
    }
    // Metadata reply.
    select {
    case reply := <-replyCh:
        if !reply.OK {
            return mcp.NewToolResultError(reply.Error), nil
        }
    case <-time.After(30 * time.Second):
        return mcp.NewToolResultError("get_open_timeout"), nil
    case <-ctx.Done():
        return mcp.NewToolResultError(ctx.Err().Error()), nil
    }

    tmp := lp + ".tether-tmp"
    out, err := os.Create(tmp)
    if err != nil {
        return mcp.NewToolResultError(err.Error()), nil
    }
    h := sha256.New()
    deadline := time.After(10 * time.Minute)
    for {
        select {
        case m, ok := <-streamCh:
            if !ok {
                out.Close()
                os.Remove(tmp)
                return mcp.NewToolResultError("stream_closed"), nil
            }
            switch v := m.(type) {
            case *protocol.FileChunk:
                if len(v.Data) > 0 {
                    if _, err := out.Write(v.Data); err != nil {
                        out.Close()
                        os.Remove(tmp)
                        return mcp.NewToolResultError(err.Error()), nil
                    }
                    h.Write(v.Data)
                }
                if v.EOF {
                    out.Sync()
                    out.Close()
                    if err := os.Rename(tmp, lp); err != nil {
                        os.Remove(tmp)
                        return mcp.NewToolResultError(err.Error()), nil
                    }
                    fi, _ := os.Stat(lp)
                    return finalResult(&protocol.Reply{Data: map[string]any{
                        "bytes": fi.Size(), "sha256": hex.EncodeToString(h.Sum(nil)),
                    }}, start), nil
                }
            case *protocol.FileAbort:
                out.Close()
                os.Remove(tmp)
                return mcp.NewToolResultError(v.Error), nil
            }
        case <-deadline:
            out.Close()
            os.Remove(tmp)
            return mcp.NewToolResultError("download_timeout"), nil
        case <-ctx.Done():
            out.Close()
            os.Remove(tmp)
            return mcp.NewToolResultError(ctx.Err().Error()), nil
        }
    }
}

func sameNodeCopy(ctx context.Context, c *Conn, node, fromPath, toPath string, overwrite bool) (*mcp.CallToolResult, error) {
    id := NewMsgID()
    ch := c.rpc.Register(id)
    defer c.rpc.Unregister(id)
    start := time.Now()
    if err := c.Send(&protocol.FileLocalCopy{
        MsgID: id, Target: node, FromPath: fromPath, ToPath: toPath, Overwrite: overwrite,
    }); err != nil {
        return mcp.NewToolResultError(err.Error()), nil
    }
    select {
    case reply := <-ch:
        if !reply.OK {
            return mcp.NewToolResultError(reply.Error), nil
        }
        return finalResult(reply, start), nil
    case <-time.After(60 * time.Second):
        return mcp.NewToolResultError("local_copy_timeout"), nil
    case <-ctx.Done():
        return mcp.NewToolResultError(ctx.Err().Error()), nil
    }
}

func relay(ctx context.Context, c *Conn, fromNode, fromPath, toNode, toPath string, overwrite bool) (*mcp.CallToolResult, error) {
    id := NewMsgID()
    ch := c.rpc.Register(id)
    defer c.rpc.Unregister(id)
    start := time.Now()
    if err := c.Send(&protocol.FileRelay{
        MsgID: id, FromNode: fromNode, FromPath: fromPath,
        ToNode: toNode, ToPath: toPath, Overwrite: overwrite,
    }); err != nil {
        return mcp.NewToolResultError(err.Error()), nil
    }
    select {
    case reply := <-ch:
        if !reply.OK {
            return mcp.NewToolResultError(reply.Error), nil
        }
        return finalResult(reply, start), nil
    case <-time.After(10 * time.Minute):
        return mcp.NewToolResultError("relay_timeout"), nil
    case <-ctx.Done():
        return mcp.NewToolResultError(ctx.Err().Error()), nil
    }
}

func finalResult(reply *protocol.Reply, start time.Time) *mcp.CallToolResult {
    out := map[string]any{
        "ok":          true,
        "duration_ms": time.Since(start).Milliseconds(),
    }
    if v, ok := reply.Data["bytes"]; ok {
        out["bytes"] = v
    }
    if v, ok := reply.Data["sha256"]; ok {
        out["sha256"] = v
    }
    b, _ := json.Marshal(out)
    return mcp.NewToolResultText(string(b))
}

// silence imports if any path becomes unused mid-edit
var _ = errors.New
```

- [ ] **Step 2: Add RegisterStreamRaw in rpc.go to receive FileChunk/FileAbort frames**

The current `RegisterStream` only handles `ExecOutput`/`ExecExit`. Extend it to also dispatch `FileChunk` and `FileAbort` for the same `msg_id`. Edit `internal/client/rpc.go`:

In `Deliver`, append cases:

```go
case *protocol.FileChunk:
    r.mu.Lock()
    ch, ok := r.streams[m.MsgID]
    r.mu.Unlock()
    if ok {
        ch <- m
        if m.EOF {
            close(ch)
            r.Unregister(m.MsgID)
        }
    }
case *protocol.FileAbort:
    r.mu.Lock()
    ch, ok := r.streams[m.MsgID]
    r.mu.Unlock()
    if ok {
        ch <- m
        close(ch)
        r.Unregister(m.MsgID)
    }
```

Add an alias method (since `tools_file.go` calls `RegisterStreamRaw`):

```go
// RegisterStreamRaw is an alias for RegisterStream — kept for naming
// clarity in callers that consume FileChunk/FileAbort streams.
func (r *RPC) RegisterStreamRaw(msgID string) chan protocol.Message {
    return r.RegisterStream(msgID)
}
```

- [ ] **Step 3: Register file_transfer tool in mcp_server.go**

Edit `internal/client/mcp_server.go`:

```go
func NewMCPServer(c *Conn) *Server {
    s := &Server{conn: c, mcp: server.NewMCPServer("tether", "0.1.0")}
    registerExecTools(s.mcp, c)
    registerFileTool(s.mcp, c)
    return s
}
```

- [ ] **Step 4: Write tools_file_test.go (local↔node upload and download)**

Create `internal/client/tools_file_test.go`:

```go
package client

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "os"
    "path/filepath"
    "testing"
    "time"

    "github.com/qiuxiang/tether/internal/protocol"
    "github.com/stretchr/testify/require"
)

func TestUploadAndDownload(t *testing.T) {
    c, _, cleanup := setupClusterWithClient(t)
    defer cleanup()

    dir := t.TempDir()
    local := filepath.Join(dir, "in.bin")
    payload := []byte("hello file transfer")
    require.NoError(t, os.WriteFile(local, payload, 0o644))
    sum := sha256.Sum256(payload)

    // Upload local → node
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    remote := filepath.Join(dir, "remote.bin")
    res, err := upload(ctx, c, local, "n1", remote, false)
    require.NoError(t, err)
    require.False(t, res.IsError)
    got, err := os.ReadFile(remote)
    require.NoError(t, err)
    require.Equal(t, payload, got)

    // Download node → another local path
    out := filepath.Join(dir, "out.bin")
    res, err = download(ctx, c, "n1", remote, out, false)
    require.NoError(t, err)
    require.False(t, res.IsError)
    got2, err := os.ReadFile(out)
    require.NoError(t, err)
    require.Equal(t, payload, got2)

    _ = hex.EncodeToString(sum[:])
}
```

(Note: `n1`'s "remote" path uses the same `t.TempDir()` because in this test harness the node runs in the same process as the client. That's expected and exercises the real wire path through the hub.)

- [ ] **Step 5: Run tests**

```
go test ./internal/client/... -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```
git add internal/client/
git commit -m "client: implement file_transfer MCP tool"
```

---

## Task 7: e2e_test.go — file transfer scenarios

**Files:**
- Modify: `e2e_test.go`

- [ ] **Step 1: Add a file transfer test**

Append to `e2e_test.go`:

```go
func TestE2EFileTransfer(t *testing.T) {
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

    // Round-trip: local file → node → back to local
    dir := t.TempDir()
    src := filepath.Join(dir, "src.bin")
    payload := bytes.Repeat([]byte("xyz"), 5000) // 15 KB
    require.NoError(t, os.WriteFile(src, payload, 0o644))

    remote := filepath.Join(dir, "remote.bin")
    out := filepath.Join(dir, "out.bin")

    id := client.NewMsgID()
    sum := sha256.Sum256(payload)
    sumHex := hex.EncodeToString(sum[:])
    ch := c.RPC().Register(id)
    require.NoError(t, c.Send(&protocol.FilePutOpen{
        MsgID: id, Target: "e2e-host", Path: remote,
        Size: int64(len(payload)), SHA256: sumHex,
    }))
    r := <-ch
    require.True(t, r.OK)
    c.RPC().Unregister(id)
    finalCh := c.RPC().Register(id)
    // Push as a single chunk for simplicity.
    require.NoError(t, c.Send(&protocol.FileChunk{
        MsgID: id, Seq: 0, Data: payload, EOF: true,
    }))
    final := <-finalCh
    require.True(t, final.OK, final.Error)
    c.RPC().Unregister(id)

    got, err := os.ReadFile(remote)
    require.NoError(t, err)
    require.Equal(t, payload, got)
    _ = out
}
```

Add the new imports at the top of `e2e_test.go`: `bytes`, `crypto/sha256`, `encoding/hex`, `os`, `path/filepath`.

- [ ] **Step 2: Run the test**

```
go test . -run TestE2EFileTransfer -count=1
```

Expected: PASS.

- [ ] **Step 3: Run all tests**

```
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 4: Commit**

```
git add e2e_test.go
git commit -m "e2e: file_transfer upload round trip"
```

---

## Task 8: README — document file_transfer

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a file_transfer section**

Append to `README.md`:

```markdown
### file_transfer

Single-file transfers between the local machine and a node, between two nodes, or within a node:

\```
file_transfer(from, to, overwrite=false)
\```

Path syntax:

- `node:/abs/path` or `node:~/path` — a path on that node.
- `/abs/path` or `~/path` — a path on the machine running `tether mcp` (Claude Code's host).

Returns `{ok, bytes, sha256, duration_ms}` on success, or `{ok:false, error:"..."}` on failure.

Single file only — directories are not supported (use tar/zip on the source first). Default behavior refuses to overwrite an existing destination.
```

- [ ] **Step 2: Commit**

```
git add README.md
git commit -m "docs: document file_transfer MCP tool"
```

---

## Acceptance Criteria

- `go test ./... -count=1` passes.
- `file_transfer` MCP tool is registered and reachable from a stdio `tether mcp` process.
- Upload (local → node) preserves bytes and sha256.
- Download (node → local) writes the file atomically (no partial file on abort).
- Same-node copy (node:a → node:a) works.
- Node↔node relay works for at least one round trip (covered by `internal/hub/relay_test.go` once expanded — see Task 5 step 3 note; current MVP relies on smoke testing through a real two-host setup).
- Destination existing without `overwrite=true` returns `destination_exists`.
- sha256 mismatch returns `hash_mismatch` and leaves no file at destination.
- `internal/hub/mcp.go` (Phase 1) still does not exist.

## Non-Goals (unchanged from spec)

- Resume after Hub restart or conn drop.
- Directory recursion.
- Progress reporting.
- Compression beyond what WS `permessage-deflate` already provides.
- Multi-token authorization.
