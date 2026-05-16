# Phase 1: MCP Architecture Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the Hub's HTTP `/mcp` endpoint; reduce the Hub to a pure WS message relay; introduce a new `tether mcp` subcommand that runs a local stdio MCP server hosting the seven existing tools.

**Architecture:** A new `internal/client/` package dials the Hub's new `/client` WSS endpoint, hosts an MCP stdio server (using `github.com/mark3labs/mcp-go`), and translates MCP tool calls into CBOR protocol messages routed through the Hub to nodes. The Hub gains a unified `Hello.Role` handshake (`node` or `client`), a `ClientRegistry`, and a routing model where every request from a client carries a `Target` hostname; the Hub's router tracks `msg_id → peer conn` and forwards replies/streams back to the originating client. The seven tools' Go code is moved from `internal/hub/mcp.go` (deleted) into `internal/client/tools_exec.go`, structurally identical but executing over the new WSS RPC. External MCP behavior (tool names, inputs, outputs) is unchanged.

**Tech Stack:** Go 1.25, `github.com/coder/websocket`, `github.com/fxamacker/cbor/v2`, `github.com/mark3labs/mcp-go` (now in the client binary), `github.com/stretchr/testify`.

---

## File Structure

**New files:**
- `internal/client/config.go` — local config loader (`hub_url`, `token`)
- `internal/client/conn.go` — WSS connection to Hub: dial, hello, read loop, reconnect
- `internal/client/rpc.go` — `msg_id` correlator: register pending requests, deliver replies/streams
- `internal/client/mcp_server.go` — stdio MCP server setup, tool registration entry point
- `internal/client/tools_exec.go` — 7 tool implementations (list_devices, exec, start/list/get_output/send_stdin/kill_process)
- `internal/cli/mcp.go` — `tether mcp` subcommand
- `internal/hub/client_ws.go` — handler for `/client` endpoint (parallel to `device_ws.go`)

**Modified files:**
- `internal/protocol/messages.go` — add `Hello.Role`, `Target` field to 7 routable types, add `ListDevices` message
- `internal/protocol/codec.go` — register `list_devices` type
- `internal/hub/server.go` — register `/client`; remove `/mcp` wiring; remove `authMCP`
- `internal/hub/registry.go` — add `ClientRegistry` (or rename + parallel)
- `internal/hub/router.go` — rework around `msg_id → peer conn` (sticky and one-shot routes); raw-byte forwarding to avoid re-encoding
- `internal/hub/device_ws.go` — refactor to use new router API; rename type if needed
- `main.go` — wire `tether mcp` subcommand
- `README.md` — document new `tether mcp` mode, drop `/mcp` URL section
- `dist/systemd/tether-hub.service`, `dist/systemd/tether-node.service` — review (Hub service may need only a comment update; add an optional `tether-mcp.service` user unit if desired)
- `e2e_test.go` — drive a `tether mcp` instance over WSS instead of `CallExecForTest`

**Deleted files:**
- `internal/hub/mcp.go`
- `internal/hub/mcp_test.go`

---

## Task 1: Add Hello.Role, Target field, ListDevices to protocol

**Files:**
- Modify: `internal/protocol/messages.go`
- Modify: `internal/protocol/codec.go`
- Test: `internal/protocol/codec_test.go`

### Why

After this task, every request a client sends to the Hub carries a `Target` field indicating which node to route to (or empty for a Hub-local op). The Hub uses `Hello.Role` to decide which registry the connecting peer belongs to. `ListDevices` is the first Hub-local op (the Hub answers from its own registry).

- [ ] **Step 1: Update messages.go**

Edit `internal/protocol/messages.go`:

Add `Target` to the 7 client-originated requests:

```go
type Exec struct {
    Type      string            `cbor:"type"`
    MsgID     string            `cbor:"msg_id"`
    Target    string            `cbor:"target,omitempty"`
    Cmd       []string          `cbor:"cmd"`
    Cwd       string            `cbor:"cwd,omitempty"`
    Env       map[string]string `cbor:"env,omitempty"`
    Stdin     []byte            `cbor:"stdin,omitempty"`
    TTY       bool              `cbor:"tty,omitempty"`
    TimeoutMs int64             `cbor:"timeout_ms,omitempty"`
}

type ExecCancel struct {
    Type   string `cbor:"type"`
    MsgID  string `cbor:"msg_id"`
    Target string `cbor:"target,omitempty"`
}

type Start struct {
    Type      string            `cbor:"type"`
    MsgID     string            `cbor:"msg_id"`
    Target    string            `cbor:"target,omitempty"`
    ProcessID string            `cbor:"process_id"`
    Cmd       []string          `cbor:"cmd"`
    Cwd       string            `cbor:"cwd,omitempty"`
    Env       map[string]string `cbor:"env,omitempty"`
    TTY       bool              `cbor:"tty,omitempty"`
    Name      string            `cbor:"name,omitempty"`
}

type Stdin struct {
    Type      string `cbor:"type"`
    Target    string `cbor:"target,omitempty"`
    ProcessID string `cbor:"process_id"`
    Data      []byte `cbor:"data"`
}

type Kill struct {
    Type      string `cbor:"type"`
    MsgID     string `cbor:"msg_id"`
    Target    string `cbor:"target,omitempty"`
    ProcessID string `cbor:"process_id"`
    Signal    string `cbor:"signal,omitempty"`
}

type GetOutput struct {
    Type      string `cbor:"type"`
    MsgID     string `cbor:"msg_id"`
    Target    string `cbor:"target,omitempty"`
    ProcessID string `cbor:"process_id"`
    Offset    int64  `cbor:"offset,omitempty"`
    Length    int    `cbor:"length,omitempty"`
}

type List struct {
    Type         string `cbor:"type"`
    MsgID        string `cbor:"msg_id"`
    Target       string `cbor:"target,omitempty"`
    StatusFilter string `cbor:"status_filter,omitempty"`
    Limit        int    `cbor:"limit,omitempty"`
}
```

Add `Role` to `Hello`:

```go
type Hello struct {
    Type         string `cbor:"type"`
    Hostname     string `cbor:"hostname"`
    OS           string `cbor:"os"`
    Arch         string `cbor:"arch"`
    AgentVersion string `cbor:"agent_version"`
    Token        string `cbor:"token"`
    Role         string `cbor:"role,omitempty"` // "node" (default) | "client"
}
```

Add the new `ListDevices` request type at the end of the file:

```go
// ListDevices is a hub-local request (no Target).
type ListDevices struct {
    Type  string `cbor:"type"`
    MsgID string `cbor:"msg_id"`
}

func (m *ListDevices) msgType() string { return "list_devices" }
```

- [ ] **Step 2: Update codec.go**

Edit `internal/protocol/codec.go`:

In `setType`, add a case for `*ListDevices`:

```go
case *ListDevices:
    v.Type = m.msgType()
```

In `Decode`, add a case in the switch:

```go
case "list_devices":
    m = &ListDevices{}
```

- [ ] **Step 3: Write round-trip test for new/changed types**

Edit `internal/protocol/codec_test.go`. Append:

```go
func TestRoundTripListDevices(t *testing.T) {
    in := &ListDevices{MsgID: "abc"}
    raw, err := Encode(in)
    require.NoError(t, err)
    out, err := Decode(raw)
    require.NoError(t, err)
    got, ok := out.(*ListDevices)
    require.True(t, ok)
    require.Equal(t, "abc", got.MsgID)
    require.Equal(t, "list_devices", got.Type)
}

func TestRoundTripExecWithTarget(t *testing.T) {
    in := &Exec{MsgID: "m1", Target: "host-a", Cmd: []string{"sh", "-c", "ls"}}
    raw, err := Encode(in)
    require.NoError(t, err)
    out, err := Decode(raw)
    require.NoError(t, err)
    got, ok := out.(*Exec)
    require.True(t, ok)
    require.Equal(t, "host-a", got.Target)
    require.Equal(t, "m1", got.MsgID)
}

func TestRoundTripHelloRole(t *testing.T) {
    in := &Hello{Hostname: "c1", Token: "t", Role: "client"}
    raw, err := Encode(in)
    require.NoError(t, err)
    out, err := Decode(raw)
    require.NoError(t, err)
    got, ok := out.(*Hello)
    require.True(t, ok)
    require.Equal(t, "client", got.Role)
}
```

- [ ] **Step 4: Run tests and verify**

```
go test ./internal/protocol/...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add internal/protocol/
git commit -m "protocol: add Hello.Role, Target field, ListDevices message"
```

---

## Task 2: Rework Hub router for peer-conn-based routing

**Files:**
- Modify: `internal/hub/router.go`
- Modify: `internal/hub/registry.go` (add `PeerConn` interface; rename `DeviceConn`)
- Modify: `internal/hub/device_ws.go` (use new router API)
- Modify: `internal/hub/router_test.go`

### Why

The current router holds per-`msg_id` channels for in-process awaiters. After the refactor, the Hub does not run in-process MCP code; instead it routes replies/streams from nodes back to whichever **client peer** initiated the request. The router now stores `msg_id → PeerConn` (one-shot for plain replies, sticky for streaming Exec output and for future file chunks) and writes raw bytes back without decoding twice.

- [ ] **Step 1: Add PeerConn interface to registry.go**

Edit `internal/hub/registry.go`. Replace `DeviceConn` with `PeerConn` (broader name) and keep `Device` using it; keep the type alias for backwards compatibility within the package:

```go
// PeerConn is a generic send-back interface implemented by both nodes
// (device sessions) and clients (client sessions).
type PeerConn interface {
    SendRaw(raw []byte) error
    Close()
}
```

Change `Device.Conn` to `PeerConn` type. Search the rest of the package for `DeviceConn` and replace with `PeerConn`.

Add a `ClientRegistry` type alongside `Registry` (don't rename `Registry` yet to keep the diff focused — Task 3 handles renaming if useful):

```go
type Client struct {
    ID          string
    ConnectedAt time.Time
    Conn        PeerConn
}

type ClientRegistry struct {
    mu      sync.RWMutex
    clients map[string]*Client
}

func NewClientRegistry() *ClientRegistry {
    return &ClientRegistry{clients: make(map[string]*Client)}
}

func (r *ClientRegistry) Register(c *Client) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.clients[c.ID] = c
}

func (r *ClientRegistry) Unregister(id string) {
    r.mu.Lock()
    defer r.mu.Unlock()
    delete(r.clients, id)
}
```

- [ ] **Step 2: Rewrite router.go**

Replace the entire contents of `internal/hub/router.go`:

```go
package hub

import "sync"

// Router maps msg_id → destination peer conn. Used to route replies and
// streamed messages back from a node to the originating client.
//
// One-shot routes are removed after the first delivery; sticky routes stay
// until explicitly unregistered (used for streamed RPCs and file transfers).
type Router struct {
    mu     sync.Mutex
    routes map[string]route
}

type route struct {
    Conn   PeerConn
    Sticky bool
}

func NewRouter() *Router {
    return &Router{routes: make(map[string]route)}
}

// Register associates msg_id with the peer that should receive the reply.
// sticky=true keeps the route alive after Forward; sticky=false removes it
// on first Forward.
func (r *Router) Register(msgID string, conn PeerConn, sticky bool) {
    r.mu.Lock()
    r.routes[msgID] = route{Conn: conn, Sticky: sticky}
    r.mu.Unlock()
}

func (r *Router) Unregister(msgID string) {
    r.mu.Lock()
    delete(r.routes, msgID)
    r.mu.Unlock()
}

// Forward writes raw bytes to the peer registered under msg_id. Returns
// true if a route existed. Removes the route if it was one-shot.
func (r *Router) Forward(msgID string, raw []byte) bool {
    r.mu.Lock()
    rt, ok := r.routes[msgID]
    if ok && !rt.Sticky {
        delete(r.routes, msgID)
    }
    r.mu.Unlock()
    if !ok {
        return false
    }
    _ = rt.Conn.SendRaw(raw)
    return true
}

// Lookup returns the conn for msg_id without removing it (used to inspect
// sticky routes when deciding what to do).
func (r *Router) Lookup(msgID string) (PeerConn, bool) {
    r.mu.Lock()
    rt, ok := r.routes[msgID]
    r.mu.Unlock()
    if !ok {
        return nil, false
    }
    return rt.Conn, true
}
```

- [ ] **Step 3: Update device_ws.go to use SendRaw and the new router**

Edit `internal/hub/device_ws.go`:

- Change `deviceSession`'s `Send(msg any) error` to send through `protocol.Encode` then `SendRaw`. Add a `SendRaw(raw []byte) error` method matching `PeerConn`.
- The read loop should peek at decoded messages: for any message that has a `MsgID` (Reply, ExecOutput, ExecExit, FileChunk later), call `s.router.Forward(msgID, raw)`. For `Event`, `Pong`, etc. that don't route, just log/drop.

Concretely, replace the existing `Send` and `run` with:

```go
func (s *deviceSession) SendRaw(raw []byte) error {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    return s.conn.Write(ctx, websocket.MessageBinary, raw)
}

// Send is retained for in-process call sites in hub package; encodes then
// forwards to SendRaw.
func (s *deviceSession) Send(msg any) error {
    m, ok := msg.(protocol.Message)
    if !ok {
        return errAuth("not a protocol.Message")
    }
    raw, err := protocol.Encode(m)
    if err != nil {
        return err
    }
    return s.SendRaw(raw)
}

func (s *deviceSession) run(ctx context.Context) {
    for {
        _, raw, err := s.conn.Read(ctx)
        if err != nil {
            return
        }
        msg, err := protocol.Decode(raw)
        if err != nil {
            log.Printf("decode from %s: %v", s.device.Hostname, err)
            continue
        }
        s.device.LastSeen = time.Now()
        id := msgID(msg)
        if id != "" {
            s.router.Forward(id, raw)
        }
    }
}

// msgID extracts MsgID from messages that carry one (returns "" otherwise).
func msgID(m protocol.Message) string {
    switch v := m.(type) {
    case *protocol.Reply:
        return v.MsgID
    case *protocol.ExecOutput:
        return v.MsgID
    case *protocol.ExecExit:
        return v.MsgID
    }
    return ""
}
```

(Note: the old `s.router.Deliver(msg)` call disappears; the router is now purely a forwarding table.)

- [ ] **Step 4: Update router_test.go**

Replace contents of `internal/hub/router_test.go`:

```go
package hub

import (
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

type fakeConn struct {
    sent [][]byte
    closed bool
}

func (f *fakeConn) SendRaw(raw []byte) error { f.sent = append(f.sent, raw); return nil }
func (f *fakeConn) Close()                    { f.closed = true }

func TestRouterOneShot(t *testing.T) {
    r := NewRouter()
    c := &fakeConn{}
    r.Register("m1", c, false)
    ok := r.Forward("m1", []byte("hello"))
    require.True(t, ok)
    require.Len(t, c.sent, 1)

    // Second Forward should miss — route removed.
    ok = r.Forward("m1", []byte("again"))
    assert.False(t, ok)
}

func TestRouterSticky(t *testing.T) {
    r := NewRouter()
    c := &fakeConn{}
    r.Register("m2", c, true)
    require.True(t, r.Forward("m2", []byte("a")))
    require.True(t, r.Forward("m2", []byte("b")))
    require.Len(t, c.sent, 2)

    r.Unregister("m2")
    assert.False(t, r.Forward("m2", []byte("c")))
}

func TestRouterMissingMsgID(t *testing.T) {
    r := NewRouter()
    assert.False(t, r.Forward("nope", []byte("x")))
}
```

- [ ] **Step 5: Run hub package tests, expect only router tests to pass cleanly; mcp_test.go may fail (next task removes it)**

```
go test ./internal/hub/ -run TestRouter
```

Expected: PASS for the 3 router tests.

- [ ] **Step 6: Commit**

```
git add internal/hub/
git commit -m "hub: rework router around peer-conn forwarding"
```

---

## Task 3: Add /client endpoint and unified handshake

**Files:**
- Create: `internal/hub/client_ws.go`
- Modify: `internal/hub/device_ws.go` (factor handshake into a shared helper)
- Modify: `internal/hub/server.go`
- Test: `internal/hub/server_test.go` (add a test that a client can connect with Role=client)

### Why

A new `/client` WS endpoint authenticates with the same shared `token`. After handshake, clients land in `ClientRegistry` and their read loop dispatches messages: hub-local ops are answered inline, routable requests register a route and forward to the target node.

- [ ] **Step 1: Refactor common handshake into endpoint_handshake helper**

Edit `internal/hub/device_ws.go`. Extract the handshake body (read first frame, check Hello, validate token, return Hello message + conn) into a shared helper:

```go
// readHello reads the initial Hello frame and validates the shared token.
func (s *Server) readHello(ctx context.Context, c *websocket.Conn) (*protocol.Hello, error) {
    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()
    _, data, err := c.Read(ctx)
    if err != nil {
        return nil, err
    }
    msg, err := protocol.Decode(data)
    if err != nil {
        return nil, err
    }
    hello, ok := msg.(*protocol.Hello)
    if !ok {
        return nil, errAuth("first message must be hello")
    }
    if hello.Token != s.opts.Token {
        return nil, errAuth("bad token")
    }
    return hello, nil
}
```

Rewrite `handleDevice` and the device `handshake` to call `readHello`. The device path runs only when `hello.Role == "" || hello.Role == "node"` (treat empty as node for backward compat).

- [ ] **Step 2: Create client_ws.go**

Create `internal/hub/client_ws.go`:

```go
package hub

import (
    "context"
    "crypto/rand"
    "encoding/hex"
    "errors"
    "fmt"
    "log"
    "net/http"
    "time"

    "github.com/coder/websocket"
    "github.com/qiuxiang/tether/internal/protocol"
)

func newClientID() string {
    var b [8]byte
    _, _ = rand.Read(b[:])
    return hex.EncodeToString(b[:])
}

func (s *Server) handleClient(w http.ResponseWriter, r *http.Request) {
    c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
    if err != nil {
        return
    }
    ctx := r.Context()

    hello, err := s.readHello(ctx, c)
    if err != nil {
        log.Printf("client handshake failed: %v", err)
        c.Close(websocket.StatusPolicyViolation, err.Error())
        return
    }
    if hello.Role != "client" {
        c.Close(websocket.StatusPolicyViolation, "role must be client")
        return
    }

    id := newClientID()
    sess := &clientSession{id: id, conn: c, server: s}
    s.clients.Register(&Client{ID: id, ConnectedAt: time.Now(), Conn: sess})
    log.Printf("client registered: id=%s", id)

    defer func() {
        log.Printf("client disconnected: id=%s", id)
        s.clients.Unregister(id)
        c.Close(websocket.StatusNormalClosure, "")
    }()
    sess.run(ctx)
}

type clientSession struct {
    id     string
    conn   *websocket.Conn
    server *Server
}

func (cs *clientSession) SendRaw(raw []byte) error {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    return cs.conn.Write(ctx, websocket.MessageBinary, raw)
}

func (cs *clientSession) Close() { cs.conn.Close(websocket.StatusNormalClosure, "") }

func (cs *clientSession) run(ctx context.Context) {
    for {
        _, raw, err := cs.conn.Read(ctx)
        if err != nil {
            return
        }
        msg, err := protocol.Decode(raw)
        if err != nil {
            log.Printf("client %s decode: %v", cs.id, err)
            continue
        }
        cs.dispatch(raw, msg)
    }
}

// dispatch handles a single client→hub message: either a hub-local op (answer
// inline) or a routable request (register msg_id route and forward to target).
func (cs *clientSession) dispatch(raw []byte, msg protocol.Message) {
    switch m := msg.(type) {
    case *protocol.ListDevices:
        cs.replyListDevices(m.MsgID)
    case *protocol.Exec:
        cs.routeStream(m.MsgID, m.Target, raw)
    case *protocol.ExecCancel:
        cs.routeOneShot(m.MsgID, m.Target, raw)
    case *protocol.Start:
        cs.routeOneShot(m.MsgID, m.Target, raw)
    case *protocol.Stdin:
        cs.forwardFireAndForget(m.Target, raw)
    case *protocol.Kill:
        cs.routeOneShot(m.MsgID, m.Target, raw)
    case *protocol.GetOutput:
        cs.routeOneShot(m.MsgID, m.Target, raw)
    case *protocol.List:
        cs.routeOneShot(m.MsgID, m.Target, raw)
    default:
        // Unknown / not-routable from client: drop.
    }
}

func (cs *clientSession) replyListDevices(msgID string) {
    list := cs.server.registry.List()
    items := make([]any, 0, len(list))
    for _, d := range list {
        items = append(items, map[string]any{
            "hostname":      d.Hostname,
            "os":            d.OS,
            "arch":          d.Arch,
            "agent_version": d.AgentVersion,
            "online":        d.Conn != nil,
            "last_seen":     d.LastSeen.Unix(),
        })
    }
    reply := &protocol.Reply{MsgID: msgID, OK: true, Data: map[string]any{"devices": items}}
    out, err := protocol.Encode(reply)
    if err != nil {
        return
    }
    _ = cs.SendRaw(out)
}

func (cs *clientSession) routeOneShot(msgID, target string, raw []byte) {
    if err := cs.routeTo(msgID, target, raw, false); err != nil {
        cs.sendErrorReply(msgID, err)
    }
}

func (cs *clientSession) routeStream(msgID, target string, raw []byte) {
    if err := cs.routeTo(msgID, target, raw, true); err != nil {
        cs.sendErrorReply(msgID, err)
    }
}

func (cs *clientSession) forwardFireAndForget(target string, raw []byte) {
    if target == "" {
        return
    }
    d, ok := cs.server.registry.Get(target)
    if !ok || d.Conn == nil {
        return
    }
    _ = d.Conn.SendRaw(raw)
}

func (cs *clientSession) routeTo(msgID, target string, raw []byte, sticky bool) error {
    if target == "" {
        return errors.New("missing target")
    }
    d, ok := cs.server.registry.Get(target)
    if !ok || d.Conn == nil {
        return fmt.Errorf("device_offline: %s", target)
    }
    cs.server.router.Register(msgID, cs, sticky)
    if err := d.Conn.SendRaw(raw); err != nil {
        cs.server.router.Unregister(msgID)
        return err
    }
    return nil
}

func (cs *clientSession) sendErrorReply(msgID string, err error) {
    reply := &protocol.Reply{MsgID: msgID, OK: false, Error: err.Error()}
    out, encErr := protocol.Encode(reply)
    if encErr != nil {
        return
    }
    _ = cs.SendRaw(out)
}
```

- [ ] **Step 3: Register /client and ClientRegistry on Server**

Edit `internal/hub/server.go`. Replace contents:

```go
package hub

import "net/http"

type Options struct {
    Token string
}

type Server struct {
    opts     Options
    registry *Registry
    clients  *ClientRegistry
    router   *Router
}

func NewServer(opts Options) *Server {
    return &Server{
        opts:     opts,
        registry: NewRegistry(),
        clients:  NewClientRegistry(),
        router:   NewRouter(),
    }
}

func (s *Server) Registry() *Registry { return s.registry }
func (s *Server) Clients() *ClientRegistry { return s.clients }
func (s *Server) Router() *Router { return s.router }

func (s *Server) Handler() http.Handler {
    mux := http.NewServeMux()
    mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(200); w.Write([]byte("ok"))
    })
    mux.HandleFunc("/device", s.handleDevice)
    mux.HandleFunc("/client", s.handleClient)
    return mux
}
```

The `authMCP` helper and `mcpHandler` reference are gone. **The build will not compile** until Task 4 deletes `mcp.go`.

- [ ] **Step 4: Update server_test.go**

Look at existing `internal/hub/server_test.go`. Anything that touches `/mcp` HTTP must be removed. Add a new test that a `/client` peer can complete the WS handshake:

```go
func TestClientHandshake(t *testing.T) {
    s := NewServer(Options{Token: "tk"})
    ts := httptest.NewServer(s.Handler())
    defer ts.Close()
    wsURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"

    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()

    c, _, err := websocket.Dial(ctx, wsURL, nil)
    require.NoError(t, err)
    defer c.Close(websocket.StatusNormalClosure, "")

    hello := &protocol.Hello{Hostname: "claude-host", Token: "tk", Role: "client"}
    raw, _ := protocol.Encode(hello)
    require.NoError(t, c.Write(ctx, websocket.MessageBinary, raw))

    // Send ListDevices, expect a Reply back.
    req := &protocol.ListDevices{MsgID: "1"}
    rraw, _ := protocol.Encode(req)
    require.NoError(t, c.Write(ctx, websocket.MessageBinary, rraw))

    _, data, err := c.Read(ctx)
    require.NoError(t, err)
    msg, err := protocol.Decode(data)
    require.NoError(t, err)
    reply, ok := msg.(*protocol.Reply)
    require.True(t, ok)
    require.Equal(t, "1", reply.MsgID)
    require.True(t, reply.OK)
}
```

Drop any existing test in this file that depends on `/mcp` HTTP.

- [ ] **Step 5: Commit (tests still won't build because mcp.go is unresolved — fix in next task)**

```
git add internal/hub/
git commit -m "hub: add /client endpoint, unified handshake, ListDevices"
```

---

## Task 4: Delete hub/mcp.go, hub/mcp_test.go and drop mcp-go dep from hub

**Files:**
- Delete: `internal/hub/mcp.go`
- Delete: `internal/hub/mcp_test.go`

- [ ] **Step 1: Delete the files**

```
git rm internal/hub/mcp.go internal/hub/mcp_test.go
```

- [ ] **Step 2: Build the hub package**

```
go build ./internal/hub/
```

Expected: PASS (no compile errors). If there are leftover references (e.g. `CallExecForTest` in `e2e_test.go`), leave them broken for now — Task 14 rewrites e2e.

- [ ] **Step 3: Run hub package tests**

```
go test ./internal/hub/...
```

Expected: PASS (router tests + client handshake test).

- [ ] **Step 4: Commit**

```
git commit -m "hub: remove /mcp endpoint and related code"
```

---

## Task 5: New client package — config + conn

**Files:**
- Create: `internal/client/config.go`
- Create: `internal/client/conn.go`
- Create: `internal/client/config_test.go`

### Why

`tether mcp` needs a config (hub URL + shared token) and a WSS connection that performs the client handshake and reconnects with backoff (mirroring `internal/node/client.go`'s structure but with `Role=client`).

- [ ] **Step 1: Write config.go**

Create `internal/client/config.go`:

```go
package client

import (
    "errors"
    "os"

    "gopkg.in/yaml.v3"
)

type Config struct {
    HubURL string `yaml:"hub_url"`
    Token  string `yaml:"token"`
}

func Load(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }
    var c Config
    if err := yaml.Unmarshal(data, &c); err != nil {
        return nil, err
    }
    if c.HubURL == "" {
        return nil, errors.New("config: hub_url is required")
    }
    if c.Token == "" {
        return nil, errors.New("config: token is required")
    }
    return &c, nil
}
```

- [ ] **Step 2: Write config_test.go**

Create `internal/client/config_test.go`:

```go
package client

import (
    "os"
    "path/filepath"
    "testing"

    "github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
    dir := t.TempDir()
    p := filepath.Join(dir, "c.yaml")
    require.NoError(t, os.WriteFile(p, []byte("hub_url: wss://h\ntoken: tk\n"), 0o600))

    cfg, err := Load(p)
    require.NoError(t, err)
    require.Equal(t, "wss://h", cfg.HubURL)
    require.Equal(t, "tk", cfg.Token)
}

func TestLoadConfigMissingFields(t *testing.T) {
    dir := t.TempDir()
    p := filepath.Join(dir, "c.yaml")
    require.NoError(t, os.WriteFile(p, []byte("hub_url: wss://h\n"), 0o600))

    _, err := Load(p)
    require.Error(t, err)
}
```

- [ ] **Step 3: Write conn.go**

Create `internal/client/conn.go`:

```go
package client

import (
    "context"
    "errors"
    "log"
    "math/rand"
    "os"
    "sync"
    "time"

    "github.com/coder/websocket"
    "github.com/qiuxiang/tether/internal/protocol"
)

// Conn maintains a WSS connection to the hub and demultiplexes incoming
// messages by msg_id into pending requests / streams (see rpc.go).
type Conn struct {
    cfg     Config
    rpc     *RPC

    mu      sync.Mutex
    ws      *websocket.Conn
    ready   chan struct{} // closed on each successful (re)connect
}

func NewConn(cfg Config) *Conn {
    return &Conn{cfg: cfg, rpc: NewRPC(), ready: make(chan struct{})}
}

func (c *Conn) RPC() *RPC { return c.rpc }

func (c *Conn) WaitReady(ctx context.Context) error {
    c.mu.Lock()
    ready := c.ready
    c.mu.Unlock()
    select {
    case <-ready:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

func (c *Conn) Send(msg protocol.Message) error {
    raw, err := protocol.Encode(msg)
    if err != nil {
        return err
    }
    c.mu.Lock()
    ws := c.ws
    c.mu.Unlock()
    if ws == nil {
        return errors.New("not connected")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    return ws.Write(ctx, websocket.MessageBinary, raw)
}

func (c *Conn) Run(ctx context.Context) {
    backoff := time.Second
    for {
        if ctx.Err() != nil {
            return
        }
        if err := c.dial(ctx); err != nil {
            log.Printf("hub connection lost: %v", err)
        }
        // Reset ready signal for next dial.
        c.mu.Lock()
        c.ws = nil
        c.ready = make(chan struct{})
        c.mu.Unlock()
        wait := backoff + time.Duration(rand.Int63n(int64(backoff/2+1)))
        select {
        case <-ctx.Done():
            return
        case <-time.After(wait):
        }
        if backoff < 30*time.Second {
            backoff *= 2
        }
    }
}

func (c *Conn) dial(ctx context.Context) error {
    dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
    defer cancel()
    ws, _, err := websocket.Dial(dialCtx, c.cfg.HubURL, nil)
    if err != nil {
        return err
    }
    defer ws.Close(websocket.StatusNormalClosure, "")

    host, _ := os.Hostname()
    hello := &protocol.Hello{
        Hostname:     host,
        Token:        c.cfg.Token,
        Role:         "client",
        AgentVersion: "0.1.0",
    }
    raw, _ := protocol.Encode(hello)
    if err := ws.Write(ctx, websocket.MessageBinary, raw); err != nil {
        return err
    }

    c.mu.Lock()
    c.ws = ws
    close(c.ready)
    c.mu.Unlock()

    for {
        _, data, err := ws.Read(ctx)
        if err != nil {
            return err
        }
        msg, err := protocol.Decode(data)
        if err != nil {
            log.Printf("decode: %v", err)
            continue
        }
        c.rpc.Deliver(msg)
    }
}
```

- [ ] **Step 4: Run tests**

```
go test ./internal/client/...
```

Expected: PASS (config tests).

- [ ] **Step 5: Commit**

```
git add internal/client/
git commit -m "client: add config loader and WSS connection"
```

---

## Task 6: Client package — RPC correlator

**Files:**
- Create: `internal/client/rpc.go`
- Create: `internal/client/rpc_test.go`

### Why

When a tool calls `Exec` it needs to await replies and streamed `ExecOutput` / `ExecExit` over the shared WS. RPC handles msg_id allocation and delivery to per-call channels.

- [ ] **Step 1: Write rpc.go**

Create `internal/client/rpc.go`:

```go
package client

import (
    "crypto/rand"
    "encoding/hex"
    "sync"

    "github.com/qiuxiang/tether/internal/protocol"
)

type RPC struct {
    mu      sync.Mutex
    replies map[string]chan *protocol.Reply
    streams map[string]chan protocol.Message
}

func NewRPC() *RPC {
    return &RPC{
        replies: make(map[string]chan *protocol.Reply),
        streams: make(map[string]chan protocol.Message),
    }
}

func NewMsgID() string {
    var b [8]byte
    _, _ = rand.Read(b[:])
    return hex.EncodeToString(b[:])
}

func (r *RPC) Register(msgID string) chan *protocol.Reply {
    ch := make(chan *protocol.Reply, 1)
    r.mu.Lock()
    r.replies[msgID] = ch
    r.mu.Unlock()
    return ch
}

func (r *RPC) RegisterStream(msgID string) chan protocol.Message {
    ch := make(chan protocol.Message, 32)
    r.mu.Lock()
    r.streams[msgID] = ch
    r.mu.Unlock()
    return ch
}

func (r *RPC) Unregister(msgID string) {
    r.mu.Lock()
    delete(r.replies, msgID)
    delete(r.streams, msgID)
    r.mu.Unlock()
}

func (r *RPC) Deliver(msg protocol.Message) {
    switch m := msg.(type) {
    case *protocol.Reply:
        r.mu.Lock()
        ch, ok := r.replies[m.MsgID]
        r.mu.Unlock()
        if ok {
            select {
            case ch <- m:
            default:
            }
        }
    case *protocol.ExecOutput:
        r.mu.Lock()
        ch, ok := r.streams[m.MsgID]
        r.mu.Unlock()
        if ok {
            ch <- m
        }
    case *protocol.ExecExit:
        r.mu.Lock()
        ch, ok := r.streams[m.MsgID]
        r.mu.Unlock()
        if ok {
            ch <- m
            close(ch)
            r.Unregister(m.MsgID)
        }
    }
}
```

- [ ] **Step 2: Write rpc_test.go**

Create `internal/client/rpc_test.go`:

```go
package client

import (
    "testing"

    "github.com/qiuxiang/tether/internal/protocol"
    "github.com/stretchr/testify/require"
)

func TestRPCReply(t *testing.T) {
    r := NewRPC()
    ch := r.Register("m1")
    r.Deliver(&protocol.Reply{MsgID: "m1", OK: true})
    got := <-ch
    require.True(t, got.OK)
}

func TestRPCStream(t *testing.T) {
    r := NewRPC()
    ch := r.RegisterStream("s1")
    r.Deliver(&protocol.ExecOutput{MsgID: "s1", Stream: "stdout", Data: []byte("x")})
    r.Deliver(&protocol.ExecExit{MsgID: "s1", Code: 0})
    a := <-ch
    require.IsType(t, &protocol.ExecOutput{}, a)
    b := <-ch
    require.IsType(t, &protocol.ExecExit{}, b)
    _, ok := <-ch
    require.False(t, ok, "channel should be closed after ExecExit")
}
```

- [ ] **Step 3: Run tests**

```
go test ./internal/client/...
```

Expected: PASS.

- [ ] **Step 4: Commit**

```
git add internal/client/rpc.go internal/client/rpc_test.go
git commit -m "client: add RPC correlator"
```

---

## Task 7: Client tools — list_devices and exec

**Files:**
- Create: `internal/client/mcp_server.go`
- Create: `internal/client/tools_exec.go`
- Create: `internal/client/tools_exec_test.go`

### Why

The MCP server skeleton hosts tools. We port `list_devices` (uses `ListDevices` hub-local op) and `exec` (uses streaming `Exec` + collects `ExecOutput`/`ExecExit`) first. Subsequent tasks add the rest.

- [ ] **Step 1: Write mcp_server.go**

Create `internal/client/mcp_server.go`:

```go
package client

import (
    "context"
    "io"

    "github.com/mark3labs/mcp-go/server"
)

// Server wires the WSS Conn into an MCP stdio server.
type Server struct {
    conn *Conn
    mcp  *server.MCPServer
}

func NewMCPServer(c *Conn) *Server {
    s := &Server{conn: c, mcp: server.NewMCPServer("tether", "0.1.0")}
    registerExecTools(s.mcp, c)
    return s
}

// Serve runs the stdio MCP loop. Returns when stdin closes or ctx is done.
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
    return server.NewStdioServer(s.mcp).Listen(ctx, in, out)
}
```

(Verify the mcp-go API names against the version in `go.mod` — `v0.54.0` exports `server.NewStdioServer(...).Listen(ctx, in, out)`. If the API differs slightly, adjust this single call site.)

- [ ] **Step 2: Write tools_exec.go (list_devices + exec)**

Create `internal/client/tools_exec.go`:

```go
package client

import (
    "context"
    "encoding/json"
    "errors"
    "time"

    "github.com/mark3labs/mcp-go/mcp"
    "github.com/mark3labs/mcp-go/server"
    "github.com/qiuxiang/tether/internal/protocol"
)

const defaultRPCTimeout = 30 * time.Second

func registerExecTools(m *server.MCPServer, c *Conn) {
    m.AddTool(
        mcp.NewTool("list_devices",
            mcp.WithDescription("List all registered devices."),
        ),
        func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
            id := NewMsgID()
            ch := c.rpc.Register(id)
            defer c.rpc.Unregister(id)
            if err := c.Send(&protocol.ListDevices{MsgID: id}); err != nil {
                return mcp.NewToolResultError(err.Error()), nil
            }
            select {
            case reply := <-ch:
                if !reply.OK {
                    return mcp.NewToolResultError(reply.Error), nil
                }
                b, _ := json.Marshal(reply.Data["devices"])
                return mcp.NewToolResultText(string(b)), nil
            case <-time.After(defaultRPCTimeout):
                return mcp.NewToolResultError("timeout"), nil
            case <-ctx.Done():
                return mcp.NewToolResultError(ctx.Err().Error()), nil
            }
        },
    )

    m.AddTool(
        mcp.NewTool("exec",
            mcp.WithDescription("Run a one-shot command on a device. Streams output, returns final stdout/stderr/exit_code."),
            mcp.WithString("device", mcp.Required()),
            mcp.WithString("cmd", mcp.Required(), mcp.Description("Shell command (passed to sh -c)")),
            mcp.WithString("cwd"),
            mcp.WithObject("env"),
            mcp.WithString("stdin"),
            mcp.WithBoolean("tty"),
            mcp.WithNumber("timeout"),
        ),
        func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
            args := req.GetArguments()
            device, _ := args["device"].(string)
            cmd, _ := args["cmd"].(string)
            cwd, _ := args["cwd"].(string)
            stdin, _ := args["stdin"].(string)
            tty, _ := args["tty"].(bool)
            timeout := 60 * time.Second
            if t, ok := args["timeout"].(float64); ok {
                timeout = time.Duration(t) * time.Second
            }
            envMap := extractStringMap(args["env"])

            id := NewMsgID()
            ch := c.rpc.RegisterStream(id)
            defer c.rpc.Unregister(id)
            msg := &protocol.Exec{
                MsgID: id, Target: device,
                Cmd: []string{"sh", "-c", cmd}, Cwd: cwd, Env: envMap,
                Stdin: []byte(stdin), TTY: tty, TimeoutMs: timeout.Milliseconds(),
            }
            if err := c.Send(msg); err != nil {
                return mcp.NewToolResultError(err.Error()), nil
            }

            var stdoutB, stderrB []byte
            deadline := time.After(timeout + 5*time.Second)
            for {
                select {
                case m, ok := <-ch:
                    if !ok {
                        return resultExec(stdoutB, stderrB, 0, false), nil
                    }
                    switch v := m.(type) {
                    case *protocol.ExecOutput:
                        if v.Stream == "stderr" {
                            stderrB = append(stderrB, v.Data...)
                        } else {
                            stdoutB = append(stdoutB, v.Data...)
                        }
                    case *protocol.ExecExit:
                        return resultExec(stdoutB, stderrB, v.Code, false), nil
                    }
                case <-deadline:
                    _ = c.Send(&protocol.ExecCancel{MsgID: id, Target: device})
                    return resultExec(stdoutB, stderrB, -1, true), nil
                case <-ctx.Done():
                    _ = c.Send(&protocol.ExecCancel{MsgID: id, Target: device})
                    return mcp.NewToolResultError(errors.New("cancelled").Error()), nil
                }
            }
        },
    )
}

func resultExec(stdoutB, stderrB []byte, code int, timedOut bool) *mcp.CallToolResult {
    b, _ := json.Marshal(map[string]any{
        "stdout": string(stdoutB), "stderr": string(stderrB),
        "exit_code": code, "timed_out": timedOut,
    })
    return mcp.NewToolResultText(string(b))
}

func extractStringMap(v any) map[string]string {
    out := map[string]string{}
    if e, ok := v.(map[string]any); ok {
        for k, vv := range e {
            if vs, ok := vv.(string); ok {
                out[k] = vs
            }
        }
    }
    return out
}
```

- [ ] **Step 3: Write tools_exec_test.go covering list_devices end-to-end**

Create `internal/client/tools_exec_test.go`:

```go
package client

import (
    "context"
    "encoding/json"
    "net/http/httptest"
    "strings"
    "testing"
    "time"

    "github.com/qiuxiang/tether/internal/hub"
    "github.com/qiuxiang/tether/internal/node"
    "github.com/qiuxiang/tether/internal/protocol"
    "github.com/stretchr/testify/require"
)

func setupClusterWithClient(t *testing.T) (*Conn, *hub.Server, func()) {
    t.Helper()
    s := hub.NewServer(hub.Options{Token: "tk"})
    ts := httptest.NewServer(s.Handler())

    // Connect a node.
    nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
    nc := node.New(node.Config{HubURL: nodeURL, Token: "tk", Hostname: "n1"})
    nc.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
    ctx, cancel := context.WithCancel(context.Background())
    go nc.Run(ctx)
    require.Eventually(t, func() bool {
        _, ok := s.Registry().Get("n1")
        return ok
    }, 2*time.Second, 20*time.Millisecond)

    // Connect a client.
    cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
    c := NewConn(Config{HubURL: cliURL, Token: "tk"})
    go c.Run(ctx)
    cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
    require.NoError(t, c.WaitReady(cctx))
    ccancel()

    cleanup := func() { cancel(); ts.Close() }
    return c, s, cleanup
}

func TestListDevicesEndToEnd(t *testing.T) {
    c, _, cleanup := setupClusterWithClient(t)
    defer cleanup()

    id := NewMsgID()
    ch := c.rpc.Register(id)
    require.NoError(t, c.Send(&protocol.ListDevices{MsgID: id}))
    select {
    case reply := <-ch:
        require.True(t, reply.OK)
        b, _ := json.Marshal(reply.Data["devices"])
        require.Contains(t, string(b), "n1")
    case <-time.After(2 * time.Second):
        t.Fatal("no reply")
    }
}

func TestExecEndToEnd(t *testing.T) {
    c, _, cleanup := setupClusterWithClient(t)
    defer cleanup()

    id := NewMsgID()
    ch := c.rpc.RegisterStream(id)
    defer c.rpc.Unregister(id)
    require.NoError(t, c.Send(&protocol.Exec{
        MsgID: id, Target: "n1",
        Cmd: []string{"sh", "-c", "echo hello"},
        TimeoutMs: 5000,
    }))
    var stdout []byte
    deadline := time.After(3 * time.Second)
    for {
        select {
        case m, ok := <-ch:
            if !ok {
                t.Fatalf("channel closed before ExecExit; stdout=%q", stdout)
            }
            switch v := m.(type) {
            case *protocol.ExecOutput:
                stdout = append(stdout, v.Data...)
            case *protocol.ExecExit:
                require.Equal(t, 0, v.Code)
                require.Contains(t, string(stdout), "hello")
                return
            }
        case <-deadline:
            t.Fatal("exec timed out")
        }
    }
}
```

- [ ] **Step 4: Run tests**

```
go test ./internal/client/... -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add internal/client/
git commit -m "client: implement list_devices and exec MCP tools"
```

---

## Task 8: Client tools — start_process, list_processes, get_output, send_stdin, kill_process

**Files:**
- Modify: `internal/client/tools_exec.go` (add 5 more tool registrations)
- Modify: `internal/client/tools_exec_test.go` (add end-to-end coverage for at least one)

### Why

Port the remaining tools. Each is structurally the same: build a CBOR message with `Target` set, register a one-shot reply, send, await, marshal data into JSON for MCP response.

- [ ] **Step 1: Add 5 tool registrations**

Append inside `registerExecTools` after the existing two tools, in `internal/client/tools_exec.go`:

```go
// start_process
m.AddTool(
    mcp.NewTool("start_process",
        mcp.WithDescription("Start a long-running background process. Returns process_id."),
        mcp.WithString("device", mcp.Required()),
        mcp.WithString("cmd", mcp.Required()),
        mcp.WithString("cwd"),
        mcp.WithObject("env"),
        mcp.WithBoolean("tty"),
        mcp.WithString("name"),
    ),
    func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
        args := req.GetArguments()
        device, _ := args["device"].(string)
        cmdStr, _ := args["cmd"].(string)
        cwd, _ := args["cwd"].(string)
        tty, _ := args["tty"].(bool)
        name, _ := args["name"].(string)
        envMap := extractStringMap(args["env"])
        pid := NewMsgID()
        id := NewMsgID()
        ch := c.rpc.Register(id)
        defer c.rpc.Unregister(id)
        if err := c.Send(&protocol.Start{
            MsgID: id, Target: device, ProcessID: pid,
            Cmd: []string{"sh", "-c", cmdStr}, Cwd: cwd, Env: envMap,
            TTY: tty, Name: name,
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
        case <-time.After(defaultRPCTimeout):
            return mcp.NewToolResultError("timeout"), nil
        case <-ctx.Done():
            return mcp.NewToolResultError(ctx.Err().Error()), nil
        }
    },
)

// list_processes — when device == "", fan out across all devices on the client side
m.AddTool(
    mcp.NewTool("list_processes",
        mcp.WithDescription("List processes on a device (or all)."),
        mcp.WithString("device"),
        mcp.WithNumber("limit"),
        mcp.WithString("status_filter"),
    ),
    func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
        args := req.GetArguments()
        device, _ := args["device"].(string)
        limit := 50
        if l, ok := args["limit"].(float64); ok {
            limit = int(l)
        }
        filter, _ := args["status_filter"].(string)

        if device != "" {
            id := NewMsgID()
            ch := c.rpc.Register(id)
            defer c.rpc.Unregister(id)
            if err := c.Send(&protocol.List{MsgID: id, Target: device, Limit: limit, StatusFilter: filter}); err != nil {
                return mcp.NewToolResultError(err.Error()), nil
            }
            select {
            case reply := <-ch:
                if !reply.OK {
                    return mcp.NewToolResultError(reply.Error), nil
                }
                b, _ := json.Marshal(reply.Data)
                return mcp.NewToolResultText(string(b)), nil
            case <-time.After(defaultRPCTimeout):
                return mcp.NewToolResultError("timeout"), nil
            case <-ctx.Done():
                return mcp.NewToolResultError(ctx.Err().Error()), nil
            }
        }

        // No device specified: fan out via ListDevices then per-device List.
        devs, err := fetchDevices(ctx, c)
        if err != nil {
            return mcp.NewToolResultError(err.Error()), nil
        }
        all := []any{}
        for _, host := range devs {
            id := NewMsgID()
            ch := c.rpc.Register(id)
            if err := c.Send(&protocol.List{MsgID: id, Target: host, Limit: limit, StatusFilter: filter}); err != nil {
                c.rpc.Unregister(id)
                continue
            }
            select {
            case reply := <-ch:
                c.rpc.Unregister(id)
                if !reply.OK {
                    continue
                }
                if procs, ok := reply.Data["processes"].([]any); ok {
                    for _, p := range procs {
                        if pm, ok := p.(map[string]any); ok {
                            pm["device"] = host
                        }
                        all = append(all, p)
                    }
                }
            case <-time.After(defaultRPCTimeout):
                c.rpc.Unregister(id)
            }
        }
        b, _ := json.Marshal(all)
        return mcp.NewToolResultText(string(b)), nil
    },
)

// get_output
m.AddTool(
    mcp.NewTool("get_output",
        mcp.WithDescription("Read log bytes from a process by offset."),
        mcp.WithString("device", mcp.Required()),
        mcp.WithString("process_id", mcp.Required()),
        mcp.WithNumber("offset"),
        mcp.WithNumber("length"),
    ),
    func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
        args := req.GetArguments()
        device, _ := args["device"].(string)
        pid, _ := args["process_id"].(string)
        var offset int64
        if o, ok := args["offset"].(float64); ok {
            offset = int64(o)
        }
        length := 65536
        if l, ok := args["length"].(float64); ok {
            length = int(l)
        }
        id := NewMsgID()
        ch := c.rpc.Register(id)
        defer c.rpc.Unregister(id)
        if err := c.Send(&protocol.GetOutput{MsgID: id, Target: device, ProcessID: pid, Offset: offset, Length: length}); err != nil {
            return mcp.NewToolResultError(err.Error()), nil
        }
        select {
        case reply := <-ch:
            if !reply.OK {
                return mcp.NewToolResultError(reply.Error), nil
            }
            b, _ := json.Marshal(reply.Data)
            return mcp.NewToolResultText(string(b)), nil
        case <-time.After(defaultRPCTimeout):
            return mcp.NewToolResultError("timeout"), nil
        case <-ctx.Done():
            return mcp.NewToolResultError(ctx.Err().Error()), nil
        }
    },
)

// send_stdin — fire and forget
m.AddTool(
    mcp.NewTool("send_stdin",
        mcp.WithDescription("Send stdin bytes to a process."),
        mcp.WithString("device", mcp.Required()),
        mcp.WithString("process_id", mcp.Required()),
        mcp.WithString("data", mcp.Required()),
    ),
    func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
        args := req.GetArguments()
        device, _ := args["device"].(string)
        pid, _ := args["process_id"].(string)
        data, _ := args["data"].(string)
        if err := c.Send(&protocol.Stdin{Target: device, ProcessID: pid, Data: []byte(data)}); err != nil {
            return mcp.NewToolResultError(err.Error()), nil
        }
        return mcp.NewToolResultText(`{"ok":true}`), nil
    },
)

// kill_process
m.AddTool(
    mcp.NewTool("kill_process",
        mcp.WithDescription("Terminate a process."),
        mcp.WithString("device", mcp.Required()),
        mcp.WithString("process_id", mcp.Required()),
        mcp.WithString("signal"),
    ),
    func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
        args := req.GetArguments()
        device, _ := args["device"].(string)
        pid, _ := args["process_id"].(string)
        sig, _ := args["signal"].(string)
        if sig == "" {
            sig = "TERM"
        }
        id := NewMsgID()
        ch := c.rpc.Register(id)
        defer c.rpc.Unregister(id)
        if err := c.Send(&protocol.Kill{MsgID: id, Target: device, ProcessID: pid, Signal: sig}); err != nil {
            return mcp.NewToolResultError(err.Error()), nil
        }
        select {
        case reply := <-ch:
            if !reply.OK {
                return mcp.NewToolResultError(reply.Error), nil
            }
            return mcp.NewToolResultText(`{"ok":true}`), nil
        case <-time.After(defaultRPCTimeout):
            return mcp.NewToolResultError("timeout"), nil
        case <-ctx.Done():
            return mcp.NewToolResultError(ctx.Err().Error()), nil
        }
    },
)
```

- [ ] **Step 2: Add the fetchDevices helper**

Append in `internal/client/tools_exec.go`:

```go
func fetchDevices(ctx context.Context, c *Conn) ([]string, error) {
    id := NewMsgID()
    ch := c.rpc.Register(id)
    defer c.rpc.Unregister(id)
    if err := c.Send(&protocol.ListDevices{MsgID: id}); err != nil {
        return nil, err
    }
    select {
    case reply := <-ch:
        if !reply.OK {
            return nil, errors.New(reply.Error)
        }
        var out []string
        if list, ok := reply.Data["devices"].([]any); ok {
            for _, d := range list {
                if dm, ok := d.(map[string]any); ok {
                    if h, ok := dm["hostname"].(string); ok {
                        out = append(out, h)
                    }
                }
            }
        }
        return out, nil
    case <-time.After(defaultRPCTimeout):
        return nil, errors.New("timeout")
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}
```

- [ ] **Step 3: Add a start_process + list_processes round-trip test**

Append to `internal/client/tools_exec_test.go`:

```go
func TestStartAndListProcesses(t *testing.T) {
    c, _, cleanup := setupClusterWithClient(t)
    defer cleanup()

    // start_process
    pid := NewMsgID()
    id := NewMsgID()
    ch := c.rpc.Register(id)
    require.NoError(t, c.Send(&protocol.Start{
        MsgID: id, Target: "n1", ProcessID: pid,
        Cmd: []string{"sh", "-c", "sleep 0.2"},
    }))
    select {
    case r := <-ch:
        require.True(t, r.OK)
    case <-time.After(2 * time.Second):
        t.Fatal("start timed out")
    }
    c.rpc.Unregister(id)

    // list_processes
    id = NewMsgID()
    ch = c.rpc.Register(id)
    require.NoError(t, c.Send(&protocol.List{MsgID: id, Target: "n1"}))
    select {
    case r := <-ch:
        require.True(t, r.OK)
        b, _ := json.Marshal(r.Data)
        require.Contains(t, string(b), pid)
    case <-time.After(2 * time.Second):
        t.Fatal("list timed out")
    }
}
```

- [ ] **Step 4: Run tests**

```
go test ./internal/client/... -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add internal/client/
git commit -m "client: implement remaining 5 MCP tools (start/list/get_output/stdin/kill)"
```

---

## Task 9: tether mcp subcommand + main.go wiring

**Files:**
- Create: `internal/cli/mcp.go`
- Modify: `main.go`

- [ ] **Step 1: Write cli/mcp.go**

Create `internal/cli/mcp.go`:

```go
package cli

import (
    "context"
    "flag"
    "fmt"
    "io"
    "os"
    "os/signal"
    "syscall"

    "github.com/qiuxiang/tether/internal/client"
)

func MCP(args []string, stderr io.Writer) int {
    fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
    configPath := fs.String("config", expandHome("~/.config/tether/client.yaml"), "Path to client config")
    if err := fs.Parse(args); err != nil {
        return 2
    }
    cfg, err := client.Load(*configPath)
    if err != nil {
        fmt.Fprintln(stderr, err)
        return 1
    }
    c := client.NewConn(*cfg)

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    go c.Run(ctx)
    if err := c.WaitReady(ctx); err != nil {
        fmt.Fprintln(stderr, "hub not reachable:", err)
        return 1
    }

    s := client.NewMCPServer(c)
    if err := s.Serve(ctx, os.Stdin, os.Stdout); err != nil {
        fmt.Fprintln(stderr, err)
        return 1
    }
    return 0
}
```

- [ ] **Step 2: Wire main.go**

Edit `main.go`:

```go
package main

import (
    "fmt"
    "io"
    "os"

    "github.com/qiuxiang/tether/internal/cli"
)

func main() { os.Exit(run(os.Args, os.Stderr)) }

func run(args []string, stderr io.Writer) int {
    if len(args) < 2 {
        fmt.Fprintln(stderr, "usage: tether <serve|join|mcp> [flags]")
        return 2
    }
    switch args[1] {
    case "serve":
        return cli.Serve(args[2:], stderr)
    case "join":
        return cli.Join(args[2:], stderr)
    case "mcp":
        return cli.MCP(args[2:], stderr)
    default:
        fmt.Fprintf(stderr, "unknown subcommand: %s\n", args[1])
        return 2
    }
}
```

- [ ] **Step 3: Build the binary**

```
go build ./...
```

Expected: PASS (no compile errors anywhere).

- [ ] **Step 4: Commit**

```
git add internal/cli/mcp.go main.go
git commit -m "cli: add tether mcp subcommand"
```

---

## Task 10: Update README and systemd templates

**Files:**
- Modify: `README.md`
- Modify: `dist/systemd/tether-hub.service` (no functional change; verify still valid)
- Create: `dist/systemd/tether-mcp.service` (user unit example, optional but useful for docs)

- [ ] **Step 1: Rewrite the relevant README sections**

Replace the "Wire up Claude Code" section in `README.md` and the "Run the hub" section with content reflecting the new model:

```markdown
## Run the hub (public-net machine)

Create `/etc/tether/config.yaml`:

\```yaml
listen: ":7000"
token: "your-secret-token"
\```

Run: `./tether serve --config /etc/tether/config.yaml`

Put nginx/caddy in front of port 7000 for TLS. The hub serves three paths:
- `/device` — WSS endpoint for nodes (`tether join`)
- `/client` — WSS endpoint for MCP control clients (`tether mcp`)
- `/health` — liveness probe

## Run the MCP client (your local machine)

Create `~/.config/tether/client.yaml`:

\```yaml
hub_url: "wss://tether.example.com/client"
token: "your-secret-token"
\```

The `tether mcp` subcommand runs a stdio MCP server that connects to the hub.

## Wire up Claude Code

\```json
{
  "mcpServers": {
    "tether": {
      "command": "/usr/local/bin/tether",
      "args": ["mcp"]
    }
  }
}
\```
```

(Use real backticks in the file — the leading `\` here is to escape them inside this plan doc.)

Delete or rewrite the line listing `Authorization: Bearer …` for `/mcp` and the tool list (the seven tools remain the same; mention `file_transfer` is coming in Phase 2 if desired or leave for Phase 2 to add).

- [ ] **Step 2: Create the optional user systemd unit**

Create `dist/systemd/tether-mcp.service`:

```ini
# Example user-mode unit. Install to ~/.config/systemd/user/ and run with
#   systemctl --user enable --now tether-mcp.service
# Note: Claude Code typically spawns `tether mcp` directly, so this unit is
# only useful if you want to keep a long-lived MCP process for other clients.
[Unit]
Description=Tether MCP client (stdio)
After=network-online.target

[Service]
ExecStart=/usr/local/bin/tether mcp
Restart=on-failure
RestartSec=2s

[Install]
WantedBy=default.target
```

- [ ] **Step 3: Commit**

```
git add README.md dist/systemd/
git commit -m "docs: update README and systemd for new tether mcp mode"
```

---

## Task 11: Rewrite e2e_test.go to drive via tether mcp / client package

**Files:**
- Modify: `e2e_test.go`

### Why

The existing test calls `s.CallExecForTest`, an exported hook on the Hub that no longer exists. Replace it with a real client-side path through the `internal/client` package.

- [ ] **Step 1: Rewrite the test**

Replace `e2e_test.go` contents:

```go
package main

import (
    "context"
    "net/http/httptest"
    "strings"
    "testing"
    "time"

    "github.com/qiuxiang/tether/internal/client"
    "github.com/qiuxiang/tether/internal/hub"
    "github.com/qiuxiang/tether/internal/node"
    "github.com/qiuxiang/tether/internal/protocol"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestE2EExec(t *testing.T) {
    s := hub.NewServer(hub.Options{Token: "secret"})
    ts := httptest.NewServer(s.Handler())
    defer ts.Close()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Node
    nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
    nc := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host"})
    nc.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
    go nc.Run(ctx)
    require.Eventually(t, func() bool {
        _, ok := s.Registry().Get("e2e-host")
        return ok
    }, 2*time.Second, 20*time.Millisecond)

    // Client (uses internal/client directly without running the stdio MCP server)
    cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
    c := client.NewConn(client.Config{HubURL: cliURL, Token: "secret"})
    go c.Run(ctx)
    cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
    require.NoError(t, c.WaitReady(cctx))
    ccancel()

    // Send an Exec, collect output, expect "hello".
    id := client.NewMsgID()
    ch := c.RPC().RegisterStream(id)
    require.NoError(t, c.Send(&protocol.Exec{
        MsgID: id, Target: "e2e-host",
        Cmd: []string{"sh", "-c", "echo hello"},
        TimeoutMs: 5000,
    }))
    var stdout []byte
    deadline := time.After(3 * time.Second)
    for {
        select {
        case m, ok := <-ch:
            if !ok {
                t.Fatalf("channel closed before ExecExit; stdout=%q", stdout)
            }
            switch v := m.(type) {
            case *protocol.ExecOutput:
                stdout = append(stdout, v.Data...)
            case *protocol.ExecExit:
                assert.Equal(t, 0, v.Code)
                assert.Contains(t, string(stdout), "hello")
                return
            }
        case <-deadline:
            t.Fatal("exec timed out")
        }
    }
}
```

- [ ] **Step 2: Run all tests**

```
go test ./...
```

Expected: PASS for every package.

- [ ] **Step 3: Commit**

```
git add e2e_test.go
git commit -m "e2e: drive via internal/client over /client WSS path"
```

---

## Task 12: Tidy go.mod and final build verification

**Files:**
- Modify: `go.mod`, `go.sum` (via `go mod tidy`)

- [ ] **Step 1: Run go mod tidy**

```
go mod tidy
```

`github.com/mark3labs/mcp-go` should now be a direct dep (used by `internal/client`), no longer indirect.

- [ ] **Step 2: Build all platforms locally**

```
go build ./...
```

Expected: PASS.

- [ ] **Step 3: Run the full suite one more time**

```
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 4: Commit**

```
git add go.mod go.sum
git commit -m "deps: tidy after mcp-go moves to client package"
```

---

## Acceptance Criteria

- `go build ./...` succeeds.
- `go test ./... -count=1` passes.
- `/mcp` endpoint is gone (curl on a running hub returns 404).
- Connecting Claude Code via stdio (`command: /usr/local/bin/tether`, `args: ["mcp"]`) exposes the same seven tools with identical behavior to before.
- `internal/hub/mcp.go` and `internal/hub/mcp_test.go` no longer exist.
- `internal/hub/` no longer imports `github.com/mark3labs/mcp-go`.
- `internal/client/` is a new package with the seven tools.
