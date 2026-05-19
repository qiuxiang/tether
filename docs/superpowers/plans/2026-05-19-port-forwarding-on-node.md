# Port Forwarding on Node Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Relocate port-forwarding rules from `client.yaml` (consumed by `tether mcp`) to node config `config.yaml` (consumed by `tether join`). The mcp client stops handling forwarding entirely; nodes own rules and emit `forward_listen`/`forward_dial` themselves.

**Why:** Multiple `tether mcp` instances on the same machine collide on local listener ports. Forwarding is a per-machine concern, not a per-MCP-session concern.

**Architecture changes:**
- `config.Node.Forwards` field; `tether join` parses + starts forwards after handshake.
- `ForwardHandler` (node, currently passive) grows an active half: `InitRules`, `ResendListens`, `OnDeviceOnline`, `OnReply`. Old client-side `ForwardManager` is deleted.
- Hub `broadcastDeviceEvent` fans device_online/offline to nodes too (so they can recover R rules when a peer comes back).
- Hub drops `forward_*` dispatch from the client WS path and the forwards cleanup on client disconnect.
- `internal/client/forward.go` + `forward_test.go` + the `Forwards` field on client config + the RPC `SetForwardHandler` integration are deleted.

**Tech Stack:** Go, existing protocol/CBOR layer, no new dependencies.

**Spec context:** `docs/superpowers/specs/2026-05-19-port-forwarding-design.md` (original client-side design — most architecture carries over; the rule-ownership boundary moves from "mcp client" to "node").

---

## File Structure

**New:**
- (none)

**Modify:**
- `internal/config/config.go` — `Node.Forwards []forward.Rule` + parse
- `internal/config/config_test.go` — new node-config forwards tests
- `internal/node/forward.go` — extend `ForwardHandler` with active half
- `internal/node/forward_test.go` — tests for active half
- `internal/node/handler.go` — route `Event`/`Reply` to ForwardHandler; expose `ForwardHandler()` accessor
- `internal/node/client.go` — add `OnConnected` callback to `Config`; invoke after handshake
- `internal/cli/join.go` — wire `cfg.Forwards` into ProcessHandler/ForwardHandler at startup
- `internal/hub/server.go` — `broadcastDeviceEvent` also fans to nodes
- `internal/hub/device_events_test.go` — new test for the node-side broadcast
- `internal/hub/client_ws.go` — delete forward dispatch case + delete forwards cleanup in disconnect defer
- `internal/hub/forward_dispatch_test.go` — **delete file** (no longer relevant; forwards never originate from clients)
- `e2e_test.go` — rewrite `TestE2EForwardLocal` / `TestE2EForwardRemote` using a two-node setup (or self-loop)

**Delete:**
- `internal/client/forward.go`
- `internal/client/forward_test.go`

**Modify (cleanup):**
- `internal/client/config.go` — remove `Forwards` field, `forward` import, the `rawConfig` shadow struct
- `internal/client/config_test.go` — remove `TestLoadForwards*` tests
- `internal/client/rpc.go` — remove `forward func(protocol.Message)`, `SetForwardHandler`, the forward case in `Deliver`, and the Reply-fanout to forward handler
- `internal/cli/mcp.go` — remove `NewForwardManager`, `SetForwardHandler`, `fm.Start`, `defer fm.Stop()`
- `README.md` — move "Port forwarding" section to describe node-side config
- `docs/design.md` — note that forwards now live in node config

---

## Task 1: Hub — broadcast device events to nodes

**Goal:** When a device registers/unregisters, fan `device_online`/`device_offline` events to *all* nodes too (currently only clients receive them).

**Files:**
- Modify: `internal/hub/server.go`
- Modify: `internal/hub/device_events_test.go`

- [ ] **Step 1: Add failing test**

Append to `internal/hub/device_events_test.go`:

```go
func TestDeviceOnlineBroadcastToNodes(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	other := &fakePeer{}
	s.Registry().Register(&Device{Hostname: "other-node", Conn: other})

	s.broadcastDeviceEvent("device_online", "mac")

	if len(other.sent) != 1 {
		t.Fatalf("other node should have received 1 event, got %d", len(other.sent))
	}
	msg, err := protocol.Decode(other.sent[0])
	if err != nil {
		t.Fatal(err)
	}
	ev, ok := msg.(*protocol.Event)
	if !ok || ev.Kind != "device_online" || ev.Device != "mac" {
		t.Fatalf("wrong event: %+v", msg)
	}
}
```

- [ ] **Step 2: Run, expect failure**

```
go test ./internal/hub/ -run TestDeviceOnlineBroadcastToNodes -v
```
Expected: FAIL (`got 0`).

- [ ] **Step 3: Extend broadcastDeviceEvent**

In `internal/hub/server.go`, find `broadcastDeviceEvent` and add a second fan-out loop:

```go
func (s *Server) broadcastDeviceEvent(kind, hostname string) {
	ev := &protocol.Event{Kind: kind, Device: hostname}
	raw, err := protocol.Encode(ev)
	if err != nil {
		log.Printf("encode device event: %v", err)
		return
	}
	for _, c := range s.clients.List() {
		if c.Conn != nil {
			_ = c.Conn.SendRaw(raw)
		}
	}
	for _, d := range s.registry.List() {
		if d.Hostname == hostname || d.Conn == nil {
			continue
		}
		_ = d.Conn.SendRaw(raw)
	}
}
```

(Skip echoing the event to the very device it's about — they don't need their own online notice.)

- [ ] **Step 4: Run all hub tests**

```
go test ./internal/hub/ -v
```
Expected: all pass.

- [ ] **Step 5: Commit**

```
git add internal/hub/server.go internal/hub/device_events_test.go
git commit -m "hub: broadcast device events to nodes too"
```

---

## Task 2: Node config — `forwards` field

**Goal:** Add `Forwards []forward.Rule` to `config.Node`, parsed from a `forwards: []string` YAML field at load time via `forward.ParseAll`.

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`

- [ ] **Step 1: Add failing tests**

Append to `internal/config/config_test.go`:

```go
func TestLoadNodeForwards(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	body := "hub_url: ws://x\ntoken: t\nforwards:\n  - \"L 9000:mac:5037\"\n  - \"R mac:8080:3000\"\n"
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	c, err := LoadNode(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Forwards) != 2 {
		t.Fatalf("got %d rules", len(c.Forwards))
	}
	if c.Forwards[0].Dir != forward.DirLocal || c.Forwards[1].Dir != forward.DirRemote {
		t.Fatalf("dirs wrong: %+v", c.Forwards)
	}
}

func TestLoadNodeForwardsInvalid(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	body := "hub_url: ws://x\ntoken: t\nforwards: [\"L bogus\"]\n"
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadNode(p); err == nil {
		t.Fatal("expected parse error")
	}
}
```

Add to top imports: `"os"`, `"path/filepath"`, `"github.com/qiuxiang/tether/internal/forward"` (some may already be present).

- [ ] **Step 2: Update `config.Node` + `LoadNode`**

In `internal/config/config.go`, replace the `Node` struct and `LoadNode`:

```go
type Node struct {
	HubURL           string         `yaml:"hub_url"`
	Token            string         `yaml:"token"`
	HostnameOverride string         `yaml:"hostname_override"`
	LogDir           string         `yaml:"log_dir"`
	Forwards         []forward.Rule `yaml:"-"`
}

type rawNode struct {
	HubURL           string   `yaml:"hub_url"`
	Token            string   `yaml:"token"`
	HostnameOverride string   `yaml:"hostname_override"`
	LogDir           string   `yaml:"log_dir"`
	Forwards         []string `yaml:"forwards"`
}

func LoadNode(path string) (*Node, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw rawNode
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	if raw.HubURL == "" {
		return nil, errors.New("config: hub_url is required")
	}
	if raw.Token == "" {
		return nil, errors.New("config: token is required")
	}
	rules, err := forward.ParseAll(raw.Forwards)
	if err != nil {
		return nil, err
	}
	logDir := raw.LogDir
	if logDir == "" {
		home, _ := os.UserHomeDir()
		logDir = filepath.Join(home, ".local", "share", "tether", "logs")
	}
	return &Node{
		HubURL:           raw.HubURL,
		Token:            raw.Token,
		HostnameOverride: raw.HostnameOverride,
		LogDir:           logDir,
		Forwards:         rules,
	}, nil
}
```

Add `"github.com/qiuxiang/tether/internal/forward"` to imports.

- [ ] **Step 3: Run config tests**

```
go test ./internal/config/ -v
go build ./...
```
Expected: pass, clean build.

- [ ] **Step 4: Commit**

```
git add internal/config/
git commit -m "config: parse node forwards rules"
```

---

## Task 3: Node ForwardHandler — active rules support

**Goal:** Extend `internal/node/forward.go::ForwardHandler` with the active half (binding L listeners, issuing `forward_listen` for R rules, handling dial-back, R-rule recovery on device events, Reply-on-failure). This unifies passive and active forwarding under one node-side type that owns the shared `streams`/`listeners` maps.

**Files:**
- Modify: `internal/node/forward.go`
- Modify: `internal/node/forward_test.go`

### Design notes

After this task `ForwardHandler` owns four maps (already has the first two):
- `listeners map[string]net.Listener` — keyed by `forward_id`. Holds BOTH peer-requested R listeners (existing) and locally-owned L listeners (new). Distinguished only by who created the entry.
- `streams map[string]*forwardStream` — keyed by `stream_id`. Used uniformly.
- `byForward map[string]forward.Rule` — keyed by `forward_id`, only populated for R rules this node owns. Look-up tells `Dial` whether to interpret an incoming `ForwardDial` as a dial-back (we own this rule, dial DestHost:DestPort from our rule) or a peer-driven dial (look at the frame's DestHost:DestPort).
- `streamMsgID map[string]string` — keyed by stream_id, holds the outbound msg_id used in the original ForwardDial for L flows. Used by `OnReply` to map a failed Reply back to a stream to close.

### Step-by-step

- [ ] **Step 1: Add failing tests for the active half**

Append to `internal/node/forward_test.go`:

```go
func TestForwardHandlerStartLocalListener(t *testing.T) {
	send := &fwdCaptureSender{}
	h := NewForwardHandler()
	rule := forward.Rule{Raw: "L 0:peer:99", Dir: forward.DirLocal,
		Bind: "127.0.0.1", ListenPort: 0, Device: "peer",
		DestHost: "127.0.0.1", DestPort: 99}
	h.InitRules([]forward.Rule{rule})
	h.Start(context.Background(), send)

	addr := h.LocalAddr(rule)
	if addr == "" {
		t.Fatal("no L listener bound")
	}
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if !waitFor(t, func() bool {
		send.mu.Lock()
		defer send.mu.Unlock()
		for _, m := range send.msgs {
			if d, ok := m.(*protocol.ForwardDial); ok && d.Target == "peer" && d.DestPort == 99 {
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("no forward_dial emitted, got %+v", send.msgs)
	}
	h.Shutdown()
}

func TestForwardHandlerStartRemoteIssuesListen(t *testing.T) {
	send := &fwdCaptureSender{}
	h := NewForwardHandler()
	rule := forward.Rule{Raw: "R peer:0:127.0.0.1:99", Dir: forward.DirRemote,
		Device: "peer", Bind: "127.0.0.1", ListenPort: 0,
		DestHost: "127.0.0.1", DestPort: 99}
	h.InitRules([]forward.Rule{rule})
	h.Start(context.Background(), send)

	if !waitFor(t, func() bool {
		send.mu.Lock()
		defer send.mu.Unlock()
		for _, m := range send.msgs {
			if fl, ok := m.(*protocol.ForwardListen); ok && fl.Target == "peer" {
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("no forward_listen emitted, got %+v", send.msgs)
	}
	h.Shutdown()
}

func TestForwardHandlerOnDeviceOnlineReissuesRemote(t *testing.T) {
	send := &fwdCaptureSender{}
	h := NewForwardHandler()
	rule := forward.Rule{Raw: "R peer:0:127.0.0.1:99", Dir: forward.DirRemote,
		Device: "peer", Bind: "127.0.0.1", ListenPort: 0,
		DestHost: "127.0.0.1", DestPort: 99}
	h.InitRules([]forward.Rule{rule})
	h.Start(context.Background(), send)
	waitFor(t, func() bool { send.mu.Lock(); defer send.mu.Unlock(); return len(send.msgs) >= 1 }, time.Second)
	send.mu.Lock()
	send.msgs = nil
	send.mu.Unlock()

	h.OnDeviceOnline("peer", send)
	if !waitFor(t, func() bool {
		send.mu.Lock()
		defer send.mu.Unlock()
		for _, m := range send.msgs {
			if _, ok := m.(*protocol.ForwardListen); ok {
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("no re-issue on device_online: %+v", send.msgs)
	}
	h.Shutdown()
}

func TestForwardHandlerDialBackEcho(t *testing.T) {
	// echo on the "client side" (i.e., our local node)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 256)
		for {
			n, err := c.Read(buf)
			if err != nil {
				return
			}
			c.Write(buf[:n])
		}
	}()
	host, port := splitHostPortTest(t, ln.Addr().String())

	send := &fwdCaptureSender{}
	h := NewForwardHandler()
	rule := forward.Rule{Raw: "R peer:0:" + host + ":" + strconv.Itoa(port),
		Dir: forward.DirRemote, Device: "peer", Bind: "127.0.0.1", ListenPort: 0,
		DestHost: host, DestPort: port}
	h.InitRules([]forward.Rule{rule})
	h.Start(context.Background(), send)

	// Pull out the forward_id assigned to the R rule.
	var fid string
	waitFor(t, func() bool {
		send.mu.Lock()
		defer send.mu.Unlock()
		for _, m := range send.msgs {
			if fl, ok := m.(*protocol.ForwardListen); ok {
				fid = fl.ForwardID
				return true
			}
		}
		return false
	}, time.Second)
	if fid == "" {
		t.Fatal("no forward_listen captured")
	}

	// Simulate peer-side accept asking us to dial-back.
	h.Dial(send, &protocol.ForwardDial{MsgID: "m1", StreamID: "s1", ForwardID: fid})
	if !waitFor(t, func() bool {
		send.mu.Lock()
		defer send.mu.Unlock()
		for _, m := range send.msgs {
			if r, ok := m.(*protocol.Reply); ok && r.MsgID == "m1" && r.OK {
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("no ok reply for dial-back")
	}

	// Push some data through, expect echo.
	h.Data(send, &protocol.ForwardData{StreamID: "s1", Data: []byte("ping")})
	if !waitFor(t, func() bool {
		send.mu.Lock()
		defer send.mu.Unlock()
		for _, m := range send.msgs {
			if d, ok := m.(*protocol.ForwardData); ok && string(d.Data) == "ping" {
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("no echo back")
	}
	h.Shutdown()
}
```

Add to imports: `"context"`, `"strconv"`. (`net`, `time`, `protocol`, `forward` likely already present; add if not.)

- [ ] **Step 2: Run, expect failure**

```
go test ./internal/node/ -run TestForwardHandler -v
```
Expected: FAIL on undefined `InitRules`, `Start`, `LocalAddr`, `OnDeviceOnline`.

- [ ] **Step 3: Extend ForwardHandler**

In `internal/node/forward.go`, modify the struct to add new fields and methods. Keep all existing methods (`Listen`, `Unlisten`, `Data`, `Close`, `Shutdown`, `attachStream`, `readPump`, `closeStream`, `acceptLoop`) — they remain valid for the passive paths.

```go
type ForwardHandler struct {
	mu          sync.Mutex
	listeners   map[string]net.Listener
	streams     map[string]*forwardStream
	rules       []forward.Rule
	byForward   map[string]forward.Rule // forward_id → R rule (we own it)
	localAddrs  map[string]string       // L rule.Raw → bound addr (for tests / debugging)
	streamMsg   map[string]string       // stream_id → msg_id (L flows, for Reply-on-failure)
	closed      bool
}

func NewForwardHandler() *ForwardHandler {
	return &ForwardHandler{
		listeners:  map[string]net.Listener{},
		streams:    map[string]*forwardStream{},
		byForward:  map[string]forward.Rule{},
		localAddrs: map[string]string{},
		streamMsg:  map[string]string{},
	}
}

func newForwardID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// InitRules stores rules and pre-allocates stable forward_ids for R rules.
// Call once at boot. L listeners are not bound here — call Start with a Sender.
func (h *ForwardHandler) InitRules(rules []forward.Rule) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rules = rules
	for _, r := range rules {
		if r.Dir == forward.DirRemote {
			fid := newForwardID()
			h.byForward[fid] = r
		}
	}
}

// Start binds local L listeners and sends forward_listen for R rules.
// Safe to call multiple times — re-sends forward_listen on each call so a
// node can call this after every successful (re)connect.
func (h *ForwardHandler) Start(ctx context.Context, send Sender) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	rules := append([]forward.Rule(nil), h.rules...)
	h.mu.Unlock()

	for _, r := range rules {
		switch r.Dir {
		case forward.DirLocal:
			h.startLocal(ctx, send, r)
		case forward.DirRemote:
			h.resendRemote(send, r)
		}
	}
}

// ResendListens re-issues forward_listen frames for all R rules using the
// current sender. Use after reconnect.
func (h *ForwardHandler) ResendListens(send Sender) {
	h.mu.Lock()
	rules := append([]forward.Rule(nil), h.rules...)
	h.mu.Unlock()
	for _, r := range rules {
		if r.Dir == forward.DirRemote {
			h.resendRemote(send, r)
		}
	}
}

// OnDeviceOnline re-issues forward_listen for R rules whose Device == host.
func (h *ForwardHandler) OnDeviceOnline(host string, send Sender) {
	h.mu.Lock()
	rules := append([]forward.Rule(nil), h.rules...)
	h.mu.Unlock()
	for _, r := range rules {
		if r.Dir == forward.DirRemote && r.Device == host {
			h.resendRemote(send, r)
		}
	}
}

// OnReply maps a failed Reply back to a local stream and tears it down.
func (h *ForwardHandler) OnReply(send Sender, m *protocol.Reply) {
	if m.OK {
		return
	}
	h.mu.Lock()
	var sid string
	for s, mid := range h.streamMsg {
		if mid == m.MsgID {
			sid = s
			break
		}
	}
	h.mu.Unlock()
	if sid == "" {
		return
	}
	h.closeStream(sid)
}

// LocalAddr returns the bound TCP address for a local L rule (or "" if not bound).
func (h *ForwardHandler) LocalAddr(r forward.Rule) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.localAddrs[r.Raw]
}

func (h *ForwardHandler) startLocal(ctx context.Context, send Sender, r forward.Rule) {
	h.mu.Lock()
	if _, dup := h.localAddrs[r.Raw]; dup {
		h.mu.Unlock()
		return // already bound from a previous Start
	}
	h.mu.Unlock()

	ln, err := net.Listen("tcp", r.ListenAddr())
	if err != nil {
		log.Printf("forward L %s: bind failed: %v", r.Raw, err)
		return
	}
	h.mu.Lock()
	h.localAddrs[r.Raw] = ln.Addr().String()
	h.mu.Unlock()
	log.Printf("forward L %s: listening on %s", r.Raw, ln.Addr())

	go h.localAcceptLoop(ctx, send, r, ln)
}

func (h *ForwardHandler) localAcceptLoop(_ context.Context, send Sender, r forward.Rule, ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		sid := newForwardID() // reuse the id format
		h.mu.Lock()
		h.streams[sid] = &forwardStream{conn: c}
		h.streamMsg[sid] = sid // msg_id = sid for the outgoing ForwardDial
		h.mu.Unlock()
		_ = send.Send(&protocol.ForwardDial{
			MsgID: sid, Target: r.Device, StreamID: sid,
			DestHost: r.DestHost, DestPort: r.DestPort,
		})
		go h.readPump(sid, h.streams[sid], send)
	}
}

func (h *ForwardHandler) resendRemote(send Sender, r forward.Rule) {
	h.mu.Lock()
	var fid string
	for id, rr := range h.byForward {
		if rr.Raw == r.Raw {
			fid = id
			break
		}
	}
	h.mu.Unlock()
	if fid == "" {
		return // InitRules should have populated it
	}
	_ = send.Send(&protocol.ForwardListen{
		MsgID: fid, Target: r.Device, ForwardID: fid,
		ListenAddr: r.ListenAddr(),
		DestHost:   r.DestHost, DestPort: r.DestPort,
	})
}
```

Modify the existing `Dial` to handle the dial-back path:

```go
func (h *ForwardHandler) Dial(send Sender, m *protocol.ForwardDial) {
	// If ForwardID matches one of our R rules, this is a dial-back.
	h.mu.Lock()
	r, isDialBack := h.byForward[m.ForwardID]
	h.mu.Unlock()

	var addr string
	if isDialBack {
		addr = net.JoinHostPort(r.DestHost, strconv.Itoa(r.DestPort))
	} else {
		addr = net.JoinHostPort(m.DestHost, strconv.Itoa(m.DestPort))
	}
	c, err := net.Dial("tcp", addr)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "dial: " + err.Error()})
		return
	}
	h.attachStream(m.StreamID, c, send)
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true})
}
```

Also modify `Shutdown` to set `h.closed = true`:

```go
func (h *ForwardHandler) Shutdown() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	lns := h.listeners
	streams := h.streams
	addrs := h.localAddrs
	h.listeners = map[string]net.Listener{}
	h.streams = map[string]*forwardStream{}
	h.localAddrs = map[string]string{}
	h.mu.Unlock()

	for _, ln := range lns {
		ln.Close()
	}
	// Also close any L-side listeners stored only in localAddrs — they were
	// not added to h.listeners. Iterate previously-bound addresses; we don't
	// keep refs, so rely on accept-loop exit when the goroutine sees error.
	_ = addrs
	for _, st := range streams {
		st.closeOnce.Do(func() { st.conn.Close() })
	}
}
```

Wait — L listeners need explicit close too. Adjust `startLocal` to also store the listener:

In `startLocal`, after binding, also do:
```go
h.mu.Lock()
h.listeners["L:"+r.Raw] = ln
h.localAddrs[r.Raw] = ln.Addr().String()
h.mu.Unlock()
```

So Shutdown closing `h.listeners` also closes L listeners.

Adjust imports in `forward.go`: add `"context"` if not already there.

- [ ] **Step 4: Run all node tests with race**

```
go test ./internal/node/ -race -v
go build ./...
```
Expected: pass.

- [ ] **Step 5: Commit**

```
git add internal/node/forward.go internal/node/forward_test.go
git commit -m "node: active forwarding — InitRules, Start, OnDeviceOnline, OnReply"
```

---

## Task 4: Node Handler — route Event and Reply through ForwardHandler

**Goal:** Wire `*protocol.Event` (for device_online) and `*protocol.Reply` (for dial-failure) into `ProcessHandler.Handle` so they reach the ForwardHandler. Expose a `ForwardHandler()` accessor on `ProcessHandler` for the CLI wiring.

**Files:**
- Modify: `internal/node/handler.go`

- [ ] **Step 1: Extend Handle switch**

In `internal/node/handler.go::ProcessHandler.Handle`, add two cases (anywhere among the existing cases):

```go
	case *protocol.Event:
		if m.Kind == "device_online" {
			h.forwardHandler.OnDeviceOnline(m.Device, send)
		}
	case *protocol.Reply:
		h.forwardHandler.OnReply(send, m)
```

- [ ] **Step 2: Add accessor**

In `internal/node/handler.go`, add:

```go
// ForwardHandler returns the embedded forward handler so callers (e.g. the
// `tether join` CLI) can seed it with rules at startup.
func (h *ProcessHandler) ForwardHandler() *ForwardHandler { return h.forwardHandler }
```

- [ ] **Step 3: Verify build + tests**

```
go build ./...
go test ./internal/node/ -race -v
```
Expected: pass.

- [ ] **Step 4: Commit**

```
git add internal/node/handler.go
git commit -m "node: route Event/Reply to ForwardHandler; expose accessor"
```

---

## Task 5: Node client — `OnConnected` hook

**Goal:** Add a `Config.OnConnected func(send Sender)` callback to `node.Client`, fired right after each successful handshake. ForwardHandler uses it (via the CLI wiring) to re-send `forward_listen` for R rules on every (re)connect.

**Files:**
- Modify: `internal/node/client.go`
- Modify: `internal/node/client_test.go`

- [ ] **Step 1: Add the callback field**

In `internal/node/client.go::Config`, add:

```go
type Config struct {
	HubURL       string
	Token        string
	Hostname     string
	AgentVersion string
	ReconnectMin time.Duration
	ReconnectMax time.Duration
	// OnConnected, if non-nil, is invoked after each successful handshake
	// with a sender bound to the new connection. Used to re-issue stateful
	// requests (forward_listen, etc.) on reconnect.
	OnConnected func(send Sender)
}
```

- [ ] **Step 2: Invoke it after handshake**

In `connectAndServe`, after `c.conn = conn` is set (inside the lock) and after starting the ping loop, add:

```go
	if c.cfg.OnConnected != nil {
		go c.cfg.OnConnected(c)
	}
```

(`c` implements `Sender`.)

- [ ] **Step 3: Add test**

Append to `internal/node/client_test.go`:

```go
func TestClientOnConnectedFired(t *testing.T) {
	// Spin up a tiny WS server that completes the handshake.
	called := make(chan struct{}, 1)
	ts := newFakeHubServer(t, func(c *websocket.Conn, ctx context.Context) {
		// Read Hello.
		_, _, err := c.Read(ctx)
		if err != nil {
			return
		}
		<-ctx.Done()
	})
	defer ts.Close()

	cli := New(Config{
		HubURL:   strings.Replace(ts.URL, "http", "ws", 1),
		Token:    "x", Hostname: "h1",
		OnConnected: func(_ Sender) { select { case called <- struct{}{}: default: } },
		ReconnectMin: 10 * time.Millisecond,
		ReconnectMax: 50 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go cli.Run(ctx)

	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("OnConnected not called")
	}
}
```

If `newFakeHubServer` (or equivalent test helper) doesn't already exist in `client_test.go`, look at the existing tests in that file — they likely already spin up a fake WS server. Reuse the existing pattern; if there is none, write a small one inline using `httptest.NewServer` + `websocket.Accept`.

- [ ] **Step 4: Run tests**

```
go test ./internal/node/ -race -v -run TestClientOnConnectedFired
go test ./internal/node/ -race -v
```
Expected: pass.

- [ ] **Step 5: Commit**

```
git add internal/node/client.go internal/node/client_test.go
git commit -m "node: OnConnected callback fires after each successful handshake"
```

---

## Task 6: Wire forwards into `tether join`

**Goal:** `internal/cli/join.go` reads `cfg.Forwards`, calls `ph.ForwardHandler().InitRules(cfg.Forwards)` before `Run`, and sets `node.Config.OnConnected` to `ph.ForwardHandler().ResendListens`. Then forwards are live as soon as the first handshake completes, and self-heal on reconnect.

**Files:**
- Modify: `internal/cli/join.go`

- [ ] **Step 1: Update join.go**

Replace the body of `Join` between the `cfg, err :=` line and `cli.Run(ctx)` with:

```go
	host := cfg.HostnameOverride
	if host == "" {
		host, _ = os.Hostname()
	}
	ph := node.NewProcessHandler(cfg.LogDir, 50)
	ph.ForwardHandler().InitRules(cfg.Forwards)

	cli := node.New(node.Config{
		HubURL:   cfg.HubURL,
		Token:    cfg.Token,
		Hostname: host,
		OnConnected: func(send node.Sender) {
			ph.ForwardHandler().Start(context.Background(), send)
		},
	})
	cli.SetHandler(ph)
```

(`Start` is idempotent across reconnects: L listeners are skipped if already bound, R rules just re-emit `forward_listen`.)

- [ ] **Step 2: Build**

```
go build ./...
go vet ./...
```
Expected: clean.

- [ ] **Step 3: Commit**

```
git add internal/cli/join.go
git commit -m "cli: wire node forwards into tether join"
```

---

## Task 7: Hub — drop client-side forward dispatch

**Goal:** Remove forward-frame routing from the client WS path. Clients no longer send forward frames, so there's no need to handle them. Also remove the forwards-cleanup defer block on client disconnect.

**Files:**
- Modify: `internal/hub/client_ws.go`
- Delete: `internal/hub/forward_dispatch_test.go`

- [ ] **Step 1: Remove the forward case + dispatchForward**

In `internal/hub/client_ws.go`, find the dispatch switch (look for `case *protocol.ForwardListen` or similar) and delete:

```go
	case *protocol.ForwardListen, *protocol.ForwardUnlisten, *protocol.ForwardDial,
		*protocol.ForwardData, *protocol.ForwardClose:
		cs.dispatchForward(raw, msg, cs)
```

Delete the entire `dispatchForward` method on `clientSession`.

- [ ] **Step 2: Remove forwards cleanup on client disconnect**

In the same file, find the `defer func() { ... }()` block in `handleClient` that runs `s.forwards.RemoveListenersForClient(sess)` and `s.forwards.EvictStreamsForClient(sess)`. Delete those two `for` blocks (the encode/fanout to nodes too). Keep the rest of the defer intact.

- [ ] **Step 3: Delete the now-irrelevant test file**

```
git rm internal/hub/forward_dispatch_test.go
```

(The tests in that file all exercised `clientSession.dispatchForward`, which no longer exists.)

- [ ] **Step 4: Run hub tests**

```
go test ./internal/hub/ -race -v
go build ./...
```
Expected: pass; build clean. If anything else in the hub still references `dispatchForward`, delete those references.

- [ ] **Step 5: Commit**

```
git add internal/hub/client_ws.go
git commit -m "hub: drop forward dispatch from client WS"
```

(The `git rm` from Step 3 is staged separately or together — bundle if convenient.)

---

## Task 8: Hub — keep `ForwardTable` operations driven by node side

**Goal:** Verify that the hub's existing forward routing still works when both `AddListener` / `OpenStream` are invoked exclusively from the node-WS path (which they should — node→node forwards always start with a `forward_listen` arriving on the *device* WS, not the *client* WS).

The existing `internal/hub/device_ws.go` already routes `forward_listen`/`forward_unlisten`/`forward_dial` etc. coming **from** a node. But there's a gap: in the old design, `forward_listen` arrived on the *client* WS. With nodes now originating them, the device WS must also dispatch them (the data/close path is already handled; listen/unlisten/dial-from-node aren't fully).

**Files:**
- Modify: `internal/hub/device_ws.go`
- Modify: `internal/hub/device_events_test.go` (add a unit test)

- [ ] **Step 1: Audit existing device_ws dispatch**

Read `internal/hub/device_ws.go`'s read loop. It currently handles:
- `*protocol.ForwardDial` — node-originated dial-from-listen (looks up `forward_id → client`) — **this case is for nodes asking clients to dial-back; with no clients in the loop anymore, it's nodes asking other nodes to dial-back. The `LookupListener` returns a node now. Same code; OK.**
- `*protocol.ForwardData` — by stream_id, peer-agnostic. OK.
- `*protocol.ForwardClose` — by stream_id, peer-agnostic. OK.

It does **NOT** currently handle `*protocol.ForwardListen`, `*protocol.ForwardUnlisten`, or client-originated `*protocol.ForwardDial{Target: peer}`. Add them:

- [ ] **Step 2: Add cases for listen / unlisten / dial-to-peer**

In the read loop, alongside the existing forward cases, add (before the generic msg_id routing):

```go
case *protocol.ForwardListen:
	d, ok := s.server.registry.Get(v.Target)
	if !ok || d.Conn == nil {
		_ = s.SendRaw(replyErr(v.MsgID, "device_offline: "+v.Target))
		continue
	}
	s.server.forwards.AddListener(v.ForwardID, s) // `s` = the requesting node session
	s.server.router.Register(v.MsgID, s, false)
	if err := d.Conn.SendRaw(raw); err != nil {
		s.server.forwards.RemoveListener(v.ForwardID)
		s.server.router.Unregister(v.MsgID)
		_ = s.SendRaw(replyErr(v.MsgID, err.Error()))
	}
	continue
case *protocol.ForwardUnlisten:
	s.server.forwards.RemoveListener(v.ForwardID)
	if d, ok := s.server.registry.Get(v.Target); ok && d.Conn != nil {
		_ = d.Conn.SendRaw(raw)
	}
	continue
```

For the existing `*protocol.ForwardDial` case, distinguish:
- If `v.Target != ""` it's a client/origin asking a peer node to dial: route to peer.
- If `v.Target == "" && v.ForwardID != ""` it's an accept-driven dial-back to the listener owner.

Replace the existing case with:

```go
case *protocol.ForwardDial:
	if v.Target != "" {
		// Origin → target node: peer-side dial request.
		d, ok := s.server.registry.Get(v.Target)
		if !ok || d.Conn == nil {
			_ = s.SendRaw(replyErr(v.MsgID, "device_offline: "+v.Target))
			continue
		}
		s.server.forwards.OpenStream(v.StreamID, s, d.Conn)
		s.server.router.Register(v.MsgID, s, false)
		if err := d.Conn.SendRaw(raw); err != nil {
			s.server.forwards.CloseStream(v.StreamID)
			s.server.router.Unregister(v.MsgID)
			_ = s.SendRaw(replyErr(v.MsgID, err.Error()))
		}
		continue
	}
	// Dial-back: peer accepted on a listener owned by some other node;
	// route back via forward_id → owner.
	owner, ok := s.server.forwards.LookupListener(v.ForwardID)
	if !ok {
		continue
	}
	s.server.forwards.OpenStream(v.StreamID, owner, s)
	s.server.router.Register(v.MsgID, owner, false)
	_ = owner.SendRaw(raw)
	continue
```

The helper `replyErr` already exists (or factor one out of `clientSession.sendErrorReply` into a package-level helper). If absent, add:

```go
func replyErr(msgID, errStr string) []byte {
	raw, _ := protocol.Encode(&protocol.Reply{MsgID: msgID, OK: false, Error: errStr})
	return raw
}
```

- [ ] **Step 3: On node disconnect, also evict listeners and notify peers**

In `device_ws.go`'s defer cleanup, add (before the existing eviction of streams):

```go
	for _, fid := range s.server.forwards.RemoveListenersForClient(s) {
		// Tell the device that was hosting the listener (if any are online) to
		// drop it. We don't track which device that was; broadcast to all and
		// let nodes ignore unknown forward_ids. Acceptable cost at current scale.
		unl := &protocol.ForwardUnlisten{ForwardID: fid}
		raw, _ := protocol.Encode(unl)
		for _, d := range s.server.registry.List() {
			if d.Conn != nil && d.Conn != s {
				_ = d.Conn.SendRaw(raw)
			}
		}
	}
```

(`RemoveListenersForClient` is misnamed for the post-refactor world but works for any `PeerConn`. Renaming is out of scope for this plan — leave it.)

- [ ] **Step 4: Add tests**

Append to `internal/hub/device_events_test.go` (or a new file `internal/hub/node_forward_test.go` if the events file is getting large):

```go
func TestNodeForwardListenRegisters(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	target := &fakePeer{}
	origin := &fakePeer{}
	s.Registry().Register(&Device{Hostname: "target", Conn: target})
	s.Registry().Register(&Device{Hostname: "origin", Conn: origin})

	// Build a session impersonating `origin` and feed it a ForwardListen.
	sess := &deviceSession{device: &Device{Hostname: "origin"}, server: s, conn: nil}
	_ = sess // The actual read-loop dispatch is hard to invoke from a unit test
	         // without a full WS; instead, exercise the lower-level table:
	s.forwards.AddListener("f1", origin)
	got, ok := s.forwards.LookupListener("f1")
	if !ok || got != origin {
		t.Fatalf("lookup miss")
	}
}
```

(The dispatch logic itself will be covered by the e2e tests in Task 9 — a unit test here would require re-implementing the WS read loop. The above just verifies the existing table works with PeerConn arguments coming from the node side.)

- [ ] **Step 5: Build + run**

```
go build ./...
go test ./internal/hub/ -race -v
```
Expected: pass, clean.

- [ ] **Step 6: Commit**

```
git add internal/hub/device_ws.go internal/hub/device_events_test.go
git commit -m "hub: route forward_listen/unlisten/dial-to-peer on device WS; evict listeners on node disconnect"
```

---

## Task 9: Delete the client-side forward code

**Goal:** Remove all forwarding code from `internal/client/` and `internal/cli/mcp.go`. After this task, `tether mcp` has no concept of forwards.

**Files:**
- Delete: `internal/client/forward.go`
- Delete: `internal/client/forward_test.go`
- Modify: `internal/client/config.go`
- Modify: `internal/client/config_test.go`
- Modify: `internal/client/rpc.go`
- Modify: `internal/cli/mcp.go`

- [ ] **Step 1: Remove `Forwards` from client config**

In `internal/client/config.go`, revert to the simpler shape pre-Task-3-of-the-original-plan:

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

In `internal/client/config_test.go`, delete `TestLoadForwards` and `TestLoadForwardsInvalid` and their imports of `forward`.

- [ ] **Step 2: Remove the RPC forward path**

In `internal/client/rpc.go`:

- Delete the `forward func(protocol.Message)` field from `RPC`.
- Delete the `SetForwardHandler` method.
- In `Deliver`, delete the `case *protocol.ForwardDial, *protocol.ForwardData, *protocol.ForwardClose, *protocol.Event` case.
- In the `*protocol.Reply` case, delete the fan-to-forward branch — keep only the reply-channel lookup.

- [ ] **Step 3: Delete the client forward files**

```
git rm internal/client/forward.go internal/client/forward_test.go
```

- [ ] **Step 4: Remove ForwardManager wiring from `tether mcp`**

In `internal/cli/mcp.go`, find the block after `WaitReady` that constructs `client.NewForwardManager(...)`, calls `SetForwardHandler`, `fm.Start(ctx)`, `defer fm.Stop()`. Delete all four lines.

- [ ] **Step 5: Build + run**

```
go build ./...
go test ./internal/client/ -v
go test ./internal/cli/ -v 2>/dev/null || true
```
Expected: clean.

- [ ] **Step 6: Commit**

```
git add internal/client/ internal/cli/mcp.go
git commit -m "client,cli: remove client-side port forwarding"
```

---

## Task 10: Rewrite E2E tests

**Goal:** Replace the existing `TestE2EForwardLocal` / `TestE2EForwardRemote` / `TestE2EForwardLocalDialFailure` with node-driven equivalents. A single-node self-loop is enough to exercise the routing.

**Files:**
- Modify: `e2e_test.go`

### Approach

Self-loop test: one hub + one node `e2e-host`. The node's config has rule `L 0:e2e-host:<echo-port>`. The node binds 0 locally, accepts, sends forward_dial{target=e2e-host} to hub, hub routes back to the same node, node's ForwardHandler.Dial fires and dials the local echo. Echo round-trips.

For R: rule `R e2e-host:0:127.0.0.1:<echo-port>` — node asks itself (via hub) to bind. Same path inverted; same machinery.

- [ ] **Step 1: Replace the test functions**

Replace `TestE2EForwardLocal`, `TestE2EForwardRemote`, `TestE2EForwardLocalDialFailure`, and the `echoLoop` helper with:

```go
func TestE2EForwardLocalSelfLoop(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Local echo (target of the forward).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go echoLoop(ln)
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	// Node with an L rule pointing back at itself.
	rule := forward.Rule{
		Raw: "L 0:e2e-host:127.0.0.1:" + portStr,
		Dir: forward.DirLocal, Bind: "127.0.0.1", ListenPort: 0,
		Device: "e2e-host", DestHost: "127.0.0.1", DestPort: port,
	}

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	ph := node.NewProcessHandler(t.TempDir(), 50)
	ph.ForwardHandler().InitRules([]forward.Rule{rule})

	nc := node.New(node.Config{
		HubURL: nodeURL, Token: "secret", Hostname: "e2e-host",
		OnConnected: func(send node.Sender) {
			ph.ForwardHandler().Start(context.Background(), send)
		},
	})
	nc.SetHandler(ph)
	go nc.Run(ctx)

	require.Eventually(t, func() bool { _, ok := s.Registry().Get("e2e-host"); return ok },
		2*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return ph.ForwardHandler().LocalAddr(rule) != "" },
		2*time.Second, 20*time.Millisecond)

	addr := ph.ForwardHandler().LocalAddr(rule)
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("hello"))
	require.NoError(t, err)
	buf := make([]byte, 5)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(buf))

	ph.Shutdown()
}

func TestE2EForwardRemoteSelfLoop(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go echoLoop(ln)
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	rule := forward.Rule{
		Raw: "R e2e-host:0:127.0.0.1:" + portStr,
		Dir: forward.DirRemote, Device: "e2e-host",
		Bind: "127.0.0.1", ListenPort: 0,
		DestHost: "127.0.0.1", DestPort: port,
	}

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	ph := node.NewProcessHandler(t.TempDir(), 50)
	ph.ForwardHandler().InitRules([]forward.Rule{rule})

	// Capture the bound listen_addr from the Reply by patching the handler
	// to intercept (or by polling the node's listener registry directly).
	// Simplest: poll lsof / net.Listen-introspection isn't available; instead,
	// retry-dial through the hub's well-known interface. But for a unit-style
	// test we have access to the node's forward state; expose it via a
	// helper getter:
	//   func (h *ForwardHandler) RemoteAddrs() map[string]string
	// or just sleep + scan reachable ports. To keep this test deterministic
	// without adding production-only test getters, listen for the Reply by
	// intercepting via a forwarding sender wrapper. Or simpler: register a
	// custom OnConnected that wraps the sender to log replies.

	addrCh := make(chan string, 1)
	nc := node.New(node.Config{
		HubURL: nodeURL, Token: "secret", Hostname: "e2e-host",
		OnConnected: func(send node.Sender) {
			ph.ForwardHandler().Start(context.Background(), &replyCapture{Sender: send, addr: addrCh})
		},
	})
	nc.SetHandler(ph)
	go nc.Run(ctx)

	var addr string
	select {
	case addr = <-addrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no listen_addr captured")
	}

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("world"))
	require.NoError(t, err)
	buf := make([]byte, 5)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, "world", string(buf))

	ph.Shutdown()
}

// replyCapture wraps a node.Sender and snoops outgoing frames to surface the
// listen_addr from a ForwardListen Reply. (Listen replies are inbound, so this
// approach won't catch them — instead, intercept on the inbound path by
// wrapping the handler too. See test note.)
type replyCapture struct {
	node.Sender
	addr chan<- string
}

func (r *replyCapture) Send(m protocol.Message) error { return r.Sender.Send(m) }
```

**IMPORTANT:** The `replyCapture` sketch above won't work because Reply frames are *inbound*, not outbound. The implementer needs to wrap the *inbound* path instead. The cleanest mechanism:

Wrap the `Handler` interface. Wrap `ph` in a `replyTapHandler{Handler: ph, onReply: ...}` whose `Handle` method intercepts `*protocol.Reply` carrying a `Data["listen_addr"]`, sends it to a channel, then delegates to the wrapped handler so the ForwardHandler still gets it.

Replace the sketch with:

```go
type replyTapHandler struct {
	inner node.Handler
	addr  chan<- string
}

func (t *replyTapHandler) Handle(ctx context.Context, send node.Sender, msg protocol.Message) {
	if r, ok := msg.(*protocol.Reply); ok && r.OK && r.Data != nil {
		if v, ok := r.Data["listen_addr"].(string); ok {
			select {
			case t.addr <- v:
			default:
			}
		}
	}
	t.inner.Handle(ctx, send, msg)
}
```

And in the R test, set `nc.SetHandler(&replyTapHandler{inner: ph, addr: addrCh})` instead of `nc.SetHandler(ph)`.

The `echoLoop` helper stays as-is (it's already in the file). Imports: keep `"io"`, `"net"`, `"strconv"`, `"github.com/qiuxiang/tether/internal/forward"`, `"github.com/qiuxiang/tether/internal/protocol"`.

- [ ] **Step 2: Run e2e**

```
go test ./... -run E2EForward -race -v -timeout 30s
go test ./... -race -timeout 60s
```
Expected: pass.

- [ ] **Step 3: Commit**

```
git add e2e_test.go
git commit -m "e2e: rewrite forward tests as node self-loop"
```

---

## Task 11: Docs

**Goal:** Update README + design.md to reflect that forwards live in node config.

**Files:**
- Modify: `README.md`
- Modify: `docs/design.md`

- [ ] **Step 1: README**

Find the existing "Port forwarding" section in `README.md`. Replace its config example with:

```markdown
### Port forwarding

Configure `tether join` (in the node's `config.yaml`) to multiplex TCP forwards
over the hub:

```yaml
hub_url: "wss://tether.example.com/device"
token: "your-secret-token"
hostname_override: "coder"
forwards:
  - "L 9000:mac:5037"           # bind 127.0.0.1:9000 on this node → mac's localhost:5037
  - "L 0.0.0.0:9000:mac:5037"   # bind 0.0.0.0:9000 on this node → mac's localhost:5037
  - "R mac:8080:3000"           # mac binds 127.0.0.1:8080 → this node's 127.0.0.1:3000
  - "R mac:0.0.0.0:8080:3000"   # mac binds 0.0.0.0:8080 → this node's 127.0.0.1:3000
```

Syntax mirrors ssh. `L` = listener on the local node, forwarded to the named
peer device. `R` = listener on the peer device, forwarded back to the local
node. `bind` defaults to `127.0.0.1` and `host` defaults to `localhost`.
Only TCP. Rules are loaded at `tether join` startup; restart the node service
to change them. A peer going offline keeps `L` listeners up (the next accept
closes immediately) and re-establishes `R` listeners automatically when the
peer reconnects.
```

Delete any prior `tether mcp` / `client.yaml` references to forwards.

- [ ] **Step 2: design.md**

In `docs/design.md`:
- In the wire-protocol section (forward frames table), change any wording that says "client" sends `forward_listen`/`forward_dial` to "node" — both ends of a forward are nodes now.
- Note explicitly: "Forwards are configured in node `config.yaml` under `forwards:`. The mcp client is not involved in forwarding."

- [ ] **Step 3: Build**

```
go build ./...
make build 2>/dev/null || true
```

- [ ] **Step 4: Commit**

```
git add README.md docs/design.md
git commit -m "docs: forwards now live in node config"
```

---

## Self-Review Notes

- **Spec coverage:** The 7 high-level decisions from the brainstorm map as follows:
  1. Config location (`forwards` in node config) → Task 2
  2. L/R semantics relative to owning node → Tasks 3, 6 (rules carry through unchanged)
  3. Code migration (client→node) → Tasks 3, 9
  4. Hub simplification (drop client dispatch + cleanup) → Tasks 7, 8
  5. Device-event broadcast extended to nodes → Task 1
  6. mcp client back to pure MCP → Task 9
  7. Lifecycle (Init + Start + OnConnected) → Tasks 3, 5, 6

- **Reusing existing code:** `forward.Rule` / `forward.ParseAll` / `protocol.Forward*` frames / hub `ForwardTable` / node `ForwardHandler` passive half — all carry over untouched.

- **What's actually new:** `ForwardHandler` active half (≈150 LOC), `OnConnected` hook (~5 LOC), device-WS dispatch additions for `forward_listen`/`unlisten`/`dial-to-peer` (~40 LOC), broadcast extension (~5 LOC), CLI wiring (~5 LOC).

- **What's deleted:** `internal/client/forward.go` (~250 LOC), client `Forwards` config, `RPC.SetForwardHandler`, `dispatchForward` on `clientSession` and its tests.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-19-port-forwarding-on-node.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task with two-stage review between tasks.
2. **Inline Execution** — Execute in this session with checkpoints.

Which approach?
