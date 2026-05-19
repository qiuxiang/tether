# Port Forwarding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement ssh `-L`/`-R` style TCP port forwarding for tether, driven by `client.yaml`, multiplexed over existing WSS+CBOR channels.

**Architecture:** New `forward_*` frame types on the existing envelope. `tether mcp` parses `forwards` rules at startup; for `L` rules it owns the local TCP listener and tells the node to dial on each accept; for `R` rules it asks the node to listen and the node forwards each accept back to it. Hub tracks two new tables (`forward_id → client_ws` for listener registrations, `stream_id → (client_ws, node_ws)` for active TCP streams) and routes frames purely on those keys. New `device_online`/`device_offline` events let clients re-register `R` listeners on reconnect.

**Tech Stack:** Go, `github.com/coder/websocket`, `github.com/fxamacker/cbor/v2`, `net` (stdlib TCP).

**Spec reference:** `docs/superpowers/specs/2026-05-19-port-forwarding-design.md`

---

## File Structure

**New:**
- `internal/protocol/forward.go` — message structs for the 5 new frame types
- `internal/forward/rule.go` — `Rule` type + ssh-style string parser
- `internal/forward/rule_test.go`
- `internal/hub/forward.go` — `ForwardTable` (listeners + streams)
- `internal/hub/forward_test.go`
- `internal/node/forward.go` — node-side listener registry + dial + byte pumps
- `internal/node/forward_test.go`
- `internal/client/forward.go` — client-side listener mgr + dial-back + recovery
- `internal/client/forward_test.go`

**Modify:**
- `internal/protocol/messages.go` — `Event.Device` field; new `msgType()` cases
- `internal/protocol/codec.go` — `Encode`/`Decode` cases for the 5 new types
- `internal/client/config.go` — `Forwards []string` field; pre-parse on `Load`
- `internal/hub/client_ws.go` — dispatch forward frames from client
- `internal/hub/device_ws.go` — route forward frames from node; emit device events
- `internal/hub/registry.go` — subscribe hook for device online/offline broadcasting
- `internal/node/handler.go` — instantiate + delegate to `ForwardHandler`
- `internal/client/conn.go` — keep raw delivery so forward frames can reach mgr
- `internal/client/rpc.go` — deliver forward frames to forward manager
- `internal/cli/mcp.go` — construct forward manager, start it after `WaitReady`
- `e2e_test.go` — port-forwarding test cases
- `README.md`, `docs/design.md` — docs

---

## Task 1: Protocol — new frame types

**Files:**
- Create: `internal/protocol/forward.go`
- Create: `internal/protocol/forward_test.go`
- Modify: `internal/protocol/messages.go`
- Modify: `internal/protocol/codec.go`

- [ ] **Step 1: Write failing CBOR roundtrip tests**

Create `internal/protocol/forward_test.go`:

```go
package protocol

import (
	"reflect"
	"testing"
)

func TestForwardListenRoundtrip(t *testing.T) {
	in := &ForwardListen{MsgID: "m1", Target: "mac", ForwardID: "f1",
		ListenAddr: "127.0.0.1:8080", DestHost: "localhost", DestPort: 3000}
	raw, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(*ForwardListen)
	if !ok {
		t.Fatalf("decoded type %T", out)
	}
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("mismatch: %+v vs %+v", in, got)
	}
}

func TestForwardUnlistenRoundtrip(t *testing.T) {
	in := &ForwardUnlisten{MsgID: "m2", Target: "mac", ForwardID: "f1"}
	raw, _ := Encode(in)
	out, _ := Decode(raw)
	got, _ := out.(*ForwardUnlisten)
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("mismatch")
	}
}

func TestForwardDialRoundtrip(t *testing.T) {
	in := &ForwardDial{MsgID: "m3", Target: "mac", StreamID: "s1",
		ForwardID: "f1", DestHost: "localhost", DestPort: 5037}
	raw, _ := Encode(in)
	out, _ := Decode(raw)
	got, _ := out.(*ForwardDial)
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("mismatch")
	}
}

func TestForwardDataRoundtrip(t *testing.T) {
	in := &ForwardData{Target: "mac", StreamID: "s1", Data: []byte("hello")}
	raw, _ := Encode(in)
	out, _ := Decode(raw)
	got, _ := out.(*ForwardData)
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("mismatch")
	}
}

func TestForwardCloseRoundtrip(t *testing.T) {
	in := &ForwardClose{Target: "mac", StreamID: "s1", Half: "write"}
	raw, _ := Encode(in)
	out, _ := Decode(raw)
	got, _ := out.(*ForwardClose)
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("mismatch")
	}
}

func TestEventDeviceRoundtrip(t *testing.T) {
	in := &Event{Kind: "device_online", Device: "mac"}
	raw, _ := Encode(in)
	out, _ := Decode(raw)
	got, _ := out.(*Event)
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("mismatch: %+v vs %+v", in, got)
	}
}
```

- [ ] **Step 2: Run tests, confirm they fail to compile**

Run: `go test ./internal/protocol/ -run Forward -v`
Expected: build errors (`undefined: ForwardListen`, etc.).

- [ ] **Step 3: Create `internal/protocol/forward.go`**

```go
package protocol

// ForwardListen — client→node: open a TCP listener on `listen_addr`.
// Hub records (forward_id → client_ws) for routing dial-backs.
type ForwardListen struct {
	Type       string `cbor:"type"`
	MsgID      string `cbor:"msg_id"`
	Target     string `cbor:"target,omitempty"`
	ForwardID  string `cbor:"forward_id"`
	ListenAddr string `cbor:"listen_addr"`
	DestHost   string `cbor:"dest_host"`
	DestPort   int    `cbor:"dest_port"`
}

// ForwardUnlisten — client→node: close a previously-opened listener.
type ForwardUnlisten struct {
	Type      string `cbor:"type"`
	MsgID     string `cbor:"msg_id"`
	Target    string `cbor:"target,omitempty"`
	ForwardID string `cbor:"forward_id"`
}

// ForwardDial — bidirectional: ask the receiver to dial dest_host:dest_port
// and bind it to stream_id. ForwardID is set when node→client (so the client
// can look up the local dest); Target is set when client→node.
type ForwardDial struct {
	Type      string `cbor:"type"`
	MsgID     string `cbor:"msg_id"`
	Target    string `cbor:"target,omitempty"`
	StreamID  string `cbor:"stream_id"`
	ForwardID string `cbor:"forward_id,omitempty"`
	DestHost  string `cbor:"dest_host,omitempty"`
	DestPort  int    `cbor:"dest_port,omitempty"`
}

// ForwardData — bidirectional payload chunk for an open stream. No reply.
type ForwardData struct {
	Type     string `cbor:"type"`
	Target   string `cbor:"target,omitempty"`
	StreamID string `cbor:"stream_id"`
	Data     []byte `cbor:"data"`
}

// ForwardClose — bidirectional half/full close. No reply.
// Half ∈ {"read","write","both"} (default "both" when omitted).
type ForwardClose struct {
	Type     string `cbor:"type"`
	Target   string `cbor:"target,omitempty"`
	StreamID string `cbor:"stream_id"`
	Half     string `cbor:"half,omitempty"`
}

func (m *ForwardListen) msgType() string   { return "forward_listen" }
func (m *ForwardUnlisten) msgType() string { return "forward_unlisten" }
func (m *ForwardDial) msgType() string     { return "forward_dial" }
func (m *ForwardData) msgType() string     { return "forward_data" }
func (m *ForwardClose) msgType() string    { return "forward_close" }
```

- [ ] **Step 4: Add `Device` field to `Event` struct**

In `internal/protocol/messages.go`, replace the existing `Event` struct with:

```go
type Event struct {
	Type      string `cbor:"type"`
	Kind      string `cbor:"kind"` // "exit" | "device_online" | "device_offline"
	ProcessID string `cbor:"process_id,omitempty"`
	Code      int    `cbor:"code,omitempty"`
	Device    string `cbor:"device,omitempty"`
}
```

(Make `ProcessID` `omitempty` — was previously required.)

- [ ] **Step 5: Wire Encode/Decode**

In `internal/protocol/codec.go`, add cases to the `setType` switch:

```go
	case *ForwardListen:
		v.Type = m.msgType()
	case *ForwardUnlisten:
		v.Type = m.msgType()
	case *ForwardDial:
		v.Type = m.msgType()
	case *ForwardData:
		v.Type = m.msgType()
	case *ForwardClose:
		v.Type = m.msgType()
```

And to the `Decode` switch:

```go
	case "forward_listen":
		m = &ForwardListen{}
	case "forward_unlisten":
		m = &ForwardUnlisten{}
	case "forward_dial":
		m = &ForwardDial{}
	case "forward_data":
		m = &ForwardData{}
	case "forward_close":
		m = &ForwardClose{}
```

- [ ] **Step 6: Run tests**

Run: `go test ./internal/protocol/ -v`
Expected: all pass (including the new `Forward*` and `EventDevice` tests).

- [ ] **Step 7: Commit**

```bash
git add internal/protocol/
git commit -m "protocol: add port-forwarding frame types"
```

---

## Task 2: Forward rule parser

**Files:**
- Create: `internal/forward/rule.go`
- Create: `internal/forward/rule_test.go`

- [ ] **Step 1: Write failing table-driven test**

Create `internal/forward/rule_test.go`:

```go
package forward

import (
	"strings"
	"testing"
)

func TestParseRule(t *testing.T) {
	cases := []struct {
		in   string
		want Rule
	}{
		{"L 9000:mac:5037",
			Rule{Dir: DirLocal, Bind: "127.0.0.1", ListenPort: 9000, Device: "mac", DestHost: "localhost", DestPort: 5037}},
		{"L 0.0.0.0:9000:mac:5037",
			Rule{Dir: DirLocal, Bind: "0.0.0.0", ListenPort: 9000, Device: "mac", DestHost: "localhost", DestPort: 5037}},
		{"L 9000:mac:192.168.1.5:5037",
			Rule{Dir: DirLocal, Bind: "127.0.0.1", ListenPort: 9000, Device: "mac", DestHost: "192.168.1.5", DestPort: 5037}},
		{"L [::1]:9000:mac:db.local:5432",
			Rule{Dir: DirLocal, Bind: "::1", ListenPort: 9000, Device: "mac", DestHost: "db.local", DestPort: 5432}},
		{"R mac:8080:3000",
			Rule{Dir: DirRemote, Device: "mac", Bind: "127.0.0.1", ListenPort: 8080, DestHost: "localhost", DestPort: 3000}},
		{"R mac:0.0.0.0:8080:3000",
			Rule{Dir: DirRemote, Device: "mac", Bind: "0.0.0.0", ListenPort: 8080, DestHost: "localhost", DestPort: 3000}},
		{"R mac:8080:db.local:5432",
			Rule{Dir: DirRemote, Device: "mac", Bind: "127.0.0.1", ListenPort: 8080, DestHost: "db.local", DestPort: 5432}},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := Parse(tc.in)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			tc.want.Raw = tc.in
			if got != tc.want {
				t.Fatalf("got %+v want %+v", got, tc.want)
			}
		})
	}
}

func TestParseRuleErrors(t *testing.T) {
	bad := []string{
		"",
		"X 1:a:2",                // unknown direction
		"L only-two:fields",      // too few colons
		"L 0:mac:5037",           // port 0
		"L 65536:mac:5037",       // port out of range
		"L abc:mac:5037",         // non-numeric port
		"R mac:8080",             // missing dest port for R
		"L 9000mac5037",          // no colons
		"L 9000:mac:5037 extra",  // trailing junk
	}
	for _, s := range bad {
		t.Run(s, func(t *testing.T) {
			_, err := Parse(s)
			if err == nil {
				t.Fatalf("expected error for %q", s)
			}
			if !strings.Contains(err.Error(), "forward rule") {
				t.Fatalf("error should mention 'forward rule': %v", err)
			}
		})
	}
}

func TestParseAllDuplicateListen(t *testing.T) {
	_, err := ParseAll([]string{
		"L 9000:mac:5037",
		"L 9000:linux:22",
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("want duplicate-listen error, got %v", err)
	}
}
```

- [ ] **Step 2: Run, expect compile failure**

Run: `go test ./internal/forward/ -v`
Expected: build error (`undefined: Rule`).

- [ ] **Step 3: Implement parser**

Create `internal/forward/rule.go`:

```go
package forward

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Direction string

const (
	DirLocal  Direction = "L"
	DirRemote Direction = "R"
)

// Rule is one parsed `forwards:` entry.
//
// For DirLocal: listener is on the client (Bind:ListenPort); each accepted
// conn becomes a forward_dial to Device asking it to dial DestHost:DestPort.
// For DirRemote: listener is on Device (Bind:ListenPort); each accept becomes
// a forward_dial back to the client which then dials DestHost:DestPort
// locally.
type Rule struct {
	Raw        string
	Dir        Direction
	Device     string
	Bind       string
	ListenPort int
	DestHost   string
	DestPort   int
}

// ParseAll parses every rule string and rejects duplicate listen tuples.
func ParseAll(in []string) ([]Rule, error) {
	out := make([]Rule, 0, len(in))
	seen := map[string]string{}
	for _, s := range in {
		r, err := Parse(s)
		if err != nil {
			return nil, err
		}
		key := keyOf(r)
		if prev, ok := seen[key]; ok {
			return nil, fmt.Errorf("forward rule: duplicate listen tuple %q (also from %q)", s, prev)
		}
		seen[key] = s
		out = append(out, r)
	}
	return out, nil
}

func keyOf(r Rule) string {
	if r.Dir == DirLocal {
		return fmt.Sprintf("L:%s:%d", r.Bind, r.ListenPort)
	}
	return fmt.Sprintf("R:%s:%s:%d", r.Device, r.Bind, r.ListenPort)
}

func Parse(s string) (Rule, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Rule{}, fmt.Errorf("forward rule: empty")
	}
	parts := strings.SplitN(s, " ", 2)
	if len(parts) != 2 {
		return Rule{}, fmt.Errorf("forward rule: missing spec in %q", s)
	}
	dir := Direction(parts[0])
	spec := strings.TrimSpace(parts[1])
	if strings.ContainsAny(spec, " \t") {
		return Rule{}, fmt.Errorf("forward rule: unexpected whitespace in spec %q", s)
	}
	switch dir {
	case DirLocal:
		return parseLocal(s, spec)
	case DirRemote:
		return parseRemote(s, spec)
	default:
		return Rule{}, fmt.Errorf("forward rule: unknown direction %q (want L|R)", parts[0])
	}
}

// parseLocal: [bind:]port:device:[host:]port
func parseLocal(raw, spec string) (Rule, error) {
	fields, err := splitHostPort(spec)
	if err != nil {
		return Rule{}, fmt.Errorf("forward rule %q: %w", raw, err)
	}
	// Variants by field count:
	//   3: port:device:port              → bind="127.0.0.1", host="localhost"
	//   4: port:device:host:port  OR  bind:port:device:port
	//   5: bind:port:device:host:port
	r := Rule{Raw: raw, Dir: DirLocal, Bind: "127.0.0.1", DestHost: "localhost"}
	switch len(fields) {
	case 3:
		if err := setPort(&r.ListenPort, fields[0], raw); err != nil {
			return Rule{}, err
		}
		r.Device = fields[1]
		if err := setPort(&r.DestPort, fields[2], raw); err != nil {
			return Rule{}, err
		}
	case 4:
		// Ambiguous shape; disambiguate by checking if fields[0] parses as a port.
		if p, err := portOf(fields[0]); err == nil {
			r.ListenPort = p
			r.Device = fields[1]
			r.DestHost = fields[2]
			if err := setPort(&r.DestPort, fields[3], raw); err != nil {
				return Rule{}, err
			}
		} else {
			r.Bind = fields[0]
			if err := setPort(&r.ListenPort, fields[1], raw); err != nil {
				return Rule{}, err
			}
			r.Device = fields[2]
			if err := setPort(&r.DestPort, fields[3], raw); err != nil {
				return Rule{}, err
			}
		}
	case 5:
		r.Bind = fields[0]
		if err := setPort(&r.ListenPort, fields[1], raw); err != nil {
			return Rule{}, err
		}
		r.Device = fields[2]
		r.DestHost = fields[3]
		if err := setPort(&r.DestPort, fields[4], raw); err != nil {
			return Rule{}, err
		}
	default:
		return Rule{}, fmt.Errorf("forward rule %q: wrong number of fields (%d)", raw, len(fields))
	}
	if r.Device == "" {
		return Rule{}, fmt.Errorf("forward rule %q: empty device", raw)
	}
	return r, nil
}

// parseRemote: device:[bind:]port:[host:]port
func parseRemote(raw, spec string) (Rule, error) {
	fields, err := splitHostPort(spec)
	if err != nil {
		return Rule{}, fmt.Errorf("forward rule %q: %w", raw, err)
	}
	// Variants:
	//   3: device:port:port               → bind="127.0.0.1", host="localhost"
	//   4: device:port:host:port OR device:bind:port:port
	//   5: device:bind:port:host:port
	r := Rule{Raw: raw, Dir: DirRemote, Bind: "127.0.0.1", DestHost: "localhost"}
	switch len(fields) {
	case 3:
		r.Device = fields[0]
		if err := setPort(&r.ListenPort, fields[1], raw); err != nil {
			return Rule{}, err
		}
		if err := setPort(&r.DestPort, fields[2], raw); err != nil {
			return Rule{}, err
		}
	case 4:
		r.Device = fields[0]
		if p, err := portOf(fields[1]); err == nil {
			r.ListenPort = p
			r.DestHost = fields[2]
			if err := setPort(&r.DestPort, fields[3], raw); err != nil {
				return Rule{}, err
			}
		} else {
			r.Bind = fields[1]
			if err := setPort(&r.ListenPort, fields[2], raw); err != nil {
				return Rule{}, err
			}
			if err := setPort(&r.DestPort, fields[3], raw); err != nil {
				return Rule{}, err
			}
		}
	case 5:
		r.Device = fields[0]
		r.Bind = fields[1]
		if err := setPort(&r.ListenPort, fields[2], raw); err != nil {
			return Rule{}, err
		}
		r.DestHost = fields[3]
		if err := setPort(&r.DestPort, fields[4], raw); err != nil {
			return Rule{}, err
		}
	default:
		return Rule{}, fmt.Errorf("forward rule %q: wrong number of fields (%d)", raw, len(fields))
	}
	if r.Device == "" {
		return Rule{}, fmt.Errorf("forward rule %q: empty device", raw)
	}
	return r, nil
}

// splitHostPort splits a colon-delimited spec, respecting [ipv6]:port brackets.
func splitHostPort(s string) ([]string, error) {
	var out []string
	i := 0
	for i < len(s) {
		if s[i] == '[' {
			end := strings.IndexByte(s[i:], ']')
			if end < 0 {
				return nil, fmt.Errorf("unterminated [")
			}
			out = append(out, s[i+1:i+end])
			i += end + 1
			if i < len(s) && s[i] != ':' {
				return nil, fmt.Errorf("expected ':' after ']'")
			}
			if i < len(s) {
				i++
			}
			continue
		}
		j := strings.IndexByte(s[i:], ':')
		if j < 0 {
			out = append(out, s[i:])
			break
		}
		out = append(out, s[i:i+j])
		i += j + 1
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty spec")
	}
	for _, f := range out {
		if f == "" {
			return nil, fmt.Errorf("empty field")
		}
	}
	return out, nil
}

func portOf(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 65535 {
		return 0, fmt.Errorf("bad port %q", s)
	}
	return n, nil
}

func setPort(dst *int, s, raw string) error {
	p, err := portOf(s)
	if err != nil {
		return fmt.Errorf("forward rule %q: %w", raw, err)
	}
	*dst = p
	return nil
}

// ListenAddr formats `Bind:ListenPort`, bracketing IPv6 literals.
func (r Rule) ListenAddr() string {
	return net.JoinHostPort(r.Bind, strconv.Itoa(r.ListenPort))
}

// DestAddr formats `DestHost:DestPort`, bracketing IPv6 literals.
func (r Rule) DestAddr() string {
	return net.JoinHostPort(r.DestHost, strconv.Itoa(r.DestPort))
}
```

- [ ] **Step 2.5: Run tests**

Run: `go test ./internal/forward/ -v`
Expected: all pass.

- [ ] **Step 3: Commit**

```bash
git add internal/forward/
git commit -m "forward: ssh-style rule parser"
```

---

## Task 3: Client config — `forwards` field

**Files:**
- Modify: `internal/client/config.go`
- Modify: `internal/client/config_test.go`

- [ ] **Step 1: Write failing test**

Append to `internal/client/config_test.go`:

```go
func TestLoadForwards(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "client.yaml")
	body := "hub_url: ws://x\ntoken: t\nforwards:\n  - \"L 9000:mac:5037\"\n  - \"R mac:8080:3000\"\n"
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	c, err := Load(p)
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

func TestLoadForwardsInvalid(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "client.yaml")
	body := "hub_url: ws://x\ntoken: t\nforwards: [\"L bogus\"]\n"
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(p); err == nil {
		t.Fatal("expected parse error")
	}
}
```

Add imports at top: `"os"`, `"path/filepath"`, `"testing"`, `"github.com/qiuxiang/tether/internal/forward"`.

- [ ] **Step 2: Run, expect failure**

Run: `go test ./internal/client/ -run Forwards -v`
Expected: build error (`Config.Forwards undefined`).

- [ ] **Step 3: Update config**

In `internal/client/config.go`:

```go
package client

import (
	"errors"
	"os"

	"github.com/qiuxiang/tether/internal/forward"
	"gopkg.in/yaml.v3"
)

type Config struct {
	HubURL   string         `yaml:"hub_url"`
	Token    string         `yaml:"token"`
	Forwards []forward.Rule `yaml:"-"`
}

type rawConfig struct {
	HubURL   string   `yaml:"hub_url"`
	Token    string   `yaml:"token"`
	Forwards []string `yaml:"forwards"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw rawConfig
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
	return &Config{HubURL: raw.HubURL, Token: raw.Token, Forwards: rules}, nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/client/ -v`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add internal/client/config.go internal/client/config_test.go
git commit -m "client: parse forwards rules from client.yaml"
```

---

## Task 4: Hub — forward routing tables

**Files:**
- Create: `internal/hub/forward.go`
- Create: `internal/hub/forward_test.go`
- Modify: `internal/hub/server.go`

- [ ] **Step 1: Write failing test**

Create `internal/hub/forward_test.go`:

```go
package hub

import (
	"sync"
	"testing"
)

type fakePeer struct {
	mu   sync.Mutex
	sent [][]byte
}

func (p *fakePeer) SendRaw(b []byte) error {
	p.mu.Lock()
	p.sent = append(p.sent, append([]byte(nil), b...))
	p.mu.Unlock()
	return nil
}
func (p *fakePeer) Close() {}

func TestForwardTableListeners(t *testing.T) {
	ft := NewForwardTable()
	c := &fakePeer{}
	ft.AddListener("f1", c)
	got, ok := ft.LookupListener("f1")
	if !ok || got != c {
		t.Fatalf("lookup miss")
	}
	ft.RemoveListenersForClient(c)
	if _, ok := ft.LookupListener("f1"); ok {
		t.Fatalf("listener not removed")
	}
}

func TestForwardTableStreams(t *testing.T) {
	ft := NewForwardTable()
	c, n := &fakePeer{}, &fakePeer{}
	ft.OpenStream("s1", c, n)
	gc, gn, ok := ft.LookupStream("s1")
	if !ok || gc != c || gn != n {
		t.Fatalf("lookup mismatch")
	}
	ft.CloseStream("s1")
	if _, _, ok := ft.LookupStream("s1"); ok {
		t.Fatalf("stream not closed")
	}
}

func TestForwardTableEvictByPeer(t *testing.T) {
	ft := NewForwardTable()
	c, n1, n2 := &fakePeer{}, &fakePeer{}, &fakePeer{}
	ft.OpenStream("s1", c, n1)
	ft.OpenStream("s2", c, n2)
	evicted := ft.EvictStreamsForNode(n1)
	if len(evicted) != 1 || evicted[0] != "s1" {
		t.Fatalf("got %v", evicted)
	}
	if _, _, ok := ft.LookupStream("s1"); ok {
		t.Fatalf("s1 should be gone")
	}
	if _, _, ok := ft.LookupStream("s2"); !ok {
		t.Fatalf("s2 should remain")
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `go test ./internal/hub/ -run Forward -v`
Expected: build error.

- [ ] **Step 3: Implement table**

Create `internal/hub/forward.go`:

```go
package hub

import "sync"

// ForwardTable tracks port-forwarding routing state.
//   listeners: forward_id → client peer that registered the listener
//   streams  : stream_id  → (client, node) for an active TCP forward
type ForwardTable struct {
	mu        sync.Mutex
	listeners map[string]PeerConn
	streams   map[string]streamRoute
}

type streamRoute struct {
	Client PeerConn
	Node   PeerConn
}

func NewForwardTable() *ForwardTable {
	return &ForwardTable{
		listeners: make(map[string]PeerConn),
		streams:   make(map[string]streamRoute),
	}
}

func (t *ForwardTable) AddListener(forwardID string, client PeerConn) {
	t.mu.Lock()
	t.listeners[forwardID] = client
	t.mu.Unlock()
}

func (t *ForwardTable) RemoveListener(forwardID string) {
	t.mu.Lock()
	delete(t.listeners, forwardID)
	t.mu.Unlock()
}

func (t *ForwardTable) LookupListener(forwardID string) (PeerConn, bool) {
	t.mu.Lock()
	p, ok := t.listeners[forwardID]
	t.mu.Unlock()
	return p, ok
}

// RemoveListenersForClient drops every listener registered by client. Returns
// the removed forward_ids so the caller can notify nodes.
func (t *ForwardTable) RemoveListenersForClient(client PeerConn) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []string
	for fid, p := range t.listeners {
		if p == client {
			out = append(out, fid)
			delete(t.listeners, fid)
		}
	}
	return out
}

func (t *ForwardTable) OpenStream(streamID string, client, node PeerConn) {
	t.mu.Lock()
	t.streams[streamID] = streamRoute{Client: client, Node: node}
	t.mu.Unlock()
}

func (t *ForwardTable) LookupStream(streamID string) (PeerConn, PeerConn, bool) {
	t.mu.Lock()
	r, ok := t.streams[streamID]
	t.mu.Unlock()
	if !ok {
		return nil, nil, false
	}
	return r.Client, r.Node, true
}

func (t *ForwardTable) CloseStream(streamID string) {
	t.mu.Lock()
	delete(t.streams, streamID)
	t.mu.Unlock()
}

// EvictStreamsForNode removes every stream whose node side is `n`.
func (t *ForwardTable) EvictStreamsForNode(n PeerConn) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []string
	for sid, r := range t.streams {
		if r.Node == n {
			out = append(out, sid)
			delete(t.streams, sid)
		}
	}
	return out
}

// EvictStreamsForClient removes every stream whose client side is `c`.
func (t *ForwardTable) EvictStreamsForClient(c PeerConn) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []string
	for sid, r := range t.streams {
		if r.Client == c {
			out = append(out, sid)
			delete(t.streams, sid)
		}
	}
	return out
}
```

- [ ] **Step 4: Hook into `Server`**

In `internal/hub/server.go`, add a field and accessor:

```go
type Server struct {
	opts     Options
	registry *Registry
	clients  *ClientRegistry
	router   *Router
	relay    *RelayCoordinator
	forwards *ForwardTable
}

func NewServer(opts Options) *Server {
	s := &Server{
		opts:     opts,
		registry: NewRegistry(),
		clients:  NewClientRegistry(),
		router:   NewRouter(),
		forwards: NewForwardTable(),
	}
	s.relay = NewRelayCoordinator(s)
	return s
}

// ...
func (s *Server) Forwards() *ForwardTable { return s.forwards }
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/hub/ -v`
Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add internal/hub/forward.go internal/hub/forward_test.go internal/hub/server.go
git commit -m "hub: forward routing tables"
```

---

## Task 5: Hub — dispatch forward frames from client

**Files:**
- Modify: `internal/hub/client_ws.go`
- Create: `internal/hub/forward_dispatch_test.go`

- [ ] **Step 1: Write failing test**

Create `internal/hub/forward_dispatch_test.go`:

```go
package hub

import (
	"testing"

	"github.com/qiuxiang/tether/internal/protocol"
)

func TestClientForwardListenRegisters(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	node := &fakePeer{}
	s.Registry().Register(&Device{Hostname: "mac", Conn: node})
	c := &fakePeer{}
	cs := &clientSession{id: "c1", server: s}
	_ = cs // suppress unused warning if not used directly

	msg := &protocol.ForwardListen{MsgID: "m1", Target: "mac", ForwardID: "f1",
		ListenAddr: "127.0.0.1:0", DestHost: "x", DestPort: 1}
	raw, _ := protocol.Encode(msg)

	// dispatch wants a *clientSession; build a real one wired to fake peer c.
	cs2 := &clientSession{id: "c1", server: s, pending: map[string]struct{}{}, conn: nil}
	// Substitute SendRaw via a wrapping fake won't work; instead use dispatchForward directly.
	cs2.dispatchForward(raw, msg, c)

	if got, ok := s.Forwards().LookupListener("f1"); !ok || got != c {
		t.Fatalf("listener not registered")
	}
	if len(node.sent) != 1 {
		t.Fatalf("node should have received the frame")
	}
}

func TestClientForwardDialRegistersStream(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	node := &fakePeer{}
	s.Registry().Register(&Device{Hostname: "mac", Conn: node})
	c := &fakePeer{}
	cs := &clientSession{id: "c1", server: s, pending: map[string]struct{}{}}

	msg := &protocol.ForwardDial{MsgID: "m2", Target: "mac", StreamID: "s1",
		DestHost: "h", DestPort: 22}
	raw, _ := protocol.Encode(msg)
	cs.dispatchForward(raw, msg, c)

	gc, gn, ok := s.Forwards().LookupStream("s1")
	if !ok || gc != c || gn != node {
		t.Fatalf("stream not opened correctly: %v %v %v", gc, gn, ok)
	}
}

func TestClientForwardDataRouted(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	c, n := &fakePeer{}, &fakePeer{}
	s.Forwards().OpenStream("s1", c, n)
	cs := &clientSession{id: "c1", server: s, pending: map[string]struct{}{}}

	msg := &protocol.ForwardData{StreamID: "s1", Data: []byte("hi")}
	raw, _ := protocol.Encode(msg)
	cs.dispatchForward(raw, msg, c)
	if len(n.sent) != 1 {
		t.Fatalf("node should have received forward_data")
	}
}

func TestClientForwardCloseClearsStream(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	c, n := &fakePeer{}, &fakePeer{}
	s.Forwards().OpenStream("s1", c, n)
	cs := &clientSession{id: "c1", server: s, pending: map[string]struct{}{}}

	msg := &protocol.ForwardClose{StreamID: "s1"}
	raw, _ := protocol.Encode(msg)
	cs.dispatchForward(raw, msg, c)
	if _, _, ok := s.Forwards().LookupStream("s1"); ok {
		t.Fatalf("stream should be closed (Half default 'both')")
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `go test ./internal/hub/ -run Forward -v`
Expected: compile error or undefined method `dispatchForward`.

- [ ] **Step 3: Add dispatch helper**

In `internal/hub/client_ws.go`, extend the `dispatch` switch and add `dispatchForward`:

```go
	case *protocol.ForwardListen, *protocol.ForwardUnlisten, *protocol.ForwardDial,
		*protocol.ForwardData, *protocol.ForwardClose:
		cs.dispatchForward(raw, msg, cs)
```

(`cs` itself implements `PeerConn` via `SendRaw`/`Close`.) Then add:

```go
// dispatchForward routes forward_* frames originating from this client.
// `client` is the PeerConn associated with this session (cs in production,
// a fake in tests).
func (cs *clientSession) dispatchForward(raw []byte, msg protocol.Message, client PeerConn) {
	switch m := msg.(type) {
	case *protocol.ForwardListen:
		d, ok := cs.server.registry.Get(m.Target)
		if !ok || d.Conn == nil {
			cs.sendErrorReply(m.MsgID, fmt.Errorf("device_offline: %s", m.Target))
			return
		}
		cs.server.forwards.AddListener(m.ForwardID, client)
		cs.server.router.Register(m.MsgID, client, false)
		if err := d.Conn.SendRaw(raw); err != nil {
			cs.server.forwards.RemoveListener(m.ForwardID)
			cs.server.router.Unregister(m.MsgID)
			cs.sendErrorReply(m.MsgID, err)
		}
	case *protocol.ForwardUnlisten:
		cs.server.forwards.RemoveListener(m.ForwardID)
		d, ok := cs.server.registry.Get(m.Target)
		if ok && d.Conn != nil {
			cs.server.router.Register(m.MsgID, client, false)
			_ = d.Conn.SendRaw(raw)
		}
	case *protocol.ForwardDial:
		d, ok := cs.server.registry.Get(m.Target)
		if !ok || d.Conn == nil {
			cs.sendErrorReply(m.MsgID, fmt.Errorf("device_offline: %s", m.Target))
			return
		}
		cs.server.forwards.OpenStream(m.StreamID, client, d.Conn)
		cs.server.router.Register(m.MsgID, client, false)
		if err := d.Conn.SendRaw(raw); err != nil {
			cs.server.forwards.CloseStream(m.StreamID)
			cs.server.router.Unregister(m.MsgID)
			cs.sendErrorReply(m.MsgID, err)
		}
	case *protocol.ForwardData:
		_, node, ok := cs.server.forwards.LookupStream(m.StreamID)
		if !ok {
			return
		}
		_ = node.SendRaw(raw)
	case *protocol.ForwardClose:
		_, node, ok := cs.server.forwards.LookupStream(m.StreamID)
		if !ok {
			return
		}
		_ = node.SendRaw(raw)
		if m.Half == "" || m.Half == "both" {
			cs.server.forwards.CloseStream(m.StreamID)
		}
	}
}
```

Also extend the client-disconnect cleanup in `handleClient`'s deferred cleanup:

```go
	defer func() {
		log.Printf("client disconnected: id=%s", id)
		s.clients.Unregister(id)
		sess.mu.Lock()
		for msgID := range sess.pending {
			s.router.Unregister(msgID)
		}
		sess.mu.Unlock()
		// Forward cleanup: drop listeners + evict streams; notify nodes.
		for _, fid := range s.forwards.RemoveListenersForClient(sess) {
			// Best-effort unlisten broadcast: we don't track which node owned
			// each forward_id; iterate all registered nodes and send. They
			// ignore unknown forward_ids.
			unl := &protocol.ForwardUnlisten{ForwardID: fid}
			raw, _ := protocol.Encode(unl)
			for _, d := range s.registry.List() {
				if d.Conn != nil {
					_ = d.Conn.SendRaw(raw)
				}
			}
		}
		for _, sid := range s.forwards.EvictStreamsForClient(sess) {
			cl := &protocol.ForwardClose{StreamID: sid, Half: "both"}
			raw, _ := protocol.Encode(cl)
			for _, d := range s.registry.List() {
				if d.Conn != nil {
					_ = d.Conn.SendRaw(raw)
				}
			}
		}
		c.Close(websocket.StatusNormalClosure, "")
	}()
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/hub/ -v`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add internal/hub/
git commit -m "hub: dispatch forward frames from client side"
```

---

## Task 6: Hub — route node→client forward frames + device events

**Files:**
- Modify: `internal/hub/device_ws.go`
- Modify: `internal/hub/registry.go`
- Create: `internal/hub/device_events_test.go`

- [ ] **Step 1: Inspect registry**

Read `internal/hub/registry.go` to know the current `Register`/`Unregister` signature; the change adds two callbacks: `OnRegister` and `OnUnregister` (slice of funcs taking the hostname). The server wires these to broadcast `device_online`/`device_offline` events to all clients.

- [ ] **Step 2: Write failing test**

Create `internal/hub/device_events_test.go`:

```go
package hub

import (
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
)

func TestDeviceOnlineBroadcast(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	c := &fakePeer{}
	s.Clients().Register(&Client{ID: "c1", ConnectedAt: time.Now(), Conn: c})

	s.broadcastDeviceEvent("device_online", "mac")

	if len(c.sent) != 1 {
		t.Fatalf("client should have received 1 event, got %d", len(c.sent))
	}
	msg, err := protocol.Decode(c.sent[0])
	if err != nil {
		t.Fatal(err)
	}
	ev, ok := msg.(*protocol.Event)
	if !ok || ev.Kind != "device_online" || ev.Device != "mac" {
		t.Fatalf("wrong event: %+v", msg)
	}
}
```

- [ ] **Step 3: Run, expect failure**

Run: `go test ./internal/hub/ -run DeviceOnline -v`
Expected: `s.broadcastDeviceEvent undefined`.

- [ ] **Step 4: Implement broadcaster + event routing**

Add to `internal/hub/server.go`:

```go
import "log"
// (add to existing imports if missing)

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
}
```

(Import `"github.com/qiuxiang/tether/internal/protocol"` in server.go if not already imported. If `ClientRegistry.List()` doesn't exist, add a `List() []*Client` returning a snapshot — mirror what `Registry.List()` does.)

In `internal/hub/device_ws.go`'s `handleDevice`, after the successful `Register`:

```go
	s.broadcastDeviceEvent("device_online", sess.device.Hostname)
	defer func() {
		log.Printf("device disconnected: hostname=%s", sess.device.Hostname)
		s.registry.Unregister(sess.device.Hostname)
		s.broadcastDeviceEvent("device_offline", sess.device.Hostname)
		// Forward cleanup: evict streams involving this node; notify clients.
		for _, sid := range s.forwards.EvictStreamsForNode(sess) {
			cl := &protocol.ForwardClose{StreamID: sid, Half: "both"}
			raw, _ := protocol.Encode(cl)
			for _, c := range s.clients.List() {
				if c.Conn != nil {
					_ = c.Conn.SendRaw(raw)
				}
			}
		}
		c.Close(websocket.StatusNormalClosure, "")
	}()
```

Replace the existing `defer func()` rather than adding a second one.

- [ ] **Step 5: Route node→client forward frames in `deviceSession.run`**

Extend the `run` loop in `internal/hub/device_ws.go`:

```go
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

		// Forward frames from node use forward_id (dial) or stream_id
		// (data/close) for routing — not msg_id.
		switch v := msg.(type) {
		case *protocol.ForwardDial:
			client, ok := s.server.forwards.LookupListener(v.ForwardID)
			if !ok {
				continue
			}
			s.server.forwards.OpenStream(v.StreamID, client, s)
			s.server.router.Register(v.MsgID, client, false)
			_ = client.SendRaw(raw)
			continue
		case *protocol.ForwardData:
			client, _, ok := s.server.forwards.LookupStream(v.StreamID)
			if !ok {
				continue
			}
			_ = client.SendRaw(raw)
			continue
		case *protocol.ForwardClose:
			client, _, ok := s.server.forwards.LookupStream(v.StreamID)
			if !ok {
				continue
			}
			_ = client.SendRaw(raw)
			if v.Half == "" || v.Half == "both" {
				s.server.forwards.CloseStream(v.StreamID)
			}
			continue
		}

		id := msgID(msg)
		if id != "" {
			s.router.ForwardToClient(id, raw)
			switch v := msg.(type) {
			case *protocol.ExecExit:
				s.router.Unregister(id)
			case *protocol.FileChunk:
				if v.EOF {
					s.router.Unregister(id)
				}
			case *protocol.FileAbort:
				s.router.Unregister(id)
			}
		}
	}
```

`deviceSession` needs access to `*Server`; add an `server *Server` field and set it in `handshake`:

```go
type deviceSession struct {
	device *Device
	conn   *websocket.Conn
	router *Router
	server *Server
}
// ...
sess := &deviceSession{device: d, conn: c, router: s.router, server: s}
```

- [ ] **Step 6: Run tests**

Run: `go test ./internal/hub/ -v`
Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add internal/hub/
git commit -m "hub: route node→client forward frames; broadcast device events"
```

---

## Task 7: Node — forward handler

**Files:**
- Create: `internal/node/forward.go`
- Create: `internal/node/forward_test.go`
- Modify: `internal/node/handler.go`

- [ ] **Step 1: Write failing E2E-ish unit test**

Create `internal/node/forward_test.go`:

```go
package node

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
)

type captureSender struct {
	mu   sync.Mutex
	msgs []protocol.Message
}

func (s *captureSender) Send(msg protocol.Message) error {
	s.mu.Lock()
	s.msgs = append(s.msgs, msg)
	s.mu.Unlock()
	return nil
}

func (s *captureSender) take() []protocol.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.msgs
	s.msgs = nil
	return out
}

func TestForwardDialEcho(t *testing.T) {
	// echo server
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

	send := &captureSender{}
	h := NewForwardHandler()
	h.Dial(send, &protocol.ForwardDial{MsgID: "m1", StreamID: "s1",
		DestHost: host, DestPort: port})

	// Expect a Reply{ok:true}
	if !waitFor(t, func() bool {
		for _, m := range send.msgs {
			if r, ok := m.(*protocol.Reply); ok && r.OK {
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("did not get ok reply, got %+v", send.msgs)
	}

	// Send data, expect echo
	h.Data(send, &protocol.ForwardData{StreamID: "s1", Data: []byte("ping")})
	if !waitFor(t, func() bool {
		for _, m := range send.msgs {
			if d, ok := m.(*protocol.ForwardData); ok && string(d.Data) == "ping" {
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("did not echo, got %+v", send.msgs)
	}

	// Close
	h.Close(send, &protocol.ForwardClose{StreamID: "s1", Half: "both"})
	h.Shutdown()
}

func TestForwardListenAcceptDialsBack(t *testing.T) {
	send := &captureSender{}
	h := NewForwardHandler()
	h.Listen(send, &protocol.ForwardListen{MsgID: "m1", ForwardID: "f1",
		ListenAddr: "127.0.0.1:0", DestHost: "ignored", DestPort: 0})

	// Find ok reply with listen_addr
	var addr string
	if !waitFor(t, func() bool {
		for _, m := range send.msgs {
			r, ok := m.(*protocol.Reply)
			if !ok || !r.OK {
				continue
			}
			if v, ok := r.Data["listen_addr"].(string); ok {
				addr = v
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("no listen reply: %+v", send.msgs)
	}

	// Dial it
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Expect ForwardDial frame sent to send
	if !waitFor(t, func() bool {
		for _, m := range send.msgs {
			if d, ok := m.(*protocol.ForwardDial); ok && d.ForwardID == "f1" {
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("no dial frame: %+v", send.msgs)
	}

	h.Unlisten(send, &protocol.ForwardUnlisten{MsgID: "u1", ForwardID: "f1"})
	h.Shutdown()
}

func waitFor(t *testing.T, cond func() bool, d time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func splitHostPortTest(t *testing.T, addr string) (string, int) {
	t.Helper()
	h, ps, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	p := 0
	for _, c := range ps {
		p = p*10 + int(c-'0')
	}
	return h, p
}

// Keep io import in use even if compiler whines.
var _ = io.EOF
```

- [ ] **Step 2: Run, expect compile failure**

Run: `go test ./internal/node/ -run Forward -v`
Expected: `undefined: NewForwardHandler`.

- [ ] **Step 3: Implement `ForwardHandler`**

Create `internal/node/forward.go`:

```go
package node

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/qiuxiang/tether/internal/protocol"
)

// ForwardHandler owns node-side state for port forwarding:
//   listeners: forward_id → net.Listener     (created by ForwardListen)
//   streams  : stream_id  → forwardStream    (created by Dial or by Accept)
type ForwardHandler struct {
	mu        sync.Mutex
	listeners map[string]net.Listener
	streams   map[string]*forwardStream
}

type forwardStream struct {
	conn      net.Conn
	closeOnce sync.Once
}

func NewForwardHandler() *ForwardHandler {
	return &ForwardHandler{
		listeners: map[string]net.Listener{},
		streams:   map[string]*forwardStream{},
	}
}

func newStreamID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Dial handles ForwardDial from a client (L direction).
func (h *ForwardHandler) Dial(send Sender, m *protocol.ForwardDial) {
	addr := net.JoinHostPort(m.DestHost, strconv.Itoa(m.DestPort))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "dial: " + err.Error()})
		return
	}
	h.attachStream(m.StreamID, c, send)
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true})
}

// Listen handles ForwardListen from a client (R direction).
func (h *ForwardHandler) Listen(send Sender, m *protocol.ForwardListen) {
	ln, err := net.Listen("tcp", m.ListenAddr)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "bind: " + err.Error()})
		return
	}
	h.mu.Lock()
	if _, dup := h.listeners[m.ForwardID]; dup {
		h.mu.Unlock()
		ln.Close()
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "duplicate forward_id"})
		return
	}
	h.listeners[m.ForwardID] = ln
	h.mu.Unlock()

	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true,
		Data: map[string]any{"listen_addr": ln.Addr().String()}})

	go h.acceptLoop(send, m.ForwardID, ln)
}

func (h *ForwardHandler) acceptLoop(send Sender, fid string, ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		sid := newStreamID()
		h.attachStream(sid, c, send)
		// Ask client to dial-back.
		send.Send(&protocol.ForwardDial{MsgID: sid, StreamID: sid, ForwardID: fid})
	}
}

// Unlisten handles ForwardUnlisten — closes the listener.
func (h *ForwardHandler) Unlisten(send Sender, m *protocol.ForwardUnlisten) {
	h.mu.Lock()
	ln, ok := h.listeners[m.ForwardID]
	delete(h.listeners, m.ForwardID)
	h.mu.Unlock()
	if ok && ln != nil {
		ln.Close()
	}
	if m.MsgID != "" {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true})
	}
}

// Data routes payload bytes to the local conn.
func (h *ForwardHandler) Data(_ Sender, m *protocol.ForwardData) {
	h.mu.Lock()
	st := h.streams[m.StreamID]
	h.mu.Unlock()
	if st == nil {
		return
	}
	if _, err := st.conn.Write(m.Data); err != nil {
		h.closeStream(m.StreamID)
	}
}

// Close handles a remote half/full close.
func (h *ForwardHandler) Close(_ Sender, m *protocol.ForwardClose) {
	h.mu.Lock()
	st := h.streams[m.StreamID]
	h.mu.Unlock()
	if st == nil {
		return
	}
	switch m.Half {
	case "write":
		if tcp, ok := st.conn.(*net.TCPConn); ok {
			tcp.CloseWrite()
		}
	case "read":
		if tcp, ok := st.conn.(*net.TCPConn); ok {
			tcp.CloseRead()
		}
	default:
		h.closeStream(m.StreamID)
	}
}

// attachStream registers the local conn and spawns the read pump that emits
// ForwardData frames back to the sender.
func (h *ForwardHandler) attachStream(sid string, c net.Conn, send Sender) {
	st := &forwardStream{conn: c}
	h.mu.Lock()
	h.streams[sid] = st
	h.mu.Unlock()
	go h.readPump(sid, st, send)
}

func (h *ForwardHandler) readPump(sid string, st *forwardStream, send Sender) {
	buf := make([]byte, 32*1024)
	for {
		n, err := st.conn.Read(buf)
		if n > 0 {
			send.Send(&protocol.ForwardData{StreamID: sid, Data: append([]byte(nil), buf[:n]...)})
		}
		if err != nil {
			if err == io.EOF {
				send.Send(&protocol.ForwardClose{StreamID: sid, Half: "write"})
			} else {
				send.Send(&protocol.ForwardClose{StreamID: sid, Half: "both"})
			}
			h.closeStream(sid)
			return
		}
	}
}

func (h *ForwardHandler) closeStream(sid string) {
	h.mu.Lock()
	st := h.streams[sid]
	delete(h.streams, sid)
	h.mu.Unlock()
	if st == nil {
		return
	}
	st.closeOnce.Do(func() {
		_ = st.conn.Close()
	})
}

// Shutdown closes all listeners and streams. Idempotent.
func (h *ForwardHandler) Shutdown() {
	h.mu.Lock()
	lns := h.listeners
	streams := h.streams
	h.listeners = map[string]net.Listener{}
	h.streams = map[string]*forwardStream{}
	h.mu.Unlock()
	for _, ln := range lns {
		ln.Close()
	}
	for _, st := range streams {
		st.closeOnce.Do(func() { st.conn.Close() })
	}
}

// Suppress unused-import warnings if package layout shifts.
var _ = log.Printf
var _ = fmt.Sprintf
```

- [ ] **Step 4: Wire into `ProcessHandler.Handle`**

In `internal/node/handler.go`:

```go
type ProcessHandler struct {
	registry       *ProcessRegistry
	logDir         string
	mu             sync.Mutex
	execMu         sync.Mutex
	execCancel     map[string]context.CancelFunc
	fileHandler    *FileHandler
	forwardHandler *ForwardHandler
}

func NewProcessHandler(logDir string, cap int) *ProcessHandler {
	return &ProcessHandler{
		registry:       NewProcessRegistry(cap),
		logDir:         logDir,
		execCancel:     make(map[string]context.CancelFunc),
		fileHandler:    NewFileHandler(),
		forwardHandler: NewForwardHandler(),
	}
}
```

Extend the `Handle` switch:

```go
	case *protocol.ForwardListen:
		h.forwardHandler.Listen(send, m)
	case *protocol.ForwardUnlisten:
		h.forwardHandler.Unlisten(send, m)
	case *protocol.ForwardDial:
		h.forwardHandler.Dial(send, m)
	case *protocol.ForwardData:
		h.forwardHandler.Data(send, m)
	case *protocol.ForwardClose:
		h.forwardHandler.Close(send, m)
```

Add to `Shutdown`:

```go
	if h.forwardHandler != nil {
		h.forwardHandler.Shutdown()
	}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/node/ -v`
Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add internal/node/
git commit -m "node: forward handler — dial, listen, accept, byte pumps"
```

---

## Task 8: Client — forward manager

**Files:**
- Create: `internal/client/forward.go`
- Create: `internal/client/forward_test.go`
- Modify: `internal/client/rpc.go`
- Modify: `internal/cli/mcp.go`

- [ ] **Step 1: Write failing test**

Create `internal/client/forward_test.go`:

```go
package client

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/forward"
	"github.com/qiuxiang/tether/internal/hub"
	"github.com/qiuxiang/tether/internal/node"
)

// E2E-ish: full client + hub + node in-process, do an L-forward echo.
func TestForwardL_EchoEndToEnd(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "x"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "x", Hostname: "node1"})
	nc.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
	go nc.Run(ctx)
	waitFor(t, func() bool { _, ok := s.Registry().Get("node1"); return ok }, 2*time.Second)

	// Stand up an echo server on the node side (just bind a local listener
	// here in-process — node and client both run in this process for tests).
	echoLn, port := startEchoServer(t)
	defer echoLn.Close()

	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	cfg := Config{
		HubURL: cliURL, Token: "x",
		Forwards: []forward.Rule{{
			Raw: "L 0:node1:127.0.0.1:" + portStr(port),
			Dir: forward.DirLocal, Bind: "127.0.0.1", ListenPort: 0,
			Device: "node1", DestHost: "127.0.0.1", DestPort: port,
		}},
	}
	conn := NewConn(cfg)
	go conn.Run(ctx)
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := conn.WaitReady(cctx); err != nil {
		t.Fatal(err)
	}
	ccancel()

	fm := NewForwardManager(conn, cfg.Forwards)
	fm.Start(ctx)
	defer fm.Stop()

	localAddr := fm.LocalAddr(cfg.Forwards[0])
	if localAddr == "" {
		t.Fatal("no local addr bound")
	}
	echoExpect(t, localAddr, "ping", "ping")
}
```

(Add `startEchoServer`, `echoExpect`, `waitFor`, `portStr` helpers in the same file. Pattern after `internal/node/forward_test.go`.)

- [ ] **Step 2: Run, expect failure**

Run: `go test ./internal/client/ -run Forward -v`
Expected: build error.

- [ ] **Step 3: Implement forward manager**

Create `internal/client/forward.go`:

```go
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/qiuxiang/tether/internal/forward"
	"github.com/qiuxiang/tether/internal/protocol"
)

type Sender interface {
	Send(msg protocol.Message) error
}

type ForwardManager struct {
	sender Sender
	rules  []forward.Rule

	mu        sync.Mutex
	listeners map[string]net.Listener // keyed by forward_id (L only)
	streams   map[string]net.Conn     // stream_id → local conn
	byForward map[string]forward.Rule // forward_id → rule (for R dial-backs)
	addrs     map[string]string       // local addr per L rule (for tests)
}

func NewForwardManager(s Sender, rules []forward.Rule) *ForwardManager {
	return &ForwardManager{
		sender:    s,
		rules:     rules,
		listeners: map[string]net.Listener{},
		streams:   map[string]net.Conn{},
		byForward: map[string]forward.Rule{},
		addrs:     map[string]string{},
	}
}

func newForwardID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func newStreamID() string { return newForwardID() }

// Start binds L listeners and asks the hub to set up R listeners on the
// relevant nodes. Safe to call again after device_online events arrive
// (re-issuing forward_listen).
func (m *ForwardManager) Start(ctx context.Context) {
	for i := range m.rules {
		r := &m.rules[i]
		switch r.Dir {
		case forward.DirLocal:
			m.startLocal(ctx, r)
		case forward.DirRemote:
			m.startRemote(ctx, r)
		}
	}
}

func (m *ForwardManager) startLocal(ctx context.Context, r *forward.Rule) {
	fid := newForwardID()
	m.mu.Lock()
	m.byForward[fid] = *r
	m.mu.Unlock()

	ln, err := net.Listen("tcp", r.ListenAddr())
	if err != nil {
		log.Printf("forward L %s: bind failed: %v", r.Raw, err)
		return
	}
	m.mu.Lock()
	m.listeners[fid] = ln
	m.addrs[r.Raw] = ln.Addr().String()
	m.mu.Unlock()
	log.Printf("forward L %s: listening on %s", r.Raw, ln.Addr())

	go m.acceptLoop(ctx, fid, ln, *r)
}

func (m *ForwardManager) acceptLoop(ctx context.Context, fid string, ln net.Listener, r forward.Rule) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		sid := newStreamID()
		m.mu.Lock()
		m.streams[sid] = c
		m.mu.Unlock()
		// Ask node to dial.
		if err := m.sender.Send(&protocol.ForwardDial{
			MsgID: sid, Target: r.Device, StreamID: sid,
			DestHost: r.DestHost, DestPort: r.DestPort,
		}); err != nil {
			m.closeStream(sid)
			continue
		}
		go m.readPump(sid, c, r.Device)
	}
}

func (m *ForwardManager) startRemote(_ context.Context, r *forward.Rule) {
	fid := newForwardID()
	m.mu.Lock()
	m.byForward[fid] = *r
	m.mu.Unlock()
	_ = m.sender.Send(&protocol.ForwardListen{
		MsgID: fid, Target: r.Device, ForwardID: fid,
		ListenAddr: r.ListenAddr(),
		DestHost:   r.DestHost, DestPort: r.DestPort,
	})
}

func (m *ForwardManager) LocalAddr(r forward.Rule) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.addrs[r.Raw]
}

// Deliver dispatches a forward frame received from the hub.
func (m *ForwardManager) Deliver(msg protocol.Message) {
	switch v := msg.(type) {
	case *protocol.ForwardDial:
		// R direction: node accepted a conn, asks us to dial locally.
		m.mu.Lock()
		r, ok := m.byForward[v.ForwardID]
		m.mu.Unlock()
		if !ok {
			_ = m.sender.Send(&protocol.Reply{MsgID: v.MsgID, OK: false, Error: "unknown forward_id"})
			return
		}
		c, err := net.Dial("tcp", r.DestAddr())
		if err != nil {
			_ = m.sender.Send(&protocol.Reply{MsgID: v.MsgID, OK: false, Error: "dial: " + err.Error()})
			return
		}
		m.mu.Lock()
		m.streams[v.StreamID] = c
		m.mu.Unlock()
		_ = m.sender.Send(&protocol.Reply{MsgID: v.MsgID, OK: true})
		go m.readPump(v.StreamID, c, r.Device)
	case *protocol.ForwardData:
		m.mu.Lock()
		c := m.streams[v.StreamID]
		m.mu.Unlock()
		if c == nil {
			return
		}
		if _, err := c.Write(v.Data); err != nil {
			m.closeStream(v.StreamID)
		}
	case *protocol.ForwardClose:
		switch v.Half {
		case "write":
			m.mu.Lock()
			c := m.streams[v.StreamID]
			m.mu.Unlock()
			if tcp, ok := c.(*net.TCPConn); ok {
				tcp.CloseWrite()
			}
		case "read":
			m.mu.Lock()
			c := m.streams[v.StreamID]
			m.mu.Unlock()
			if tcp, ok := c.(*net.TCPConn); ok {
				tcp.CloseRead()
			}
		default:
			m.closeStream(v.StreamID)
		}
	case *protocol.Event:
		if v.Kind == "device_online" {
			m.onDeviceOnline(v.Device)
		}
	case *protocol.Reply:
		if !v.OK {
			// Dial/bind failure — close any matching stream identified by msg_id.
			m.closeStream(v.MsgID)
		}
	}
}

// onDeviceOnline re-issues forward_listen for every R rule targeting `host`.
func (m *ForwardManager) onDeviceOnline(host string) {
	for _, r := range m.rules {
		if r.Dir == forward.DirRemote && r.Device == host {
			r := r
			m.startRemote(context.Background(), &r)
		}
	}
}

func (m *ForwardManager) readPump(sid string, c net.Conn, device string) {
	buf := make([]byte, 32*1024)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			_ = m.sender.Send(&protocol.ForwardData{
				Target: device, StreamID: sid, Data: append([]byte(nil), buf[:n]...),
			})
		}
		if err != nil {
			half := "both"
			if err == io.EOF {
				half = "write"
			}
			_ = m.sender.Send(&protocol.ForwardClose{Target: device, StreamID: sid, Half: half})
			m.closeStream(sid)
			return
		}
	}
}

func (m *ForwardManager) closeStream(sid string) {
	m.mu.Lock()
	c := m.streams[sid]
	delete(m.streams, sid)
	m.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

func (m *ForwardManager) Stop() {
	m.mu.Lock()
	lns := m.listeners
	streams := m.streams
	m.listeners = map[string]net.Listener{}
	m.streams = map[string]net.Conn{}
	m.mu.Unlock()
	for _, ln := range lns {
		ln.Close()
	}
	for _, c := range streams {
		c.Close()
	}
}

// helper to please govet on `strconv` import absence in callers.
var _ = strconv.Itoa
```

- [ ] **Step 4: Deliver forward frames in `Conn.RPC`**

In `internal/client/rpc.go`, extend the `Deliver` switch so forward frames reach a registered manager. Add to `RPC`:

```go
type RPC struct {
	mu      sync.Mutex
	replies map[string]chan *protocol.Reply
	streams map[string]chan protocol.Message
	forward func(protocol.Message)
}

func (r *RPC) SetForwardHandler(f func(protocol.Message)) {
	r.mu.Lock()
	r.forward = f
	r.mu.Unlock()
}
```

In `Deliver`, add cases:

```go
	case *protocol.ForwardDial, *protocol.ForwardData, *protocol.ForwardClose, *protocol.Event:
		r.mu.Lock()
		f := r.forward
		r.mu.Unlock()
		if f != nil {
			f(m)
		}
		// Also fall through to existing Reply handling for ForwardDial replies?
		// No — replies are *protocol.Reply, handled in the case above.
```

(For `*protocol.Reply` from forward operations, the existing Reply case still delivers it to anyone registered; the forward manager registers a per-msg_id reply channel only if it needs the reply. For unsolicited `Reply` matches via forward path, also call `forward`:)

Update the `*protocol.Reply` case to also fan to forward:

```go
	case *protocol.Reply:
		r.mu.Lock()
		ch, ok := r.replies[m.MsgID]
		f := r.forward
		r.mu.Unlock()
		if ok {
			select {
			case ch <- m:
			default:
			}
			return
		}
		if f != nil {
			f(m)
		}
```

- [ ] **Step 5: Wire into `tether mcp`**

In `internal/cli/mcp.go`, after `WaitReady`:

```go
	fm := client.NewForwardManager(c, cfg.Forwards)
	c.RPC().SetForwardHandler(fm.Deliver)
	fm.Start(ctx)
	defer fm.Stop()
```

`client.NewConn` exposes `Send` already; ensure it satisfies the `Sender` interface (it does — has `Send(protocol.Message) error`).

- [ ] **Step 6: Run tests**

Run: `go test ./internal/client/ -v`
Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add internal/client/ internal/cli/mcp.go
git commit -m "client: forward manager — local listeners, dial-back, recovery"
```

---

## Task 9: E2E tests for port forwarding

**Files:**
- Modify: `e2e_test.go`

- [ ] **Step 1: Add E2E tests**

Append to `e2e_test.go` (helpers reused from existing tests; add only what's missing):

```go
func TestE2EForwardLocal(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host"})
	nc.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
	go nc.Run(ctx)
	require.Eventually(t, func() bool { _, ok := s.Registry().Get("e2e-host"); return ok },
		2*time.Second, 20*time.Millisecond)

	// Echo server bound to a known port on the node side (here = same process).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go echoLoop(ln)
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	cfg := client.Config{
		HubURL: cliURL, Token: "secret",
		Forwards: []forward.Rule{{
			Raw: "L 0:e2e-host:127.0.0.1:" + portStr,
			Dir: forward.DirLocal, Bind: "127.0.0.1", ListenPort: 0,
			Device: "e2e-host", DestHost: "127.0.0.1", DestPort: port,
		}},
	}
	c := client.NewConn(cfg)
	go c.Run(ctx)
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, c.WaitReady(cctx))
	ccancel()

	fm := client.NewForwardManager(c, cfg.Forwards)
	c.RPC().SetForwardHandler(fm.Deliver)
	fm.Start(ctx)
	defer fm.Stop()

	require.Eventually(t, func() bool { return fm.LocalAddr(cfg.Forwards[0]) != "" },
		2*time.Second, 20*time.Millisecond)

	conn, err := net.Dial("tcp", fm.LocalAddr(cfg.Forwards[0]))
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("hello"))
	require.NoError(t, err)
	buf := make([]byte, 5)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(buf))
}

func TestE2EForwardRemote(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host"})
	nc.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
	go nc.Run(ctx)
	require.Eventually(t, func() bool { _, ok := s.Registry().Get("e2e-host"); return ok },
		2*time.Second, 20*time.Millisecond)

	// Echo server on client side
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go echoLoop(ln)
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	// R rule asks node to bind 127.0.0.1:0 → client localhost:port
	cfg := client.Config{
		HubURL: cliURL, Token: "secret",
		Forwards: []forward.Rule{{
			Raw: "R e2e-host:0:127.0.0.1:" + portStr,
			Dir: forward.DirRemote, Device: "e2e-host",
			Bind: "127.0.0.1", ListenPort: 0,
			DestHost: "127.0.0.1", DestPort: port,
		}},
	}
	c := client.NewConn(cfg)
	go c.Run(ctx)
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, c.WaitReady(cctx))
	ccancel()

	// Capture the node-side bound port from the Reply.
	gotAddr := make(chan string, 1)
	c.RPC().SetForwardHandler(func(m protocol.Message) {
		if r, ok := m.(*protocol.Reply); ok && r.OK {
			if v, ok := r.Data["listen_addr"].(string); ok {
				select {
				case gotAddr <- v:
				default:
				}
				return
			}
		}
	})
	// Now wire the real forward manager too — chain handlers:
	fm := client.NewForwardManager(c, cfg.Forwards)
	c.RPC().SetForwardHandler(func(m protocol.Message) {
		if r, ok := m.(*protocol.Reply); ok && r.OK {
			if v, ok := r.Data["listen_addr"].(string); ok {
				select {
				case gotAddr <- v:
				default:
				}
			}
		}
		fm.Deliver(m)
	})
	fm.Start(ctx)
	defer fm.Stop()

	var addr string
	select {
	case addr = <-gotAddr:
	case <-time.After(2 * time.Second):
		t.Fatal("no listen_addr reply")
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
}

func echoLoop(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			io.Copy(c, c)
		}(c)
	}
}
```

Add to top-of-file imports: `"io"`, `"net"`, `"strconv"`, `"github.com/qiuxiang/tether/internal/forward"`.

- [ ] **Step 2: Run E2E**

Run: `go test ./... -run E2EForward -v -timeout 30s`
Expected: pass.

- [ ] **Step 3: Run full test suite**

Run: `go test ./... -timeout 60s`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add e2e_test.go
git commit -m "e2e: port forwarding L and R round-trips"
```

---

## Task 10: Documentation

**Files:**
- Modify: `README.md`
- Modify: `docs/design.md`

- [ ] **Step 1: README — add Port forwarding section**

Read `README.md`, find the section after `file_transfer` (around the end), insert a new section:

```markdown
### Port forwarding

Configure `tether mcp` to multiplex TCP forwards over its hub connection:

```yaml
hub_url: "wss://tether.example.com/client"
token: "your-secret-token"
forwards:
  - "L 9000:mac:5037"           # local 127.0.0.1:9000 → mac's localhost:5037
  - "L 0.0.0.0:9000:mac:5037"   # local 0.0.0.0:9000 → mac's localhost:5037
  - "R mac:8080:3000"           # mac's 127.0.0.1:8080 → local 127.0.0.1:3000
  - "R mac:0.0.0.0:8080:3000"   # mac's 0.0.0.0:8080 → local 127.0.0.1:3000
```

Syntax mirrors ssh. `L` = local listener forwarded to the named device; `R` =
remote (node-side) listener forwarded back. `bind` defaults to `127.0.0.1`
and the `host` segment defaults to `localhost`. Only TCP. Rules are loaded
at `tether mcp` startup; restart `tether mcp` to change them. A node going
offline keeps `L` listeners up (the next accept will close immediately) and
re-establishes `R` listeners automatically when it reconnects.
```

- [ ] **Step 2: design.md — flip the non-goal and reference the spec**

In `docs/design.md`:

- §1 「显式非目标」: delete the `❌ 端口转发（ssh -L 等价物）` bullet
- §4 add a row group below the existing tables, or a new sub-section listing the 5 new frame types with a one-liner each
- §9 add: `0. 见 docs/superpowers/specs/2026-05-19-port-forwarding-design.md`

Edits via `Edit` tool against the exact lines.

- [ ] **Step 3: Build the binary to confirm everything still wires**

Run: `make build`
Expected: clean build.

- [ ] **Step 4: Commit**

```bash
git add README.md docs/design.md
git commit -m "docs: port forwarding section in README; design.md updated"
```

---

## Self-Review Notes

- **Spec coverage:** §1 background → Task 10. §2 architecture → Tasks 4-8. §3 config → Tasks 2-3. §4 wire protocol → Task 1, dispatched in Tasks 5-8. §5 errors → covered piecewise in Tasks 5-8 (dial fail = Reply{ok:false}; node offline = hub error; client exit = handleClient defer). §6 tests → Tasks 4-9. §7 implementation slicing → matches task order.
- **Half-close semantics:** `Half:"write"` means "the sender finished writing; receiver should treat its read side as EOF and close its write side toward us." Task 7's `acceptLoop`/`readPump` and Task 8's `readPump` follow this convention.
- **Types:** `forward.Rule`, `forward.DirLocal`, `forward.DirRemote`, `client.ForwardManager`, `node.ForwardHandler`, `hub.ForwardTable`. Method names used in Task 5 (`AddListener`, `OpenStream`, `LookupStream`, `LookupListener`, `CloseStream`, `EvictStreamsForNode`, `EvictStreamsForClient`, `RemoveListenersForClient`, `RemoveListener`) all exist in Task 4.
- **`PeerConn` interface:** existing in `internal/hub` (used by `Router` and `Device.Conn`). `*clientSession` and `*deviceSession` both satisfy it via `SendRaw`/`Close`. `*fakePeer` in tests implements it.
- **`ClientRegistry.List()`:** if it doesn't currently exist, add it as part of Task 6 Step 4 (a one-line method mirroring `Registry.List`).

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-19-port-forwarding.md`. Two execution options:

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration
2. **Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
