# Unified Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the three separate config types (hub, node, client) with a single `config.Config` struct and one `config.Load`, so one file can drive any combination of roles.

**Architecture:** One `Config` struct holds every field; a role-agnostic `Load` parses the file, defaults `listen`, and validates only `token`. Each subcommand checks its own role-specific requirements. `internal/client/config.go` is deleted and `client.NewConn` takes plain strings.

**Tech Stack:** Go, `gopkg.in/yaml.v3`, `internal/forward` for forward-rule parsing.

**Spec:** `docs/superpowers/specs/2026-05-21-unified-config-design.md`

---

## File Structure

- `internal/config/config.go` — rewritten: `Config`, `raw`, `Load`. Old `Hub`/`Node`/`rawNode`/`LoadHub`/`LoadNode` deleted.
- `internal/config/config_test.go` — rewritten for `Load`.
- `internal/client/config.go` — **deleted**.
- `internal/client/conn.go` — `NewConn` signature change; `Conn` stores `hubURL`/`token`.
- `internal/cli/serve.go`, `join.go`, `mcp.go` — use `config.Load`, unified default path, role checks.
- `internal/client/tools_exec_test.go`, `e2e_test.go` — `NewConn` call sites updated.
- `docs/design.md`, `README.md` — config sections rewritten.

---

## Task 1: Unified `config.Config` + `Load`, rewire subcommands

**Files:**
- Modify: `internal/config/config.go`
- Test: `internal/config/config_test.go`
- Modify: `internal/cli/serve.go`, `internal/cli/join.go`, `internal/cli/mcp.go`

This task replaces the config package and updates the three subcommands that call it. `internal/client/config.go` is left intact here (removed in Task 2); `mcp.go` bridges through it temporarily.

- [ ] **Step 1: Write the new config tests**

Replace the entire contents of `internal/config/config_test.go` with:

```go
package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/qiuxiang/tether/internal/forward"
)

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "token: abc\nlisten: \":8080\"\nhub_url: wss://x.example/device\nhostname_override: host-a\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Token != "abc" || cfg.Listen != ":8080" {
		t.Fatalf("got %+v", cfg)
	}
	if cfg.HubURL != "wss://x.example/device" || cfg.HostnameOverride != "host-a" {
		t.Fatalf("got %+v", cfg)
	}
}

func TestLoadDefaultsListen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("token: abc\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":7000" {
		t.Fatalf("expected default listen :7000, got %q", cfg.Listen)
	}
}

func TestLoadMissingToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("listen: \":7000\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected error on missing token")
	}
}

func TestLoadForwards(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "token: t\nhub_url: ws://x\nforwards:\n  - \"L 9000:mac:5037\"\n  - \"R mac:8080:3000\"\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Forwards) != 2 {
		t.Fatalf("got %d rules", len(cfg.Forwards))
	}
	if cfg.Forwards[0].Dir != forward.DirLocal || cfg.Forwards[1].Dir != forward.DirRemote {
		t.Fatalf("dirs wrong: %+v", cfg.Forwards)
	}
}

func TestLoadForwardsInvalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "token: t\nhub_url: ws://x\nforwards: [\"L bogus\"]\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/config/`
Expected: FAIL — compile error, `undefined: Load` (the old package only has `LoadHub`/`LoadNode`).

- [ ] **Step 3: Rewrite `config.go`**

Replace the entire contents of `internal/config/config.go` with:

```go
package config

import (
	"errors"
	"os"

	"github.com/qiuxiang/tether/internal/forward"
	"gopkg.in/yaml.v3"
)

// Config is the unified configuration for every tether role. A single file
// can drive the hub, a node, the MCP client, or any combination — each
// subcommand reads the fields it needs and ignores the rest.
type Config struct {
	Token            string         // every role
	Listen           string         // hub
	HubURL           string         // node + client
	HostnameOverride string         // node
	Forwards         []forward.Rule // node
}

// raw mirrors the YAML file. forwards is decoded as []string and parsed into
// []forward.Rule, so a separate decode struct is needed.
type raw struct {
	Token            string   `yaml:"token"`
	Listen           string   `yaml:"listen"`
	HubURL           string   `yaml:"hub_url"`
	HostnameOverride string   `yaml:"hostname_override"`
	Forwards         []string `yaml:"forwards"`
}

// Load reads and parses a unified config file. It validates only token (the
// one field every role requires); role-specific requirements such as hub_url
// are checked by the subcommand that needs them.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r raw
	if err := yaml.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	if r.Token == "" {
		return nil, errors.New("config: token is required")
	}
	if r.Listen == "" {
		r.Listen = ":7000"
	}
	rules, err := forward.ParseAll(r.Forwards)
	if err != nil {
		return nil, err
	}
	return &Config{
		Token:            r.Token,
		Listen:           r.Listen,
		HubURL:           r.HubURL,
		HostnameOverride: r.HostnameOverride,
		Forwards:         rules,
	}, nil
}
```

- [ ] **Step 4: Run the config tests to verify they pass**

Run: `go test ./internal/config/ -v`
Expected: PASS (6 tests). Note: `go build ./...` will still FAIL at this point because `serve.go`/`join.go` call the now-deleted `config.LoadHub`/`LoadNode` — fixed in Steps 5–7.

- [ ] **Step 5: Update `serve.go`**

In `internal/cli/serve.go`, change the `--config` default and the loader call. Replace:

```go
	configPath := fs.String("config", "/etc/tether/config.yaml", "Path to config")
```

with:

```go
	configPath := fs.String("config", expandHome("~/.config/tether/config.yaml"), "Path to config")
```

and replace:

```go
	cfg, err := config.LoadHub(*configPath)
```

with:

```go
	cfg, err := config.Load(*configPath)
```

The rest of `serve.go` is unchanged — it already uses `cfg.Token` and `cfg.Listen`, both present on the unified `Config`. (`expandHome` already exists in `internal/cli/join.go`, same package.)

- [ ] **Step 6: Update `join.go`**

In `internal/cli/join.go`, change the loader call and add the role check. Replace:

```go
	cfg, err := config.LoadNode(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
```

with:

```go
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if cfg.HubURL == "" {
		fmt.Fprintln(stderr, "config: hub_url is required")
		return 1
	}
```

The `--config` default in `join.go` is already `expandHome("~/.config/tether/config.yaml")` — leave it. The rest of `join.go` is unchanged; it uses `cfg.HubURL`, `cfg.Token`, `cfg.HostnameOverride`, `cfg.Forwards`, all on the unified `Config`.

- [ ] **Step 7: Update `mcp.go`**

In `internal/cli/mcp.go`, change the `--config` default, the loader call, add the role check, and bridge to the (still-existing) `client.Config`. Replace:

```go
	configPath := fs.String("config", expandHome("~/.config/tether/client.yaml"), "Path to client config")
```

with:

```go
	configPath := fs.String("config", expandHome("~/.config/tether/config.yaml"), "Path to config")
```

and replace:

```go
	cfg, err := client.Load(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	c := client.NewConn(*cfg)
```

with:

```go
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if cfg.HubURL == "" {
		fmt.Fprintln(stderr, "config: hub_url is required")
		return 1
	}
	c := client.NewConn(client.Config{HubURL: cfg.HubURL, Token: cfg.Token})
```

Update the import block: `mcp.go` now imports `github.com/qiuxiang/tether/internal/config` in addition to `internal/client`. (The `client.NewConn(client.Config{...})` bridge is temporary — Task 2 removes it.)

- [ ] **Step 8: Verify no stale references and the full build**

Run: `grep -rn "LoadHub\|LoadNode\|config\.Hub\b\|config\.Node\b" --include=*.go .`
Expected: no hits.

Run: `go build ./... && go test ./...`
Expected: build succeeds; full suite PASSES.

- [ ] **Step 9: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go internal/cli/serve.go internal/cli/join.go internal/cli/mcp.go
git commit -m "config: unify hub/node/client into one Config and Load"
```

---

## Task 2: Delete `client.Config`, simplify `NewConn`

**Files:**
- Modify: `internal/client/conn.go`
- Delete: `internal/client/config.go`
- Modify: `internal/cli/mcp.go`
- Test: `internal/client/tools_exec_test.go`, `e2e_test.go`

`client.Config` only exists to hold `HubURL`/`Token` for `NewConn`. The unified `config.Config` now supplies those, so `client.Config` is redundant.

- [ ] **Step 1: Change `Conn` and `NewConn` in `conn.go`**

In `internal/client/conn.go`, replace the `Conn` struct's `cfg` field and `NewConn`. Replace:

```go
type Conn struct {
	cfg   Config
	rpc   *RPC

	mu    sync.Mutex
	ws    *websocket.Conn
	ready chan struct{} // closed on each successful (re)connect
}

func NewConn(cfg Config) *Conn {
	return &Conn{cfg: cfg, rpc: NewRPC(), ready: make(chan struct{})}
}
```

with:

```go
type Conn struct {
	hubURL string
	token  string
	rpc    *RPC

	mu    sync.Mutex
	ws    *websocket.Conn
	ready chan struct{} // closed on each successful (re)connect
}

func NewConn(hubURL, token string) *Conn {
	return &Conn{hubURL: hubURL, token: token, rpc: NewRPC(), ready: make(chan struct{})}
}
```

Then update the two `c.cfg` reads in the same file. At the `websocket.Dial` call, replace `c.cfg.HubURL` with `c.hubURL`. At the hello/handshake build, replace `c.cfg.Token` with `c.token`. (Use `grep -n "c\.cfg" internal/client/conn.go` to find both — they are the only two.)

- [ ] **Step 2: Delete `internal/client/config.go`**

```bash
git rm internal/client/config.go
```

- [ ] **Step 3: Update `mcp.go` to the new `NewConn`**

In `internal/cli/mcp.go`, replace:

```go
	c := client.NewConn(client.Config{HubURL: cfg.HubURL, Token: cfg.Token})
```

with:

```go
	c := client.NewConn(cfg.HubURL, cfg.Token)
```

- [ ] **Step 4: Update test call sites**

In `internal/client/tools_exec_test.go`, replace:

```go
	c := NewConn(Config{HubURL: cliURL, Token: "tk"})
```

with:

```go
	c := NewConn(cliURL, "tk")
```

In `e2e_test.go`, there are four identical lines:

```go
	c := client.NewConn(client.Config{HubURL: cliURL, Token: "secret"})
```

Replace every occurrence with:

```go
	c := client.NewConn(cliURL, "secret")
```

(Use `grep -n "client.NewConn(client.Config" e2e_test.go` to confirm all four are updated.)

- [ ] **Step 5: Verify no stale references and the full build**

Run: `grep -rn "client\.Config\|client\.Load\b" --include=*.go .`
Expected: no hits.

Run: `go build ./... && go test ./...`
Expected: build succeeds; full suite PASSES.

- [ ] **Step 6: Commit**

```bash
git add internal/client/conn.go internal/cli/mcp.go internal/client/tools_exec_test.go e2e_test.go
git commit -m "client: drop client.Config; NewConn takes hubURL and token"
```

---

## Task 3: Documentation

**Files:**
- Modify: `docs/design.md`, `README.md`

- [ ] **Step 1: Update `docs/design.md`**

Run: `grep -n "config\|配置\|log_dir\|hub.yaml\|client.yaml\|listen\|hub_url" docs/design.md`

Locate the node/hub config section (spec references §7). Rewrite it to describe the single unified config file. The canonical example to document:

```yaml
# ~/.config/tether/config.yaml — one file, any role
token: "shared-secret"          # required by every role
listen: ":7000"                 # hub: address to listen on (default :7000)
hub_url: "wss://hub.example/device"  # node + client: hub WebSocket URL
hostname_override: "my-host"    # node: registered hostname (optional)
forwards:                       # node: port-forward rules (optional)
  - "L 2022:mac:22"
```

State that each subcommand reads only the fields its role needs and ignores the rest, so a host running multiple roles uses one file. Remove any description of separate `hub.yaml` / `client.yaml` files and any mention of `log_dir`.

- [ ] **Step 2: Update `README.md`**

Run: `grep -n "config\|log_dir\|hub.yaml\|client.yaml\|hub_url\|listen" README.md`

Merge any separate hub/node/client config examples into the single `config.yaml` shown above. Update the default config path references to `~/.config/tether/config.yaml`. Remove `log_dir` if mentioned.

- [ ] **Step 3: Sanity-check for stale references**

Run: `grep -rn "log_dir\|client.yaml\|LoadHub\|LoadNode" --include=*.md docs/design.md README.md`
Expected: no hits (occurrences inside `docs/superpowers/` plan/spec files are historical records and are not edited).

- [ ] **Step 4: Commit**

```bash
git add docs/design.md README.md
git commit -m "docs: describe the unified config file"
```

---

## Final Verification

- [ ] Run `go build ./...` — succeeds.
- [ ] Run `go test ./...` — full suite passes.
- [ ] Run `go vet ./...` — no warnings introduced.
- [ ] Confirm `grep -rn "LoadHub\|LoadNode\|client\.Config\|client\.Load\b\|log_dir\|LogDir" --include=*.go .` returns nothing.

---

## Notes for the Executor

- Tasks 1 and 2 each end with a green `go build ./...` and `go test ./...`. Task 1 leaves `internal/client/config.go` intact deliberately; Task 2 removes it.
- The working tree may contain untracked files unrelated to this plan (built binaries, scratch configs). Stage only the files each task names — do not `git add -A`.
- The spec is the source of truth: `docs/superpowers/specs/2026-05-21-unified-config-design.md`.
