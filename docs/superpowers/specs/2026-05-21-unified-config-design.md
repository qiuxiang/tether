# Unify hub / node / client config into one format

Date: 2026-05-21

## Motivation

Tether has three config types for its three roles:

- `config.Hub` — `listen`, `token`
- `config.Node` — `hub_url`, `token`, `hostname_override`, `log_dir`, `forwards`
- `client.Config` — `hub_url`, `token`

They overlap heavily: `token` is used by all three, `hub_url` by node and
client. A host running more than one role needs duplicate files with the
token copied verbatim — node05 today carries both `hub.yaml` and
`config.yaml` with the same token. `log_dir` is also dead: after the
process-management refactor nothing reads it.

This unifies the three into a single `config.Config` struct and a single
loader, so one file can serve any combination of roles.

## Scope

In scope:

- One `config.Config` struct with all fields; one `config.Load`.
- Delete `internal/client/config.go`; `client` uses `config.Config` values.
- Drop the dead `log_dir` field.
- One shared default config path for all three subcommands.
- Update `docs/design.md` and `README.md` config sections.

Out of scope: the wire protocol, the `--config` flag itself (kept), and any
change to what the three roles do.

## Decisions

- **Loader is role-agnostic.** `config.Load(path)` parses the file, applies
  universal defaults, and validates only `token` (the one field every role
  requires). Role-specific required fields are checked by each subcommand.
  The `config` package gains no `Role` concept.
- **Backward compatibility is free.** The unified format is a strict superset
  of all three old formats — same YAML keys. `yaml.v3` ignores unknown keys,
  so an old node config still containing `log_dir:` loads fine (key ignored).
  No migration step.
- **Default path changes.** All three subcommands default `--config` to
  `~/.config/tether/config.yaml`. This changes the old defaults for `serve`
  (`/etc/tether/config.yaml`) and `mcp` (`~/.config/tether/client.yaml`).
  Acceptable: the production deployment passes `--config` explicitly
  (systemd units, the dev-box hub command), so bare-default users are the
  only ones affected.
- **`log_dir` is removed**, not retained — it is unused dead code.

## The unified config (`internal/config/config.go`)

`config.go` is rewritten. `Hub`, `Node`, `rawNode`, `LoadHub`, `LoadNode` are
all deleted and replaced by:

```go
package config

type Config struct {
	Token            string
	Listen           string         // hub
	HubURL           string         // node + client
	HostnameOverride string         // node
	Forwards         []forward.Rule // node
}

// raw mirrors the YAML file. forwards is parsed from []string into
// []forward.Rule, so a separate decode struct is needed.
type raw struct {
	Token            string   `yaml:"token"`
	Listen           string   `yaml:"listen"`
	HubURL           string   `yaml:"hub_url"`
	HostnameOverride string   `yaml:"hostname_override"`
	Forwards         []string `yaml:"forwards"`
}

func Load(path string) (*Config, error)
```

`Load` behavior:

1. `os.ReadFile(path)` — a missing file is returned as an error.
2. `yaml.Unmarshal` into `raw`.
3. If `raw.Token == ""` → error `config: token is required`.
4. If `raw.Listen == ""` → default to `:7000`.
5. `forward.ParseAll(raw.Forwards)` → `[]forward.Rule`; a parse error is
   returned.
6. Return the assembled `*Config`.

`Load` does **not** validate `hub_url` — it is irrelevant to the hub role.

## Subcommand wiring (`internal/cli/`)

All three subcommands default `--config` to `~/.config/tether/config.yaml`,
expanded through the existing `expandHome` helper. `serve.go` does not
currently use `expandHome`; it is added there.

- **`serve.go`** — `config.Load`; use `cfg.Token`, `cfg.Listen`. No
  role-specific validation.
- **`join.go`** — `config.Load`; then `if cfg.HubURL == ""` → error
  `config: hub_url is required`. Use `cfg.HubURL`, `cfg.Token`,
  `cfg.HostnameOverride`, `cfg.Forwards` as today.
- **`mcp.go`** — `config.Load`; then the same `hub_url` check. Build the
  client connection with `cfg.HubURL` and `cfg.Token`.

## Client package change

`internal/client/config.go` (the `Config` struct and its `Load`) is deleted.

`client.NewConn` currently takes a `client.Config` value. Its signature
becomes:

```go
func NewConn(hubURL, token string) *Conn
```

Every caller is updated mechanically: `mcp.go` passes `cfg.HubURL,
cfg.Token`; `e2e_test.go` and the `internal/client` test files pass their
two string literals directly.

## Testing

- **`internal/config/config_test.go`** is rewritten for the new `Load`:
  - valid file with all fields loads correctly;
  - missing `token` → error;
  - missing `listen` → defaults to `:7000`;
  - `forwards` strings parse into `[]forward.Rule`;
  - a malformed `forwards` entry → error;
  - a non-existent path → error.
  All `log_dir` assertions are removed.
- The `hub_url`-required check in `join`/`mcp` is covered by a lightweight
  test if the `cli` package already has tests to extend; if it has none,
  no new test file is forced — the e2e suite already exercises the happy
  path.
- `go build ./...` and `go test ./...` stay green.

## Documentation

- `docs/design.md` §7 (node config) is rewritten to describe the single
  unified config and its fields, noting which fields each role reads.
- `README.md` config examples are updated to the unified file; the separate
  `hub.yaml` / `client.yaml` examples are merged into one `config.yaml`.

## Out-of-scope / explicitly not done

- No new config fields or features.
- No `/etc` fallback or path auto-discovery — one default path only.
- No migration tooling — old files already load unchanged.
