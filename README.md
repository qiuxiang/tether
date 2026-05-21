# Tether

A relay service that exposes behind-firewall devices (mac/linux/windows) to AI through an MCP stdio server. Devices hold an outbound WSS connection to a public-net hub; Claude Code runs a local stdio MCP server that connects to the hub to list devices, exec commands, and transfer files — without each device needing its own public address.

## Build

```bash
make build
```

Produces `./tether` (single binary, three subcommands: `serve`, `join`, `mcp`).

## Config file

All three subcommands (`serve`, `join`, `mcp`) read `~/.config/tether/config.yaml` by default (override with `--config`). A single file can drive any combination of roles — each subcommand reads only the fields its role needs and ignores the rest.

```yaml
# ~/.config/tether/config.yaml
token: "your-secret-token"            # required by every role
listen: ":7000"                       # hub: address to listen on (default :7000)
hub_url: "wss://tether.example.com/device"  # node + MCP client: hub WebSocket URL
hostname_override: ""                 # node: registered hostname (defaults to os.Hostname())
forwards:                             # node: port-forward rules (optional)
  - "L 9000:mac:5037"
```

## Run the hub (public-net machine)

```bash
./tether serve
```

Put nginx/caddy in front of port 7000 for TLS. The hub serves three paths:
- `/device` — WSS endpoint for nodes (`tether join`)
- `/client` — WSS endpoint for MCP control clients (`tether mcp`)
- `/health` — liveness probe

## Run a node (each behind-firewall device)

```bash
./tether join
```

The node will connect, register, and accept commands from the hub. Commands are run as plain subprocesses via `sh -c`; for long-running or interactive work, run `tmux` through the `exec` tool.

## Run the MCP client (your local machine)

```bash
./tether mcp
```

The `tether mcp` subcommand runs a stdio MCP server that holds an outbound WSS connection to the hub's `/client` endpoint and translates MCP tool calls into hub-routed requests.

## Wire up Claude Code

```json
{
  "mcpServers": {
    "tether": {
      "command": "/usr/local/bin/tether",
      "args": ["mcp"]
    }
  }
}
```

6 tools become available: `list_devices`, `exec`, `file_transfer`, `read_file`, `write_file`, `edit_file`.

### exec

Runs a shell command on a device as a plain subprocess (`sh -c`), waits for it to exit, and returns its output:

```
exec(device, cmd, cwd?, env?, timeout?=30)
  → {stdout, stderr, exit_code, timed_out, truncated}
```

If the command does not finish within `timeout` seconds (default 30), the node kills the whole process group and returns `timed_out=true` with whatever output was captured so far. Each of `stdout` and `stderr` is capped at 4 MiB; if either is truncated, `truncated=true` is set.

For long-running or interactive work, run `tmux` through this tool — e.g. `tmux new-session -d -s foo 'long-running-cmd'` to start a session, `tmux capture-pane -pt foo` to read its output, and `tmux send-keys -t foo 'r' Enter` to send keystrokes.

### file_transfer

Single-file transfers between the local machine and a node, between two nodes, or within a node:

```
file_transfer(from, to, overwrite=false)
```

Path syntax:

- `node:/abs/path` or `node:~/path` — a path on that node.
- `/abs/path` or `~/path` — a path on the machine running `tether mcp` (Claude Code's host).

Returns `{ok, bytes, sha256, duration_ms}` on success, or `{ok:false, error:"..."}` on failure.

Single file only — directories are not supported (use tar/zip on the source first). Default behavior refuses to overwrite an existing destination.

### read_file / write_file / edit_file

In-place file editing on a node, mirroring Claude Code's built-in Read / Write / Edit tools but with `node:` paths.

~~~
read_file(path, offset?=0, limit?=2000)
  → {lines, total_lines, truncated, sha256, binary}

write_file(path, content, overwrite?=false, create_dirs?=false)
  → {bytes, sha256}

edit_file(path, old_string, new_string, replace_all?=false)
  → {replacements, sha256}
~~~

All paths must be `node:/abs/path` or `node:~/path` — local files are handled by Claude Code's built-in tools. Writes are atomic (temp file + fsync + rename in the same directory). `edit_file` requires `old_string` to occur exactly once unless `replace_all=true`. The 10 MB per-file limit applies to all three — use `file_transfer` for anything larger.

### Port forwarding

Add `forwards` rules to the node's `~/.config/tether/config.yaml` to multiplex
TCP forwards over the hub:

```yaml
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

## Service files

systemd unit templates are in `dist/systemd/`.

## Design

See `docs/design.md` for the full design (architecture, wire protocol, exec model).
