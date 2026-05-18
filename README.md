# Tether

A relay service that exposes behind-firewall devices (mac/linux/windows) to AI through an MCP stdio server. Devices hold an outbound WSS connection to a public-net hub; Claude Code runs a local stdio MCP server that connects to the hub to list devices, exec commands, and manage long-running processes — without each device needing its own public address.

## Build

```bash
make build
```

Produces `./tether` (single binary, three subcommands: `serve`, `join`, `mcp`).

## Run the hub (public-net machine)

Create `/etc/tether/config.yaml`:

```yaml
listen: ":7000"
token: "your-secret-token"
```

Run: `./tether serve --config /etc/tether/config.yaml`

Put nginx/caddy in front of port 7000 for TLS. The hub serves three paths:
- `/device` — WSS endpoint for nodes (`tether join`)
- `/client` — WSS endpoint for MCP control clients (`tether mcp`)
- `/health` — liveness probe

## Run a node (each behind-firewall device)

Create `~/.config/tether/config.yaml`:

```yaml
hub_url: "wss://tether.example.com/device"
token: "your-secret-token"
hostname_override: ""                          # defaults to os.Hostname()
log_dir: "~/.local/share/tether/logs"          # per-process log files live here
```

Run: `./tether join`

The node will connect, register, and accept commands from the hub. Processes are always launched inside a PTY (200×50 reported to the child, 200×10000 scrollback buffered for `capture_screen`).

## Run the MCP client (your local machine)

Create `~/.config/tether/client.yaml`:

```yaml
hub_url: "wss://tether.example.com/client"
token: "your-secret-token"
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

8 tools become available: `list_devices`, `exec`, `start_process`, `list_processes`, `capture_screen`, `send_stdin`, `kill_process`, `file_transfer`.

### capture_screen

Returns the rendered terminal screen of a running process — ANSI sequences resolved, cursor moves and CR overwrites applied, colors stripped. Works for both `tty` and pipe processes; output is what would appear on a 200-column terminal after the program's bytes are played through it.

```
capture_screen(device, process_id, start_line?, end_line?)
```

`start_line` / `end_line` use tmux semantics: negative indices count from the end, omitted means "extreme" (top for start, bottom for end). Returns `{lines, cursor:{row,col}, cols, total_lines}`.

The virtual terminal holds up to 10000 lines of scrollback. To retrieve raw bytes beyond that (or for binary debugging), use `list_processes` to read each entry's `log_path` and then `file_transfer` to fetch the file.

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

## Service files

systemd unit templates are in `dist/systemd/`.

## Design

See `docs/design.md` for the full design (architecture, wire protocol, process model).
