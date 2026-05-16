# Tether

A relay service that exposes behind-firewall devices (mac/linux/windows) to AI through an MCP HTTP/SSE server. Devices hold an outbound WSS connection to a public-net hub; the hub also serves an MCP endpoint that Claude Code (or any MCP client) can use to list devices, exec commands, and manage long-running processes — without each device needing its own public address.

## Build

```bash
make build
```

Produces `./tether` (single binary, two subcommands).

## Run the hub (public-net machine)

Create `/etc/tether/config.yaml`:

```yaml
listen: ":7000"
token: "your-secret-token"
```

Run: `./tether serve --config /etc/tether/config.yaml`

Put nginx/caddy in front of port 7000 for TLS. The hub serves three paths:
- `/device` — WS endpoint for nodes
- `/mcp` — MCP HTTP/SSE (requires `Authorization: Bearer <token>`)
- `/health` — liveness probe

## Run a node (each behind-firewall device)

Create `~/.config/tether/config.yaml`:

```yaml
hub_url: "wss://tether.example.com/device"
token: "your-secret-token"
```

Run: `./tether join`

The node will connect, register, and accept commands from the hub.

## Wire up Claude Code

```json
{
  "mcpServers": {
    "tether": {
      "transport": "http",
      "url": "https://tether.example.com/mcp",
      "headers": { "Authorization": "Bearer your-secret-token" }
    }
  }
}
```

7 tools become available: `list_devices`, `exec`, `start_process`, `list_processes`, `get_output`, `send_stdin`, `kill_process`.

## Service files

systemd unit templates are in `dist/systemd/`.

## Design

See `docs/design.md` for the full design (architecture, wire protocol, process model).
