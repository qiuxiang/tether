# Tether

A relay service that exposes behind-firewall devices (mac/linux/windows) to AI through an MCP stdio server. Devices hold an outbound WSS connection to a public-net hub; Claude Code runs a local stdio MCP server that connects to the hub to list devices, exec commands, and manage long-running processes — without each device needing its own public address.

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
- `/device` — WSS endpoint for nodes (`tether join`)
- `/client` — WSS endpoint for MCP control clients (`tether mcp`)
- `/health` — liveness probe

## Run a node (each behind-firewall device)

Create `~/.config/tether/config.yaml`:

```yaml
hub_url: "wss://tether.example.com/device"
token: "your-secret-token"
```

Run: `./tether join`

The node will connect, register, and accept commands from the hub.

## Run the MCP client (your local machine)

Create `~/.config/tether/client.yaml`:

```yaml
hub_url: "wss://tether.example.com/client"
token: "your-secret-token"
```

The `tether mcp` subcommand runs a stdio MCP server that connects to the hub.

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

7 tools become available: `list_devices`, `exec`, `start_process`, `list_processes`, `get_output`, `send_stdin`, `kill_process`.

## Service files

systemd unit templates are in `dist/systemd/`.

## Design

See `docs/design.md` for the full design (architecture, wire protocol, process model).
