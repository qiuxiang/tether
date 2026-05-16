# File Transfer + MCP Architecture Refactor — Design

Date: 2026-05-16
Status: draft → awaiting user review

## Background and Goals

Current Tether architecture: behind-firewall devices (nodes) hold outbound WSS connections to a public Hub; the Hub exposes an HTTP/SSE MCP endpoint at `/mcp`. Claude Code calls MCP tools over HTTP and the Hub translates those calls into CBOR protocol messages routed to the target node. Seven tools exist today: `list_devices`, `exec`, `start_process`, `list_processes`, `get_output`, `send_stdin`, `kill_process`.

Requirement: **support bidirectional file transfer**, exposed as a single scp-style MCP tool `file_transfer(from, to)`.

This requirement surfaces an architectural constraint: **a remote HTTP MCP server (the Hub) cannot read or write files on the MCP client's machine**. For "local path" semantics to work, an MCP process must run on the user's machine.

This design therefore covers two parts:

1. **Architecture refactor (Phase 1)** — remove the Hub's `/mcp` endpoint, reduce the Hub to a pure WS message relay, and add a new `tether mcp` subcommand that runs as a local stdio MCP process. All seven existing tools move to this local implementation.
2. **file_transfer implementation (Phase 2)** — add six file-related protocol messages and wire up client/hub/node logic.

Both phases share this spec but ship as two independent implementation plans.

## Overall Architecture

```
┌─────────────────────────┐                  ┌─────────────────────────┐
│   Claude Code (user)    │                  │   Tether node (device)  │
│                         │                  │                         │
│  ─── stdio ───►         │                  │     ▲                   │
│       ┌──────────────┐  │                  │     │ WSS (outbound)    │
│       │ tether mcp   │──┼─── WSS ─────────►┼─────┤                   │
│       │ (local proxy)│  │                  │     │                   │
│       └──────────────┘  │     ┌────────┐   │                         │
│                         │     │  Hub   │   │                         │
│                         │     │ (relay)│   │                         │
└─────────────────────────┘     └────────┘   └─────────────────────────┘
```

Three components:

- **`tether serve` (Hub)** — pure WS message relay. Two kinds of peers connect:
  - **node** (`/device`): execution side, registered in the device registry.
  - **client** (`/client`): control side, runs MCP tool calls, registered in the client registry.
  The Hub maintains both registries plus an in-flight routing table (`msg_id → conn`). It no longer understands MCP. The `/mcp` path is removed and `internal/hub/mcp.go` is deleted.
- **`tether join` (node)** — largely unchanged. Continues to provide exec / process capabilities and gains file operations.
- **`tether mcp` (local, new)** — stdio MCP server. On startup, it dials the Hub's `/client` endpoint over WSS using configured credentials. It translates MCP tool calls into CBOR protocol messages, sends them to the Hub for routing to the target node, and turns replies back into MCP responses.

**Authentication:** Hub `/device` and `/client` share a single `token`. The `Hello` message gains a `role` field: `"node"` (default) or `"client"`. MVP has no fine-grained authorization — holding the token grants access to any node.

## Protocol Messages

### Existing (unchanged)

`Exec`, `ExecCancel`, `Start`, `Stdin`, `Kill`, `GetOutput`, `List`, `Ping`, `Hello`, `Reply`, `ExecOutput`, `ExecExit`, `Event`, `Pong`.

### Hello extension

```go
type Hello struct {
    // ... existing fields
    Role string `cbor:"role,omitempty"`  // "node" (default) | "client"
}
```

### Six new file messages

```go
// Download: client → hub → node
type FileGetOpen struct {
    Type  string `cbor:"type"`   // "file_get_open"
    MsgID string `cbor:"msg_id"`
    Path  string `cbor:"path"`
}
// Node replies Reply{ok:true, data:{size:int64, mode:uint32, sha256:string}},
// then pushes FileChunk frames until EOF.

// Upload: client → hub → node
type FilePutOpen struct {
    Type      string `cbor:"type"`   // "file_put_open"
    MsgID     string `cbor:"msg_id"`
    Path      string `cbor:"path"`
    Size      int64  `cbor:"size"`
    Mode      uint32 `cbor:"mode,omitempty"`
    Overwrite bool   `cbor:"overwrite,omitempty"`
    SHA256    string `cbor:"sha256,omitempty"`
}
// Node replies Reply{ok:true} when ready; client pushes FileChunk frames
// until EOF=true; node verifies sha256 and sends the final Reply.

// Streaming chunk, keyed by msg_id
type FileChunk struct {
    Type  string `cbor:"type"`   // "file_chunk"
    MsgID string `cbor:"msg_id"`
    Seq   int64  `cbor:"seq"`
    Data  []byte `cbor:"data"`
    EOF   bool   `cbor:"eof,omitempty"`
}

// Either side aborts
type FileAbort struct {
    Type  string `cbor:"type"`   // "file_abort"
    MsgID string `cbor:"msg_id"`
    Error string `cbor:"error"`
}

// node ↔ node: client → hub only; Hub coordinates both ends
type FileRelay struct {
    Type      string `cbor:"type"`   // "file_relay"
    MsgID     string `cbor:"msg_id"`
    FromNode  string `cbor:"from_node"`
    FromPath  string `cbor:"from_path"`
    ToNode    string `cbor:"to_node"`
    ToPath    string `cbor:"to_path"`
    Overwrite bool   `cbor:"overwrite,omitempty"`
}

// Same-node copy between two paths
type FileLocalCopy struct {
    Type      string `cbor:"type"`   // "file_local_copy"
    MsgID     string `cbor:"msg_id"`
    FromPath  string `cbor:"from_path"`
    ToPath    string `cbor:"to_path"`
    Overwrite bool   `cbor:"overwrite,omitempty"`
}
```

### Protocol notes

- **Chunk size**: fixed at 256 KB.
- **Streaming model**: open establishes a session → unidirectional continuous chunks → final frame with `EOF=true`. No window, no acks — rely on WS/TCP backpressure.
- **Hub forwarding of FileChunk**: the `Data` field is not decoded — frames are forwarded zero-copy.
- **Synchronous semantics**: the MCP tool blocks until completion or abort; no intermediate progress reporting.
- **Size limit**: config `max_file_size_bytes` (default 5 GiB). Nodes reject at `FilePutOpen`; downloads check during the metadata reply.
- **Concurrency limit**: at most 4 in-flight transfers per client (static MVP value).
- **Path rules**: absolute or starts with `~`; `~` is expanded on the executing side.

## MCP Tool Interface

### New tool `file_transfer`

Input schema:

```jsonc
{
  "from": "node:/abs/path | /abs/path | ~/path",
  "to":   "node:/abs/path | /abs/path | ~/path",
  "overwrite": false
}
```

Path syntax:

- `<nodename>:/path` or `<nodename>:~/path` — a path on that node.
- `/path` or `~/path` — a path on the machine running `tether mcp` (the Claude Code host).
- Directories not supported (single file only).
- `from == to` on the same machine is rejected.

Five routing combinations:

| from | to | implementation |
|------|----|----------------|
| local → local | rejected, error `"use os tools"` |
| local → node | client reads local file, pushes to node: `FilePutOpen` → multiple `FileChunk` → final `Reply` |
| node → local | client pulls from node, writes locally: `FileGetOpen` → chunks → local fsync |
| nodeA → nodeB | client sends `FileRelay` to Hub; Hub streams between the two nodes |
| nodeA → nodeA (same node) | client sends `FileLocalCopy`; node performs an OS-level copy |

Return value:

```jsonc
{
  "ok": true,
  "bytes": 1234567,
  "duration_ms": 842,
  "sha256": "abc123..."
}
```

On failure: `ok: false` plus `error: "..."`. Common codes: `path_not_found`, `permission_denied`, `destination_exists`, `device_offline`, `size_limit_exceeded`, `hash_mismatch`, `disk_full`, `source_disconnected`, `dest_disconnected`.

### Other seven MCP tools

`list_devices`, `exec`, `start_process`, `list_processes`, `get_output`, `send_stdin`, `kill_process` — all moved from the Hub to the local `tether mcp` process. Behavior is preserved (params → CBOR over WSS → Hub forwards → reply → MCP response). External behavior is unchanged during MVP; only the transport path is different.

## Hub Relay Flow (node ↔ node)

Clients cannot talk to two nodes at once (the protocol is single-hop client ↔ Hub ↔ node), so the Hub needs a small amount of coordination logic:

1. Client sends `FileRelay { msg_id_X, from_node, from_path, to_node, to_path, overwrite }`.
2. Hub sends `FileGetOpen { msg_id_A, path=from_path }` to `from_node` and awaits the metadata reply.
3. Hub sends `FilePutOpen { msg_id_B, path=to_path, size, sha256, overwrite }` to `to_node` and awaits the ready reply.
4. Streaming state: `from_node` pushes `FileChunk { msg_id_A, ... }` to the Hub; the Hub rewrites the msg_id and forwards `FileChunk { msg_id_B, ... }` to `to_node`.
5. After the EOF frame, `to_node` verifies sha256 and returns the final Reply.
6. The Hub forwards this final Reply to the client (keyed on `msg_id_X`).
7. If either side aborts, the Hub cancels the other in-flight side and reports back to the client.

The Hub is the fan-out point but persists no bytes — at most 1–2 chunks in flight.

## File Layout and Module Boundaries

```
internal/
  cli/
    serve.go            ← change: stop starting MCP
    join.go             ← unchanged
    mcp.go              ← new: tether mcp subcommand entry
  hub/
    server.go           ← change: serve /device /client /health only
    endpoint_ws.go      ← rename of device_ws.go; handles both /device and /client
    registry.go         ← change: two registries (devices, clients)
    router.go           ← change: support sticky routing
    relay.go            ← new: file_relay coordinator
    mcp.go              ← deleted
    mcp_test.go         ← deleted
  client/               ← new package
    config.go           ← local config (hub_url / token)
    conn.go             ← WSS connection to Hub + reconnect
    rpc.go              ← request/response routing
    mcp_server.go       ← stdio MCP server
    tools_exec.go       ← list_devices / exec / process tools
    tools_file.go       ← file_transfer tool
  node/
    handler.go          ← change: dispatch adds file branches
    file.go             ← new: FileGetOpen / FilePutOpen / FileLocalCopy
    process.go / pty.go ← unchanged
  protocol/
    messages.go         ← add six file messages + Hello.Role
    codec.go            ← register new types
```

**Hub router extension (sticky routing):**
The current router is a one-shot "msg_id → single reply channel" model. File chunks are multi-frame with no per-frame reply, so we add a second registration type: "msg_id → forwarding conn", which lasts until EOF or abort. Simplest implementation: a sticky table mapping `msg_id → peer conn`.

**Node atomic writes (`internal/node/file.go`):**

- `FileGetOpen` handler: open file → send Reply with metadata → spawn a goroutine to stream-read → send `FileChunk` frames → EOF.
- `FilePutOpen` handler: validate path / overwrite / writable → `Reply{ok:true}` → receive `FileChunk` frames until EOF → write and verify sha256 → final Reply.
- `FileLocalCopy` handler: `io.Copy` between two local paths through a temp file with atomic rename.
- **Write strategy**: write to `<path>.tether-tmp-<msg_id>`, verify sha256, then `os.Rename` to the destination. On failure, remove the temp file. **The target file is either fully present or fully absent** — never a partial file.

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Node disconnects mid-download | Hub detects `from_node` close and sends `FileAbort{error:"source_disconnected"}` to client; client cleans its partial local file; MCP tool returns error |
| Node disconnects mid-upload | Hub detects `to_node` close; client aborts and stops pushing |
| Hub restart | Both client and node WSS connections reconnect; in-flight transfers abort (no resume in MVP) |
| Client disconnects | Hub cancels related transfers and instructs nodes to remove `.tether-tmp-*` files |
| Disk full on node | Node catches `ENOSPC` during write, sends `FileAbort{error:"disk_full"}`, removes temp file |
| sha256 mismatch | Node does not rename, removes temp file, sends `Reply{ok:false, error:"hash_mismatch"}` |
| Path missing / permission denied | Reported synchronously at the open stage, `ok:false` |
| Size over limit | Rejected at `FilePutOpen`; for downloads, rejected at the metadata stage of `FileGetOpen` |

**Invariants:**

- Node destination file is either fully written or absent (cleanup on failure).
- Hub persists no bytes; only a 1–2 chunk sliding window in memory.
- Aborts propagate to both sides — Hub is the fan-out point.

## Testing Strategy

- **Protocol unit tests** — round-trip CBOR encode/decode for new messages (extend `internal/protocol/codec_test.go`).
- **Hub relay unit tests** (`internal/hub/relay_test.go`) — two in-memory fake conns simulate `from_node` and `to_node`; verify chunk forwarding, abort propagation, msg_id rewriting.
- **Node file unit tests** (`internal/node/file_test.go`) — use `t.TempDir()` to exercise `FileGetOpen`, `FilePutOpen`, `FileLocalCopy`, covering overwrite, missing target, existing target, permission denied, sha mismatch.
- **Client tool unit tests** (`internal/client/tools_file_test.go`) — end-to-end local ↔ fake-node round trips.
- **e2e_test.go extensions** — in the in-memory cluster, drive `tether mcp` ↔ Hub ↔ node, transfer a ~10 MB file and verify sha; cover the node↔node relay path independently.
- **Shutdown path tests** — close conns mid-transfer and verify temp files are cleaned up.

## Implementation Phases

### Phase 1 — Architecture Refactor (no external behavior change)

1. New `internal/client/` package: stdio MCP server skeleton + WSS conn + migrate the seven existing tools.
2. Hub: add `/client` endpoint, extend `Hello.Role`, split the registry, unify both peer kinds in `endpoint_ws.go`.
3. CLI: add `tether mcp` subcommand.
4. Delete `internal/hub/mcp.go` and `mcp_test.go`; `server.go` stops mounting `/mcp`.
5. Update README and systemd templates.
6. Refit `e2e_test.go` to drive the new path; behavior equivalent.
7. **Acceptance**: the seven original tools behave identically; only deployment shape changes (HTTP MCP → local stdio MCP).

### Phase 2 — file_transfer

1. Add six file messages and codec registrations in `protocol/`.
2. Implement `node/file.go` (`FileGetOpen`, `FilePutOpen`, `FileLocalCopy`).
3. Add sticky routing to `hub/router.go`; implement `hub/relay.go` for `file_relay` coordination.
4. Implement `client/tools_file.go` — MCP `file_transfer` tool, local read/write + scheduling all five routing combinations.
5. End-to-end tests + medium-size stress (hundreds of MB).
6. Documentation / README updates.

### Sizing

- Phase 1: ~700–900 lines added, ~400 lines removed.
- Phase 2: ~600–800 lines added.

Each phase ships and verifies independently.

## Non-Goals

- Resumable transfer (Hub restart or conn drop forces a restart from zero).
- Recursive directory transfer (users tar/zip first).
- Progress reporting (synchronous, return-on-completion).
- Compression (rely on WS `permessage-deflate`).
- Multi-token / fine-grained authorization (MVP is single shared token).
- Fair-sharing across concurrent clients on one Hub (MVP uses hard caps only).
- Backwards compatibility after removing `/mcp` (single-shot switch, no deprecation window).
