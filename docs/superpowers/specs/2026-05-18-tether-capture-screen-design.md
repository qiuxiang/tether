# Capture Screen: Rendered Process Output

Status: Approved (design)
Date: 2026-05-18

## Problem

Tether's current `get_output` returns the raw byte stream of a process's stdout/stderr (PTY or pipe). When the process is interactive or progress-heavy (e.g. `flutter run`, build tools with spinners), that stream contains heavy ANSI noise: color escape codes, cursor moves, carriage-return overwrites, braille spinner frames. The AI consuming the output wastes tokens on noise and must mentally re-render the screen to find the actual visible state.

We want a way for the AI to get the rendered terminal state — what would be visible on screen if the bytes were played through a real terminal — analogous to `tmux capture-pane`.

## Goals

- AI can ask for the rendered screen state of any process with one call.
- ANSI sequences, CR overwrites, cursor moves, clear-screen, etc. are resolved before bytes reach the AI.
- Colors are stripped (plain text only) to minimize tokens.
- Works for both `tty: true` and `tty: false` processes.
- Optional line-range parameters allow grabbing historical chunks of scrollback (tmux semantics).
- Full historical raw bytes remain accessible via the existing log file + `file_transfer`.

## Non-Goals

- Streaming / `tail -f` style following. Snapshot model only; AI polls when it wants the latest.
- Configurable VT dimensions. Fixed at 200 cols × 10000 row scrollback.
- Preserving colors, bold, or other display attributes.
- Cursor visibility flags, window titles, alternate screen indicators, or other VT metadata beyond cursor position.
- Filtering or grepping output server-side.

## Breaking Change

`get_output` (MCP tool, protocol message, node handler, and `Process.ReadOutput`) is removed. Callers that need:

- **Live rendered view** → use `capture_screen`.
- **Full raw bytes / historical archive beyond scrollback** → call `list_processes`, take the new `log_path` field, fetch via `file_transfer`.

Tether is not widely deployed, so a hard cutover is acceptable.

## Architecture

```
process stdout/stderr  (PTY master or pipe)
        │
        ▼
   io.MultiWriter
   ┌─────┴─────┐
   ▼           ▼
 logFile     vtSink ──► vt10x.Terminal (200 × 10000)
 (existing)             ↑ Process.vtMu
                        │
                        ▼
                 Process.CaptureScreen(startLine, endLine)
                   → rendered lines + cursor + total_lines
```

- Each `Process` owns one `vt10x.Terminal` instance, constructed in `Start()`.
- PTY copy loop and non-PTY pipe writers are wrapped: `io.Copy(io.MultiWriter(logFile, &vtSink{p}), src)`. `vtSink` acquires `Process.vtMu` before each `vt.Write`.
- The log file path and contents are unchanged — full raw byte stream is preserved on disk.
- VT instance lives as long as the `Process` struct (until evicted from the registry), so capture works after the process has exited.

### Memory budget

200 cols × 10000 rows × ~24 bytes per cell ≈ 50 MB per process worst case. A node hosting 50 concurrent processes tops out around 2.5 GB of VT memory. Acceptable for current deployments. If this becomes a concern, scrollback rows can be lowered in a follow-up without API change.

### Concurrency

- `vt10x.Terminal` is not safe for concurrent use.
- `Process.vtMu sync.Mutex` serializes every `vt.Write` (writer side, PTY copy goroutine) and every `vt.Cell` / `vt.Cursor` read (reader side, handler goroutine).
- Lock holds are short: a single buffer write (up to PTY read buffer, ~32 KiB) or a single capture traversal over the requested line range.

## API

### MCP tool: `mcp__tether__capture_screen`

```
Input
  device      string   required
  process_id  string   required
  start_line  int?     optional — tmux-style index; negative = from end, 0 = top of scrollback,
                       omitted = from top
  end_line    int?     optional — same semantics; omitted = current last line
                       Out-of-range values are clamped, not rejected.

Output
  lines        string[]   rendered lines in the requested range, trailing whitespace stripped per line
  cursor       {row,col}  cursor position. `row` is the absolute scrollback line index (0-based,
                          same coordinate space as start_line/end_line). `col` is 0-based from the
                          left. If the cursor is outside the requested range, the caller can still
                          see where it is.
  cols         int        always 200
  total_lines  int        number of lines that have received any content (i.e. highest written
                          line index + 1). Empty trailing scrollback rows are not counted.
```

### MCP tool: `mcp__tether__list_processes` (modified)

Each item gains:

```
log_path  string   absolute path on the node host to the raw byte log file
```

AI uses this with `file_transfer` to retrieve the full historical byte stream when needed (e.g. when content has scrolled past the 10000-line buffer).

### Protocol message

Add to `internal/protocol/messages.go`:

```go
type CaptureScreen struct {
    MsgID     string
    ProcessID string
    StartLine *int  // nil = from top
    EndLine   *int  // nil = to bottom
}
```

Reply uses the existing `protocol.Reply`. `Data` carries the output fields above.

### Removed

- MCP tool `mcp__tether__get_output`
- Protocol message `protocol.GetOutput` (and its codec registration)
- Node handler `handleGetOutput`
- `Process.ReadOutput`
- Client-side MCP registration of `get_output`

## Component Changes

### `go.mod`
- Add `github.com/hinshun/vt10x`.

### `internal/node/process.go`
- `Process` gains `vt vt10x.Terminal` and `vtMu sync.Mutex`.
- `Start()` constructs the VT (200 × 10000) before launching the command.
- Non-PTY path: `cmd.Stdout` and `cmd.Stderr` are set to `io.MultiWriter(logFile, &vtSink{p})`.
- Add `vtSink` (an `io.Writer` adapter that grabs `vtMu` and calls `vt.Write`).
- Add method `CaptureScreen(startLine, endLine *int) (lines []string, cursorRow, cursorCol, totalLines int)`.
  - Acquires `vtMu`.
  - Resolves omitted/negative indices against current total line count, clamps to valid range.
  - Iterates `vt.Cell(row, col)` over the resolved range, builds strings, trims trailing whitespace per line.
  - Reads cursor from `vt.Cursor()`.
- Remove `Process.ReadOutput`.

### `internal/node/pty.go`
- `io.Copy(logFile, p)` → `io.Copy(io.MultiWriter(logFile, &vtSink{proc}), p)`.

### `internal/node/handler.go`
- Switch in `Handle()` adds `case *protocol.CaptureScreen` and removes the `*protocol.GetOutput` arm.
- Add `handleCaptureScreen(send, m)`: registry lookup → `p.CaptureScreen(m.StartLine, m.EndLine)` → reply with structured `Data`.
- Remove `handleGetOutput`.
- `handleList` includes `log_path` in each item.

### `internal/protocol/messages.go` and `codec.go`
- Add `CaptureScreen` message type and register encode/decode.
- Remove `GetOutput` type and registration.

### `internal/client/`
- New file `tools_capture.go`: MCP tool registration for `capture_screen`. Translates MCP args → `protocol.CaptureScreen`, forwards via hub, awaits reply, returns `Data` to MCP caller.
- Remove `get_output` registration block in `internal/client/tools_exec.go` (lines around 227–260).
- Update `cmd/probe/main.go` to use `capture_screen` in its smoke-test path (currently calls `get_output` at lines 81 and 86).
- `list_processes` tool surfaces the new `log_path` field in its response.

### `README.md` / `docs/`
- Update protocol docs to reflect the new tool and the `get_output` removal.
- Add a short note on the historical-log workflow: `list_processes` → `log_path` → `file_transfer`.

## Error Handling

| Condition | Behavior |
|---|---|
| Process not found | `Reply{OK: false, Error: "process not found"}` |
| `start_line`/`end_line` out of range or inverted | Clamp to `[0, total_lines)`; if `start > end` after clamping, return empty `lines` with correct `total_lines` and `cursor`. Never error. |
| VT not initialized (shouldn't happen — every process gets one) | `Reply{OK: false, Error: "vt not initialized"}` as a defensive guard |
| `vt.Write` errors | vt10x's `Write` does not return errors; nothing to handle |

## Testing

### Unit — `internal/node/process_test.go`
- Basic render: process writes `"hello\nworld\n"`, after exit `CaptureScreen(nil, nil)` returns `["hello", "world"]`, cursor on row 3.
- ANSI stripped: input `"\x1b[31mred\x1b[0m\n"` → `["red"]`, no escape chars present.
- CR overwrite: input `"foo\rbar\n"` → `["bar"]`.
- Clear screen + cursor home: input `"line1\nline2\n\x1b[2J\x1b[H"` → screen empty after.
- Line range: write 100 lines; `start=-10,end=-1` returns last 10; `start=50,end=59` returns middle 10; `start=-9999` clamps to top.
- Empty buffer: brand-new process with no output → `lines: []`, `total_lines: 0`, no panic.
- Non-TTY process: `Start(..., tty=false)` — VT still receives writes; capture returns rendered content.

### Race — `internal/node/process_test.go` (run under `-race`)
- One process continuously writing, two goroutines: PTY copy + capture loop polling for 1 s. Verify no race detected.

### Protocol — `internal/protocol/codec_test.go`
- `CaptureScreen` round-trip encode/decode, including `StartLine`/`EndLine` set and nil cases.

### Handler — `internal/node/handler_test.go`
- Process not found → `OK: false`.
- Happy path → `Data` matches expected `lines`/`cursor`/`cols`/`total_lines` shape.

### End-to-end — `e2e_test.go`
- Through the hub: client sends `capture_screen` → hub routes → node renders → reply. Assert structured output.
- `list_processes` returns `log_path` and the path exists on disk.

## Open Questions

None at design time. Implementation is expected to surface the exact `vt10x` API shape for cursor and scrollback iteration; if the library does not expose total scrollback line count directly, `Process` will track it independently (incrementing on newline writes).
