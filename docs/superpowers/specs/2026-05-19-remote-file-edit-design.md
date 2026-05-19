# Remote File Edit for Tether MCP

**Date:** 2026-05-19
**Status:** Approved for implementation

## Problem

Tether's MCP server exposes 8 tools for driving a remote node, but the only file primitive is `file_transfer`, which moves whole files. To make a small edit on a remote file, the AI has to transfer the file local, edit locally, then transfer back. That is wasteful for small edits and clumsy as a workflow.

## Goal

Add three MCP tools that let the AI read, write, and edit files on a remote node directly, with semantics matching Claude Code's built-in Read / Write / Edit tools so the AI's existing habits transfer.

## Non-goals

- No directory recursion, batch patch, or unified diff. The AI can call edit multiple times.
- No CAS (`expected_sha256` on write). The response carries `sha256` so it can be added later without breaking callers.
- No path allowlist. OS permissions of the node process are the only boundary, matching `file_transfer`.
- No edit history or undo. Users rely on git.

## Tool surface

All three tools require `node:/abs/path` or `node:~/path`. Paths without the `node:` prefix are rejected — local files are handled by Claude Code's built-in tools and must not be confused with remote ones.

```
read_file(path, offset?=0, limit?=2000)
  → {lines: ["...", ...], total_lines, truncated, sha256, binary?}

write_file(path, content, overwrite?=false, create_dirs?=false)
  → {ok, bytes, sha256}

edit_file(path, old_string, new_string, replace_all?=false)
  → {ok, replacements, sha256}
```

### Semantics

- `read_file`: returns `lines` as a slice of the file split on `\n`, starting at `offset` (0-based line index) up to `limit` lines. `total_lines` is the full count. `truncated` is true when more lines exist past the window. `sha256` is the hash of the whole file. If the file contains non-UTF-8 bytes, replacement characters are substituted and `binary: true` is set so the AI knows to fall back to `file_transfer`.
- `write_file`: writes the file atomically (temp file in the same directory, `fsync`, `rename`). `overwrite=false` (default) refuses if the destination exists. `create_dirs=false` (default) refuses if the parent directory is missing.
- `edit_file`: replaces `old_string` with `new_string`. With `replace_all=false` (default), `old_string` must occur exactly once in the file or the call fails with `not_unique` (zero matches → `not_found`). With `replace_all=true`, every occurrence is replaced. Write is atomic, reusing `write_file`'s temp+rename path.

### Error codes

`not_found`, `not_unique`, `exists`, `permission_denied`, `is_directory`, `too_large`. Files larger than 10 MB are rejected on all three operations to bound node memory.

## Protocol (`internal/protocol/messages.go`)

Three new request/response pairs, routed by `msg_id` like the existing `file_transfer` / `exec` flows:

```
ReadFileReq   {path, offset, limit}                              → ReadFileResp  {lines[], total_lines, truncated, sha256, binary, err?}
WriteFileReq  {path, content, overwrite, create_dirs}            → WriteFileResp {bytes, sha256, err?}
EditFileReq   {path, old_string, new_string, replace_all}        → EditFileResp  {replacements, sha256, err?}
```

`content` (and `old_string` / `new_string`) are base64-encoded for binary safety, matching `file_transfer`. Codec tests are extended to cover the three new message types.

## Node implementation (`internal/node/edit.go`, new file)

Three handlers wired into `handler.go`:

- `handleReadFile`: open the file, count lines streaming, emit only lines within `[offset, offset+limit)`. Compute sha256 in the same pass.
- `handleWriteFile`: write to `<path>.tether-<rand>.tmp` in the same directory, `f.Sync()`, then `os.Rename` over the destination. When `overwrite=false`, `Lstat` first because `rename` will clobber. When `create_dirs=true`, `MkdirAll` the parent before opening the temp file.
- `handleEditFile`: `ReadFile`, count occurrences, decide single-replace vs `ReplaceAll`, then reuse `handleWriteFile`'s atomic-write path.

The 10 MB limit is enforced at all three entry points before any allocation.

## Client implementation (`internal/client/tools_edit.go`, new file)

Three MCP tool handlers, thin wrappers:

- Parse `node:/path` via the existing `parseNodePath` helper from `tools_file.go`.
- Reject paths without the `node:` prefix with a clear error — never silently fall back to local.
- Issue the RPC, translate protocol error codes into human-readable MCP errors.
- Register the three tools in `mcp_server.go`'s tool list (count goes from 8 to 11).

## Testing

- `internal/node/edit_test.go`: atomic-write interruption leaves no half-file; `edit_file` uniqueness check; binary-byte fallback; 10 MB cap; `overwrite=false` behavior; `create_dirs` behavior.
- `internal/client/tools_edit_test.go`: RPC mock covering happy path, missing `node:` prefix, each error-code translation.
- `e2e_test.go`: end-to-end `write_file` → `read_file` → `edit_file` → `read_file` round trip against a real node.

## Documentation

- `README.md`: tool count 8 → 11; one section per new tool, written in the same style as `capture_screen` and `file_transfer`.
- `docs/design.md`: add the three protocol messages to the protocol section if one exists.

## Compatibility

No breaking changes. Old clients ignore unknown tool names. A new client talking to an old node will get `unknown method` from the existing hub router — no special handling needed.
