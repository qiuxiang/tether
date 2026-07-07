package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/qiuxiang/tether/internal/protocol"
)

const fileChunkSize = 256 * 1024

type pathSpec struct {
	Node string // empty = local
	Path string // raw (may include "~")
}

func parsePath(s string) (pathSpec, error) {
	if i := strings.Index(s, ":"); i > 0 && !strings.ContainsAny(s[:i], "/.~") {
		return pathSpec{Node: s[:i], Path: s[i+1:]}, nil
	}
	return pathSpec{Path: s}, nil
}

func expandLocal(p string) (string, error) {
	if strings.HasPrefix(p, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		p = home + p[1:]
	}
	if !filepath.IsAbs(p) {
		return "", fmt.Errorf("path must be absolute: %s", p)
	}
	return p, nil
}

func registerFileTool(m *server.MCPServer, c *Conn) {
	m.AddTool(
		mcp.NewTool("file_transfer",
			mcp.WithDescription("Transfer a single file between the local machine and a node, or between two nodes. Paths use 'node:/abs/path' for a node path or '/abs/path' (or '~/path') for the local machine running 'tether mcp'."),
			mcp.WithString("from", mcp.Required()),
			mcp.WithString("to", mcp.Required()),
			mcp.WithBoolean("overwrite"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			fromStr, _ := args["from"].(string)
			toStr, _ := args["to"].(string)
			overwrite, _ := args["overwrite"].(bool)

			from, err := parsePath(fromStr)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			to, err := parsePath(toStr)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}

			switch {
			case from.Node == "" && to.Node == "":
				return mcp.NewToolResultError("use os tools — both paths are local"), nil
			case from.Node == "" && to.Node != "":
				return upload(ctx, c, from.Path, to.Node, to.Path, overwrite)
			case from.Node != "" && to.Node == "":
				return download(ctx, c, from.Node, from.Path, to.Path, overwrite)
			case from.Node == to.Node:
				return sameNodeCopy(ctx, c, from.Node, from.Path, to.Path, overwrite)
			default:
				return relay(ctx, c, from.Node, from.Path, to.Node, to.Path, overwrite)
			}
		},
	)
}

func upload(ctx context.Context, c *Conn, localPath, node, remotePath string, overwrite bool) (*mcp.CallToolResult, error) {
	p, err := expandLocal(localPath)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return mcp.NewToolResultError("path_not_found"), nil
		}
		return mcp.NewToolResultError(err.Error()), nil
	}
	defer f.Close()
	fi, _ := f.Stat()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	sumHex := hex.EncodeToString(h.Sum(nil))
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	id := NewMsgID()
	// Buffer 2: the hub sends both the ok-to-send Reply and the final Reply
	// back on the same msg_id (sticky route). Pre-allocating room for both
	// avoids a race where the final Reply arrives before we drain the channel.
	ch := c.rpc.RegisterBuf(id, 2)
	defer c.rpc.Unregister(id)
	start := time.Now()
	if err := c.Send(&protocol.FilePutOpen{
		MsgID: id, Target: node, Path: remotePath,
		Size: fi.Size(), Mode: uint32(fi.Mode().Perm()),
		Overwrite: overwrite, SHA256: sumHex,
	}); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	// Wait for ok-to-send.
	select {
	case reply := <-ch:
		if !reply.OK {
			return mcp.NewToolResultError(reply.Error), nil
		}
	case <-time.After(30 * time.Second):
		return mcp.NewToolResultError("put_open_timeout"), nil
	case <-ctx.Done():
		return mcp.NewToolResultError(ctx.Err().Error()), nil
	}

	buf := make([]byte, fileChunkSize)
	var seq int64
	for {
		n, rerr := f.Read(buf)
		eof := rerr == io.EOF
		if n > 0 || eof {
			if err := c.Send(&protocol.FileChunk{
				MsgID: id, Seq: seq, Data: append([]byte(nil), buf[:n]...), EOF: eof,
			}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			seq++
		}
		if eof {
			break
		}
		if rerr != nil {
			return mcp.NewToolResultError(rerr.Error()), nil
		}
	}

	select {
	case reply := <-ch:
		if !reply.OK {
			return mcp.NewToolResultError(reply.Error), nil
		}
		return finalResult(reply, start), nil
	case <-time.After(60 * time.Second):
		return mcp.NewToolResultError("final_reply_timeout"), nil
	case <-ctx.Done():
		return mcp.NewToolResultError(ctx.Err().Error()), nil
	}
}

func download(ctx context.Context, c *Conn, node, remotePath, localPath string, overwrite bool) (*mcp.CallToolResult, error) {
	lp, err := expandLocal(localPath)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if _, err := os.Stat(lp); err == nil && !overwrite {
		return mcp.NewToolResultError("destination_exists"), nil
	}

	id := NewMsgID()
	replyCh := c.rpc.Register(id)
	streamCh := c.rpc.RegisterStream(id)
	defer c.rpc.Unregister(id)
	start := time.Now()
	if err := c.Send(&protocol.FileGetOpen{MsgID: id, Target: node, Path: remotePath}); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	// Metadata reply.
	select {
	case reply := <-replyCh:
		if !reply.OK {
			return mcp.NewToolResultError(reply.Error), nil
		}
	case <-time.After(30 * time.Second):
		return mcp.NewToolResultError("get_open_timeout"), nil
	case <-ctx.Done():
		return mcp.NewToolResultError(ctx.Err().Error()), nil
	}

	tmp := lp + ".tether-tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	h := sha256.New()
	deadline := time.After(10 * time.Minute)
	for {
		select {
		case m, ok := <-streamCh:
			if !ok {
				out.Close()
				os.Remove(tmp)
				return mcp.NewToolResultError("stream_closed"), nil
			}
			switch v := m.(type) {
			case *protocol.FileChunk:
				if len(v.Data) > 0 {
					if _, err := out.Write(v.Data); err != nil {
						out.Close()
						os.Remove(tmp)
						return mcp.NewToolResultError(err.Error()), nil
					}
					h.Write(v.Data)
				}
				if v.EOF {
					out.Sync()
					out.Close()
					if err := os.Rename(tmp, lp); err != nil {
						os.Remove(tmp)
						return mcp.NewToolResultError(err.Error()), nil
					}
					fi, _ := os.Stat(lp)
					return finalResult(&protocol.Reply{Data: map[string]any{
						"bytes": fi.Size(), "sha256": hex.EncodeToString(h.Sum(nil)),
					}}, start), nil
				}
			case *protocol.FileAbort:
				out.Close()
				os.Remove(tmp)
				return mcp.NewToolResultError(v.Error), nil
			}
		case <-deadline:
			out.Close()
			os.Remove(tmp)
			return mcp.NewToolResultError("download_timeout"), nil
		case <-ctx.Done():
			out.Close()
			os.Remove(tmp)
			return mcp.NewToolResultError(ctx.Err().Error()), nil
		}
	}
}

func sameNodeCopy(ctx context.Context, c *Conn, node, fromPath, toPath string, overwrite bool) (*mcp.CallToolResult, error) {
	id := NewMsgID()
	ch := c.rpc.Register(id)
	defer c.rpc.Unregister(id)
	start := time.Now()
	if err := c.Send(&protocol.FileLocalCopy{
		MsgID: id, Target: node, FromPath: fromPath, ToPath: toPath, Overwrite: overwrite,
	}); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	select {
	case reply := <-ch:
		if !reply.OK {
			return mcp.NewToolResultError(reply.Error), nil
		}
		return finalResult(reply, start), nil
	case <-time.After(60 * time.Second):
		return mcp.NewToolResultError("local_copy_timeout"), nil
	case <-ctx.Done():
		return mcp.NewToolResultError(ctx.Err().Error()), nil
	}
}

func relay(ctx context.Context, c *Conn, fromNode, fromPath, toNode, toPath string, overwrite bool) (*mcp.CallToolResult, error) {
	id := NewMsgID()
	ch := c.rpc.Register(id)
	defer c.rpc.Unregister(id)
	start := time.Now()
	if err := c.Send(&protocol.FileRelay{
		MsgID: id, FromNode: fromNode, FromPath: fromPath,
		ToNode: toNode, ToPath: toPath, Overwrite: overwrite,
	}); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	select {
	case reply := <-ch:
		if !reply.OK {
			return mcp.NewToolResultError(reply.Error), nil
		}
		return finalResult(reply, start), nil
	case <-time.After(10 * time.Minute):
		return mcp.NewToolResultError("relay_timeout"), nil
	case <-ctx.Done():
		return mcp.NewToolResultError(ctx.Err().Error()), nil
	}
}

func finalResult(reply *protocol.Reply, start time.Time) *mcp.CallToolResult {
	out := map[string]any{
		"ok":          true,
		"duration_ms": time.Since(start).Milliseconds(),
	}
	if v, ok := reply.Data["bytes"]; ok {
		out["bytes"] = v
	}
	if v, ok := reply.Data["sha256"]; ok {
		out["sha256"] = v
	}
	b, _ := json.Marshal(out)
	return mcp.NewToolResultText(string(b))
}
