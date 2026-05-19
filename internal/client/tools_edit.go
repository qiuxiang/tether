package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/qiuxiang/tether/internal/protocol"
)

const editRPCTimeout = 30 * time.Second

func registerEditTools(m *server.MCPServer, c *Conn) {
	m.AddTool(
		mcp.NewTool("read_file",
			mcp.WithDescription("Read a slice of a file on a remote node. Path must be 'node:/abs/path' or 'node:~/path'. Returns {lines, total_lines, truncated, sha256, binary}. Max file size 10 MB — use file_transfer for larger files."),
			mcp.WithString("path", mcp.Required()),
			mcp.WithNumber("offset"),
			mcp.WithNumber("limit"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			pathStr, _ := args["path"].(string)
			offset, _ := args["offset"].(float64)
			limit, _ := args["limit"].(float64)
			node, path, err := mustNodePath(pathStr)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.ReadFileReq{
				MsgID: id, Target: node, Path: path,
				Offset: int(offset), Limit: int(limit),
			}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return awaitReply(ctx, ch)
		},
	)

	m.AddTool(
		mcp.NewTool("write_file",
			mcp.WithDescription("Atomically write a file on a remote node. Path must be 'node:/abs/path' or 'node:~/path'. Default refuses to overwrite. Max 10 MB."),
			mcp.WithString("path", mcp.Required()),
			mcp.WithString("content", mcp.Required()),
			mcp.WithBoolean("overwrite"),
			mcp.WithBoolean("create_dirs"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			pathStr, _ := args["path"].(string)
			content, _ := args["content"].(string)
			overwrite, _ := args["overwrite"].(bool)
			createDirs, _ := args["create_dirs"].(bool)
			node, path, err := mustNodePath(pathStr)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.WriteFileReq{
				MsgID: id, Target: node, Path: path,
				Content: []byte(content), Overwrite: overwrite, CreateDirs: createDirs,
			}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return awaitReply(ctx, ch)
		},
	)

	m.AddTool(
		mcp.NewTool("edit_file",
			mcp.WithDescription("Replace old_string with new_string in a file on a remote node. Path must be 'node:/abs/path'. With replace_all=false (default), old_string must occur exactly once. Max 10 MB."),
			mcp.WithString("path", mcp.Required()),
			mcp.WithString("old_string", mcp.Required()),
			mcp.WithString("new_string", mcp.Required()),
			mcp.WithBoolean("replace_all"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			pathStr, _ := args["path"].(string)
			oldS, _ := args["old_string"].(string)
			newS, _ := args["new_string"].(string)
			replaceAll, _ := args["replace_all"].(bool)
			node, path, err := mustNodePath(pathStr)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.EditFileReq{
				MsgID: id, Target: node, Path: path,
				OldString: []byte(oldS), NewString: []byte(newS), ReplaceAll: replaceAll,
			}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return awaitReply(ctx, ch)
		},
	)
}

// mustNodePath enforces the node: prefix; local paths are rejected.
// Reuses parsePath from tools_file.go.
func mustNodePath(s string) (node, path string, err error) {
	ps, perr := parsePath(s)
	if perr != nil {
		return "", "", perr
	}
	if ps.Node == "" {
		return "", "", fmt.Errorf("path must be in 'node:/abs/path' form; got %q", s)
	}
	return ps.Node, ps.Path, nil
}

func awaitReply(ctx context.Context, ch chan *protocol.Reply) (*mcp.CallToolResult, error) {
	select {
	case r := <-ch:
		if !r.OK {
			return mcp.NewToolResultError(r.Error), nil
		}
		b, _ := json.Marshal(r.Data)
		return mcp.NewToolResultText(string(b)), nil
	case <-ctx.Done():
		return mcp.NewToolResultError(ctx.Err().Error()), nil
	case <-time.After(editRPCTimeout):
		return mcp.NewToolResultError("rpc_timeout"), nil
	}
}
