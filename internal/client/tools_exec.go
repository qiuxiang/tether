package client

import (
	"context"
	"encoding/json"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/qiuxiang/tether/internal/protocol"
)

const defaultRPCTimeout = 30 * time.Second

func registerExecTools(m *server.MCPServer, c *Conn) {
	m.AddTool(
		mcp.NewTool("list_devices",
			mcp.WithDescription("List all registered devices."),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.ListDevices{MsgID: id}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			select {
			case reply := <-ch:
				if !reply.OK {
					return mcp.NewToolResultError(reply.Error), nil
				}
				b, _ := json.Marshal(reply.Data["devices"])
				return mcp.NewToolResultText(string(b)), nil
			case <-time.After(defaultRPCTimeout):
				return mcp.NewToolResultError("timeout"), nil
			case <-ctx.Done():
				return mcp.NewToolResultError(ctx.Err().Error()), nil
			}
		},
	)

	m.AddTool(
		mcp.NewTool("exec",
			mcp.WithDescription("Run an executable on a device as a plain subprocess, wait for it to exit, and return its output. The process is spawned directly from name+args — no shell is involved, so pipes, globs, redirections, `&&` etc. are not interpreted (use the bash tool for those). If the command does not exit within `timeout` seconds (default 30), the device kills its process group and returns timed_out=true with whatever output was captured. Returns {stdout, stderr, exit_code, timed_out, truncated}. For long-running or interactive work, run tmux through this tool."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("name", mcp.Required(), mcp.Description("Executable name (resolved via the device's PATH) or absolute path")),
			mcp.WithArray("args", mcp.Description("Argument list passed to the executable verbatim"), mcp.WithStringItems()),
			mcp.WithString("cwd"),
			mcp.WithObject("env"),
			mcp.WithNumber("timeout", mcp.Description("Seconds to wait before the device kills the command. Default 30.")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			name, _ := args["name"].(string)
			argv := append([]string{name}, extractStringSlice(args["args"])...)
			return execRPC(ctx, c, args, argv)
		},
	)

	m.AddTool(
		mcp.NewTool("bash",
			mcp.WithDescription("Run a shell script on a device via bash (`bash -c script`), wait for it to exit, and return its output. Use this for pipes, globs, redirections, `&&` and other shell syntax. Requires bash on the device's PATH (on Windows install Git Bash or MSYS and add it to PATH). Timeout/output semantics are identical to the exec tool. Returns {stdout, stderr, exit_code, timed_out, truncated}."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("script", mcp.Required(), mcp.Description("Shell script passed to bash -c")),
			mcp.WithString("cwd"),
			mcp.WithObject("env"),
			mcp.WithNumber("timeout", mcp.Description("Seconds to wait before the device kills the command. Default 30.")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			script, _ := args["script"].(string)
			return execRPC(ctx, c, args, []string{"bash", "-c", script})
		},
	)
}

// execRPC sends an Exec with the given argv, pulling device/cwd/env/timeout
// from the tool arguments, and waits for the node's Reply.
func execRPC(ctx context.Context, c *Conn, args map[string]any, argv []string) (*mcp.CallToolResult, error) {
	device, _ := args["device"].(string)
	cwd, _ := args["cwd"].(string)
	envMap := extractStringMap(args["env"])

	timeoutSecs := 30
	if t, ok := args["timeout"].(float64); ok && t > 0 {
		timeoutSecs = int(t)
	}

	id := NewMsgID()
	ch := c.rpc.Register(id)
	defer c.rpc.Unregister(id)
	if err := c.Send(&protocol.Exec{
		MsgID: id, Target: device,
		Args: argv, Cwd: cwd, Env: envMap,
		Timeout: timeoutSecs,
	}); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	// Client-side safety net: wait a bit longer than the node's own
	// timeout so a node that never replies still surfaces an error.
	safety := time.Duration(timeoutSecs)*time.Second + 15*time.Second
	select {
	case reply := <-ch:
		if !reply.OK {
			return mcp.NewToolResultError(reply.Error), nil
		}
		b, _ := json.Marshal(reply.Data)
		return mcp.NewToolResultText(string(b)), nil
	case <-time.After(safety):
		return mcp.NewToolResultError("timeout"), nil
	case <-ctx.Done():
		return mcp.NewToolResultError(ctx.Err().Error()), nil
	}
}

// extractStringSlice pulls a []string out of an MCP array argument, which
// arrives as []any with string elements. Non-string elements are skipped.
func extractStringSlice(v any) []string {
	var out []string
	if e, ok := v.([]any); ok {
		for _, vv := range e {
			if vs, ok := vv.(string); ok {
				out = append(out, vs)
			}
		}
	}
	return out
}

func extractStringMap(v any) map[string]string {
	out := map[string]string{}
	if e, ok := v.(map[string]any); ok {
		for k, vv := range e {
			if vs, ok := vv.(string); ok {
				out[k] = vs
			}
		}
	}
	return out
}
