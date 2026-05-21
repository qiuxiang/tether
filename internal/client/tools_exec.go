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
			mcp.WithDescription("Run a command on a device as a plain subprocess (sh -c), wait for it to exit, and return its output. If the command does not exit within `timeout` seconds (default 30), the device kills its process group and returns timed_out=true with whatever output was captured. Returns {stdout, stderr, exit_code, timed_out, truncated}. For long-running or interactive work, run tmux through this tool."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("cmd", mcp.Required(), mcp.Description("Shell command (passed to sh -c)")),
			mcp.WithString("cwd"),
			mcp.WithObject("env"),
			mcp.WithNumber("timeout", mcp.Description("Seconds to wait before the device kills the command. Default 30.")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			cmd, _ := args["cmd"].(string)
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
				Cmd: []string{"sh", "-c", cmd}, Cwd: cwd, Env: envMap,
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
		},
	)
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
