package client

import (
	"context"
	"encoding/json"
	"errors"
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
			mcp.WithDescription("Run a one-shot command on a device. Streams output, returns final stdout/stderr/exit_code."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("cmd", mcp.Required(), mcp.Description("Shell command (passed to sh -c)")),
			mcp.WithString("cwd"),
			mcp.WithObject("env"),
			mcp.WithString("stdin"),
			mcp.WithBoolean("tty"),
			mcp.WithNumber("timeout"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			cmd, _ := args["cmd"].(string)
			cwd, _ := args["cwd"].(string)
			stdin, _ := args["stdin"].(string)
			tty, _ := args["tty"].(bool)
			timeout := 60 * time.Second
			if t, ok := args["timeout"].(float64); ok {
				timeout = time.Duration(t) * time.Second
			}
			envMap := extractStringMap(args["env"])

			id := NewMsgID()
			ch := c.rpc.RegisterStream(id)
			defer c.rpc.Unregister(id)
			msg := &protocol.Exec{
				MsgID: id, Target: device,
				Cmd: []string{"sh", "-c", cmd}, Cwd: cwd, Env: envMap,
				Stdin: []byte(stdin), TTY: tty, TimeoutMs: timeout.Milliseconds(),
			}
			if err := c.Send(msg); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}

			var stdoutB, stderrB []byte
			deadline := time.After(timeout + 5*time.Second)
			for {
				select {
				case m, ok := <-ch:
					if !ok {
						return resultExec(stdoutB, stderrB, 0, false), nil
					}
					switch v := m.(type) {
					case *protocol.ExecOutput:
						if v.Stream == "stderr" {
							stderrB = append(stderrB, v.Data...)
						} else {
							stdoutB = append(stdoutB, v.Data...)
						}
					case *protocol.ExecExit:
						return resultExec(stdoutB, stderrB, v.Code, false), nil
					}
				case <-deadline:
					_ = c.Send(&protocol.ExecCancel{MsgID: id, Target: device})
					return resultExec(stdoutB, stderrB, -1, true), nil
				case <-ctx.Done():
					_ = c.Send(&protocol.ExecCancel{MsgID: id, Target: device})
					return mcp.NewToolResultError(errors.New("cancelled").Error()), nil
				}
			}
		},
	)
}

func resultExec(stdoutB, stderrB []byte, code int, timedOut bool) *mcp.CallToolResult {
	b, _ := json.Marshal(map[string]any{
		"stdout": string(stdoutB), "stderr": string(stderrB),
		"exit_code": code, "timed_out": timedOut,
	})
	return mcp.NewToolResultText(string(b))
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
