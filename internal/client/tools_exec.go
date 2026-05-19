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
			mcp.WithDescription("Run a command on a device, wait for it to exit, return merged pty output. If the MCP call is cancelled while the command is still running, returns success with timed_out=true and a process_id so the caller can re-attach or inspect via list_processes/capture_screen/kill_process. The command keeps running on the device in that case."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("cmd", mcp.Required(), mcp.Description("Shell command (passed to sh -c)")),
			mcp.WithString("cwd"),
			mcp.WithObject("env"),
			mcp.WithString("stdin"),
			mcp.WithString("description", mcp.Description("Free-form annotation so you can find this command later via list_processes when timed out.")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			cmd, _ := args["cmd"].(string)
			cwd, _ := args["cwd"].(string)
			stdin, _ := args["stdin"].(string)
			desc, _ := args["description"].(string)
			envMap := extractStringMap(args["env"])

			pid := NewMsgID()

			// 1) Start.
			startID := NewMsgID()
			startCh := c.rpc.Register(startID)
			if err := c.Send(&protocol.Start{
				MsgID: startID, Target: device, ProcessID: pid,
				Cmd: []string{"sh", "-c", cmd}, Cwd: cwd, Env: envMap,
				Description: desc,
			}); err != nil {
				c.rpc.Unregister(startID)
				return mcp.NewToolResultError(err.Error()), nil
			}
			select {
			case reply := <-startCh:
				c.rpc.Unregister(startID)
				if !reply.OK {
					return mcp.NewToolResultError(reply.Error), nil
				}
			case <-ctx.Done():
				c.rpc.Unregister(startID)
				return mcp.NewToolResultError(ctx.Err().Error()), nil
			}

			// 2) Optional initial stdin (fire-and-forget; node accepts after Start).
			if len(stdin) > 0 {
				_ = c.Send(&protocol.Stdin{Target: device, ProcessID: pid, Data: []byte(stdin)})
			}

			// 3) Attach. Reply channel for OK/error signal, stream channel for output.
			attachID := NewMsgID()
			attachReplyCh := c.rpc.Register(attachID)
			ch := c.rpc.RegisterStream(attachID)
			defer c.rpc.Unregister(attachID)
			if err := c.Send(&protocol.Attach{MsgID: attachID, Target: device, ProcessID: pid}); err != nil {
				return resultExec(nil, 0, pid, false, err.Error()), nil
			}

			// Wait for the initial Attach reply.
			select {
			case reply := <-attachReplyCh:
				if !reply.OK {
					return mcp.NewToolResultError(reply.Error), nil
				}
			case <-ctx.Done():
				_ = c.Send(&protocol.Detach{MsgID: attachID, Target: device, ProcessID: pid})
				return resultExec(nil, 0, pid, true, ""), nil
			}

			var output []byte
			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						// Stream closed without ProcessExit — treat as ended.
						return resultExec(output, 0, pid, false, ""), nil
					}
					switch v := msg.(type) {
					case *protocol.ProcessOutput:
						output = append(output, v.Data...)
					case *protocol.ProcessExit:
						return resultExec(output, v.Code, pid, false, ""), nil
					}
				case <-ctx.Done():
					_ = c.Send(&protocol.Detach{MsgID: attachID, Target: device, ProcessID: pid})
					return resultExec(output, 0, pid, true, ""), nil
				}
			}
		},
	)

	// start_process
	m.AddTool(
		mcp.NewTool("start_process",
			mcp.WithDescription("Start a long-running background process. Returns process_id."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("cmd", mcp.Required()),
			mcp.WithString("cwd"),
			mcp.WithObject("env"),
			mcp.WithString("description", mcp.Description("Free-form annotation for later identification via list_processes.")),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			cmdStr, _ := args["cmd"].(string)
			cwd, _ := args["cwd"].(string)
			desc, _ := args["description"].(string)
			envMap := extractStringMap(args["env"])
			pid := NewMsgID()
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.Start{
				MsgID: id, Target: device, ProcessID: pid,
				Cmd: []string{"sh", "-c", cmdStr}, Cwd: cwd, Env: envMap,
				Description: desc,
			}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			select {
			case reply := <-ch:
				if !reply.OK {
					return mcp.NewToolResultError(reply.Error), nil
				}
				b, _ := json.Marshal(map[string]any{"process_id": pid})
				return mcp.NewToolResultText(string(b)), nil
			case <-ctx.Done():
				return mcp.NewToolResultError(ctx.Err().Error()), nil
			}
		},
	)

	// list_processes — when device == "", fan out across all devices on the client side
	m.AddTool(
		mcp.NewTool("list_processes",
			mcp.WithDescription("List processes on a device (or all)."),
			mcp.WithString("device"),
			mcp.WithNumber("limit"),
			mcp.WithString("status_filter"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			limit := 50
			if l, ok := args["limit"].(float64); ok {
				limit = int(l)
			}
			filter, _ := args["status_filter"].(string)

			if device != "" {
				id := NewMsgID()
				ch := c.rpc.Register(id)
				defer c.rpc.Unregister(id)
				if err := c.Send(&protocol.List{MsgID: id, Target: device, Limit: limit, StatusFilter: filter}); err != nil {
					return mcp.NewToolResultError(err.Error()), nil
				}
				select {
				case reply := <-ch:
					if !reply.OK {
						return mcp.NewToolResultError(reply.Error), nil
					}
					b, _ := json.Marshal(reply.Data)
					return mcp.NewToolResultText(string(b)), nil
				case <-time.After(defaultRPCTimeout):
					return mcp.NewToolResultError("timeout"), nil
				case <-ctx.Done():
					return mcp.NewToolResultError(ctx.Err().Error()), nil
				}
			}

			// No device specified: fan out via ListDevices then per-device List.
			devs, err := fetchDevices(ctx, c)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			all := []any{}
			for _, host := range devs {
				id := NewMsgID()
				ch := c.rpc.Register(id)
				if err := c.Send(&protocol.List{MsgID: id, Target: host, Limit: limit, StatusFilter: filter}); err != nil {
					c.rpc.Unregister(id)
					continue
				}
				select {
				case reply := <-ch:
					c.rpc.Unregister(id)
					if !reply.OK {
						continue
					}
					if procs, ok := reply.Data["processes"].([]any); ok {
						for _, p := range procs {
							if pm, ok := p.(map[string]any); ok {
								pm["device"] = host
							}
							all = append(all, p)
						}
					}
				case <-time.After(defaultRPCTimeout):
					c.rpc.Unregister(id)
				}
			}
			b, _ := json.Marshal(all)
			return mcp.NewToolResultText(string(b)), nil
		},
	)

	// capture_screen
	m.AddTool(
		mcp.NewTool("capture_screen",
			mcp.WithDescription("Return the rendered terminal screen of a process (ANSI sequences resolved, colors stripped). Tmux-style line ranges. For full historical bytes beyond scrollback, use list_processes + file_transfer on log_path."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("process_id", mcp.Required()),
			mcp.WithNumber("start_line"),
			mcp.WithNumber("end_line"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			pid, _ := args["process_id"].(string)
			var startLine *int
			if v, ok := args["start_line"].(float64); ok {
				n := int(v)
				startLine = &n
			}
			var endLine *int
			if v, ok := args["end_line"].(float64); ok {
				n := int(v)
				endLine = &n
			}
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.CaptureScreen{MsgID: id, Target: device, ProcessID: pid, StartLine: startLine, EndLine: endLine}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			select {
			case reply := <-ch:
				if !reply.OK {
					return mcp.NewToolResultError(reply.Error), nil
				}
				b, _ := json.Marshal(reply.Data)
				return mcp.NewToolResultText(string(b)), nil
			case <-time.After(defaultRPCTimeout):
				return mcp.NewToolResultError("timeout"), nil
			case <-ctx.Done():
				return mcp.NewToolResultError(ctx.Err().Error()), nil
			}
		},
	)

	// send_stdin — fire and forget
	m.AddTool(
		mcp.NewTool("send_stdin",
			mcp.WithDescription("Send stdin bytes to a process."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("process_id", mcp.Required()),
			mcp.WithString("data", mcp.Required()),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			pid, _ := args["process_id"].(string)
			data, _ := args["data"].(string)
			if err := c.Send(&protocol.Stdin{Target: device, ProcessID: pid, Data: []byte(data)}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return mcp.NewToolResultText(`{"ok":true}`), nil
		},
	)

	// kill_process
	m.AddTool(
		mcp.NewTool("kill_process",
			mcp.WithDescription("Terminate a process."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("process_id", mcp.Required()),
			mcp.WithString("signal"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			pid, _ := args["process_id"].(string)
			sig, _ := args["signal"].(string)
			if sig == "" {
				sig = "TERM"
			}
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.Kill{MsgID: id, Target: device, ProcessID: pid, Signal: sig}); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			select {
			case reply := <-ch:
				if !reply.OK {
					return mcp.NewToolResultError(reply.Error), nil
				}
				return mcp.NewToolResultText(`{"ok":true}`), nil
			case <-time.After(defaultRPCTimeout):
				return mcp.NewToolResultError("timeout"), nil
			case <-ctx.Done():
				return mcp.NewToolResultError(ctx.Err().Error()), nil
			}
		},
	)
}

func resultExec(output []byte, code int, processID string, timedOut bool, errStr string) *mcp.CallToolResult {
	payload := map[string]any{
		"output":     string(output),
		"exit_code":  code,
		"process_id": processID,
		"timed_out":  timedOut,
	}
	if errStr != "" {
		payload["error"] = errStr
	}
	b, _ := json.Marshal(payload)
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

func fetchDevices(ctx context.Context, c *Conn) ([]string, error) {
	id := NewMsgID()
	ch := c.rpc.Register(id)
	defer c.rpc.Unregister(id)
	if err := c.Send(&protocol.ListDevices{MsgID: id}); err != nil {
		return nil, err
	}
	select {
	case reply := <-ch:
		if !reply.OK {
			return nil, errors.New(reply.Error)
		}
		var out []string
		if list, ok := reply.Data["devices"].([]any); ok {
			for _, d := range list {
				if dm, ok := d.(map[string]any); ok {
					if h, ok := dm["hostname"].(string); ok {
						out = append(out, h)
					}
				}
			}
		}
		return out, nil
	case <-time.After(defaultRPCTimeout):
		return nil, errors.New("timeout")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
