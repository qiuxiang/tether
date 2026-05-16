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

	// start_process
	m.AddTool(
		mcp.NewTool("start_process",
			mcp.WithDescription("Start a long-running background process. Returns process_id."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("cmd", mcp.Required()),
			mcp.WithString("cwd"),
			mcp.WithObject("env"),
			mcp.WithBoolean("tty"),
			mcp.WithString("name"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			cmdStr, _ := args["cmd"].(string)
			cwd, _ := args["cwd"].(string)
			tty, _ := args["tty"].(bool)
			name, _ := args["name"].(string)
			envMap := extractStringMap(args["env"])
			pid := NewMsgID()
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.Start{
				MsgID: id, Target: device, ProcessID: pid,
				Cmd: []string{"sh", "-c", cmdStr}, Cwd: cwd, Env: envMap,
				TTY: tty, Name: name,
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
			case <-time.After(defaultRPCTimeout):
				return mcp.NewToolResultError("timeout"), nil
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

	// get_output
	m.AddTool(
		mcp.NewTool("get_output",
			mcp.WithDescription("Read log bytes from a process by offset."),
			mcp.WithString("device", mcp.Required()),
			mcp.WithString("process_id", mcp.Required()),
			mcp.WithNumber("offset"),
			mcp.WithNumber("length"),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := req.GetArguments()
			device, _ := args["device"].(string)
			pid, _ := args["process_id"].(string)
			var offset int64
			if o, ok := args["offset"].(float64); ok {
				offset = int64(o)
			}
			length := 65536
			if l, ok := args["length"].(float64); ok {
				length = int(l)
			}
			id := NewMsgID()
			ch := c.rpc.Register(id)
			defer c.rpc.Unregister(id)
			if err := c.Send(&protocol.GetOutput{MsgID: id, Target: device, ProcessID: pid, Offset: offset, Length: length}); err != nil {
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
