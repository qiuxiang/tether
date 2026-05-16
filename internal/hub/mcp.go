//go:build ignore

package hub

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/qiuxiang/tether/internal/protocol"
)

const defaultRPCTimeout = 30 * time.Second

func newMsgID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (s *Server) call(ctx context.Context, hostname string, makeReq func(msgID string) protocol.Message, timeout time.Duration) (*protocol.Reply, error) {
	d, ok := s.registry.Get(hostname)
	if !ok {
		return nil, fmt.Errorf("device_offline: %s", hostname)
	}
	id := newMsgID()
	ch := s.router.Register(id)
	defer s.router.Unregister(id)

	if err := d.Conn.Send(makeReq(id)); err != nil {
		return nil, err
	}
	if timeout == 0 {
		timeout = defaultRPCTimeout
	}
	select {
	case r := <-ch:
		if !r.OK {
			return r, errors.New(r.Error)
		}
		return r, nil
	case <-time.After(timeout):
		return nil, errors.New("rpc timeout")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// callExec runs an exec on the device, collecting streamed output into one result.
func (s *Server) callExec(ctx context.Context, hostname string, req *protocol.Exec, timeout time.Duration) (stdout, stderr []byte, exitCode int, timedOut bool, err error) {
	d, ok := s.registry.Get(hostname)
	if !ok {
		return nil, nil, 0, false, fmt.Errorf("device_offline: %s", hostname)
	}
	req.MsgID = newMsgID()
	ch := s.router.RegisterStream(req.MsgID)
	defer s.router.Unregister(req.MsgID)

	if err := d.Conn.Send(req); err != nil {
		return nil, nil, 0, false, err
	}
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	deadline := time.After(timeout)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				return stdout, stderr, exitCode, false, nil
			}
			switch v := m.(type) {
			case *protocol.ExecOutput:
				if v.Stream == "stderr" {
					stderr = append(stderr, v.Data...)
				} else {
					stdout = append(stdout, v.Data...)
				}
			case *protocol.ExecExit:
				return stdout, stderr, v.Code, false, nil
			}
		case <-deadline:
			// Best-effort cancel.
			d.Conn.Send(&protocol.ExecCancel{MsgID: req.MsgID})
			return stdout, stderr, -1, true, nil
		case <-ctx.Done():
			d.Conn.Send(&protocol.ExecCancel{MsgID: req.MsgID})
			return stdout, stderr, -1, false, ctx.Err()
		}
	}
}

// CallExecForTest is exported for end-to-end tests to bypass the MCP HTTP layer.
func (s *Server) CallExecForTest(ctx context.Context, host string, req *protocol.Exec, timeout time.Duration) ([]byte, []byte, int, bool, error) {
	return s.callExec(ctx, host, req, timeout)
}

func (s *Server) mcpHandler() http.Handler {
	m := server.NewMCPServer("tether", "0.1.0")

	m.AddTool(
		mcp.NewTool("list_devices",
			mcp.WithDescription("List all registered devices."),
		),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			list := s.registry.List()
			items := make([]map[string]any, 0, len(list))
			for _, d := range list {
				items = append(items, map[string]any{
					"hostname":      d.Hostname,
					"os":            d.OS,
					"arch":          d.Arch,
					"agent_version": d.AgentVersion,
					"online":        d.Conn != nil,
					"last_seen":     d.LastSeen.Unix(),
				})
			}
			b, _ := json.Marshal(items)
			return mcp.NewToolResultText(string(b)), nil
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
			envMap := extractEnv(args["env"])
			execReq := &protocol.Exec{Cmd: []string{"sh", "-c", cmd}, Cwd: cwd, Env: envMap, Stdin: []byte(stdin), TTY: tty, TimeoutMs: timeout.Milliseconds()}
			stdoutB, stderrB, code, timedOut, err := s.callExec(ctx, device, execReq, timeout)
			if err != nil && len(stdoutB) == 0 && len(stderrB) == 0 {
				return mcp.NewToolResultError(err.Error()), nil
			}
			result := map[string]any{
				"stdout": string(stdoutB), "stderr": string(stderrB),
				"exit_code": code, "timed_out": timedOut,
			}
			b, _ := json.Marshal(result)
			return mcp.NewToolResultText(string(b)), nil
		},
	)

	s.addProcessTools(m)

	return server.NewSSEServer(m, server.WithBasePath("/mcp"))
}

func (s *Server) addProcessTools(m *server.MCPServer) {
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
			envMap := extractEnv(args["env"])
			pid := newMsgID()
			_, err := s.call(ctx, device, func(id string) protocol.Message {
				return &protocol.Start{MsgID: id, ProcessID: pid, Cmd: []string{"sh", "-c", cmdStr}, Cwd: cwd, Env: envMap, TTY: tty, Name: name}
			}, defaultRPCTimeout)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(map[string]any{"process_id": pid})
			return mcp.NewToolResultText(string(b)), nil
		},
	)

	// list_processes
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
				reply, err := s.call(ctx, device, func(id string) protocol.Message {
					return &protocol.List{MsgID: id, Limit: limit, StatusFilter: filter}
				}, defaultRPCTimeout)
				if err != nil {
					return mcp.NewToolResultError(err.Error()), nil
				}
				b, _ := json.Marshal(reply.Data)
				return mcp.NewToolResultText(string(b)), nil
			}
			// Fan out across all devices.
			all := []any{}
			for _, d := range s.registry.List() {
				reply, err := s.call(ctx, d.Hostname, func(id string) protocol.Message {
					return &protocol.List{MsgID: id, Limit: limit, StatusFilter: filter}
				}, defaultRPCTimeout)
				if err != nil {
					continue
				}
				if procs, ok := reply.Data["processes"].([]any); ok {
					for _, p := range procs {
						if pm, ok := p.(map[any]any); ok {
							pm["device"] = d.Hostname
						}
						all = append(all, p)
					}
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
			reply, err := s.call(ctx, device, func(id string) protocol.Message {
				return &protocol.GetOutput{MsgID: id, ProcessID: pid, Offset: offset, Length: length}
			}, defaultRPCTimeout)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			b, _ := json.Marshal(reply.Data)
			return mcp.NewToolResultText(string(b)), nil
		},
	)

	// send_stdin
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
			d, ok := s.registry.Get(device)
			if !ok {
				return mcp.NewToolResultError(fmt.Sprintf("device_offline: %s", device)), nil
			}
			if err := d.Conn.Send(&protocol.Stdin{ProcessID: pid, Data: []byte(data)}); err != nil {
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
			_, err := s.call(ctx, device, func(id string) protocol.Message {
				return &protocol.Kill{MsgID: id, ProcessID: pid, Signal: sig}
			}, defaultRPCTimeout)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			return mcp.NewToolResultText(`{"ok":true}`), nil
		},
	)
}

func extractEnv(v any) map[string]string {
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
