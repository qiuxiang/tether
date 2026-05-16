package node

import (
	"context"
	"sync"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
)

type ProcessHandler struct {
	registry   *ProcessRegistry
	logDir     string
	mu         sync.Mutex
	execMu     sync.Mutex
	execCancel map[string]context.CancelFunc
}

func NewProcessHandler(logDir string, cap int) *ProcessHandler {
	return &ProcessHandler{
		registry:   NewProcessRegistry(cap),
		logDir:     logDir,
		execCancel: make(map[string]context.CancelFunc),
	}
}

func (h *ProcessHandler) Handle(ctx context.Context, send Sender, msg protocol.Message) {
	switch m := msg.(type) {
	case *protocol.Start:
		h.handleStart(send, m)
	case *protocol.Kill:
		h.handleKill(send, m)
	case *protocol.Stdin:
		h.handleStdin(m)
	case *protocol.GetOutput:
		h.handleGetOutput(send, m)
	case *protocol.List:
		h.handleList(send, m)
	case *protocol.Exec:
		go h.handleExec(send, m)
	case *protocol.ExecCancel:
		h.handleExecCancel(m)
	}
}

func (h *ProcessHandler) handleStart(send Sender, m *protocol.Start) {
	p := &Process{ID: m.ProcessID, Name: m.Name, Cmd: m.Cmd}
	err := p.Start(context.Background(), h.logDir, m.Env, m.Cwd, m.TTY, func(code int) {
		send.Send(&protocol.Event{Kind: "exit", ProcessID: m.ProcessID, Code: code})
	})
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	h.registry.Add(p)
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{"process_id": m.ProcessID}})
}

func (h *ProcessHandler) handleKill(send Sender, m *protocol.Kill) {
	p, ok := h.registry.Get(m.ProcessID)
	if !ok {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "not found"})
		return
	}
	if err := p.Kill(m.Signal); err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true})
}

func (h *ProcessHandler) handleStdin(m *protocol.Stdin) {
	if p, ok := h.registry.Get(m.ProcessID); ok {
		_ = p.WriteStdin(m.Data)
	}
}

func (h *ProcessHandler) handleGetOutput(send Sender, m *protocol.GetOutput) {
	p, ok := h.registry.Get(m.ProcessID)
	if !ok {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "not found"})
		return
	}
	data, next, eof, err := p.ReadOutput(m.Offset, m.Length)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"data": data, "next_offset": next, "eof": eof,
	}})
}

func (h *ProcessHandler) handleExec(send Sender, m *protocol.Exec) {
	ctx, cancel := context.WithCancel(context.Background())
	if m.TimeoutMs > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, time.Duration(m.TimeoutMs)*time.Millisecond)
		defer stop()
	}
	h.execMu.Lock()
	h.execCancel[m.MsgID] = cancel
	h.execMu.Unlock()
	defer func() {
		h.execMu.Lock()
		delete(h.execCancel, m.MsgID)
		h.execMu.Unlock()
		cancel()
	}()

	code, err := runExecStream(ctx, m, send)
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	send.Send(&protocol.ExecExit{MsgID: m.MsgID, Code: code, Error: errStr})
}

func (h *ProcessHandler) handleExecCancel(m *protocol.ExecCancel) {
	h.execMu.Lock()
	if c, ok := h.execCancel[m.MsgID]; ok {
		c()
	}
	h.execMu.Unlock()
}

// Shutdown sends SIGTERM to all running process groups. Idempotent.
func (h *ProcessHandler) Shutdown() {
	for _, p := range h.registry.List("running", 0) {
		p.mu.Lock()
		pid := p.Pid
		p.mu.Unlock()
		if pid > 0 {
			killGroup(pid)
		}
	}
}

func (h *ProcessHandler) handleList(send Sender, m *protocol.List) {
	limit := m.Limit
	if limit == 0 {
		limit = 50
	}
	// Use ListSnapshots so that Status, LastActiveAt, and ExitCode are all
	// captured under p.mu inside the registry call.  No locking is needed here.
	list := h.registry.ListSnapshots(m.StatusFilter, limit)
	items := make([]map[string]any, 0, len(list))
	for _, snap := range list {
		entry := map[string]any{
			"process_id":     snap.ID,
			"name":           snap.Name,
			"cmd":            snap.Cmd,
			"status":         snap.Status,
			"started_at":     snap.StartedAt.Unix(),
			"last_active_at": snap.LastActiveAt.Unix(),
		}
		if snap.ExitCode != nil {
			entry["exit_code"] = *snap.ExitCode
		}
		items = append(items, entry)
	}
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{"processes": items}})
}
