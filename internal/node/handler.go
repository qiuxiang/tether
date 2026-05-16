package node

import (
	"context"
	"sync"

	"github.com/qiuxiang/tether/internal/protocol"
)

type processHandler struct {
	registry *ProcessRegistry
	logDir   string
	mu       sync.Mutex
}

func NewProcessHandler(logDir string, cap int) *processHandler {
	return &processHandler{registry: NewProcessRegistry(cap), logDir: logDir}
}

func (h *processHandler) Handle(ctx context.Context, send Sender, msg protocol.Message) {
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
	}
}

func (h *processHandler) handleStart(send Sender, m *protocol.Start) {
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

func (h *processHandler) handleKill(send Sender, m *protocol.Kill) {
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

func (h *processHandler) handleStdin(m *protocol.Stdin) {
	if p, ok := h.registry.Get(m.ProcessID); ok {
		_ = p.WriteStdin(m.Data)
	}
}

func (h *processHandler) handleGetOutput(send Sender, m *protocol.GetOutput) {
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

func (h *processHandler) handleList(send Sender, m *protocol.List) {
	limit := m.Limit
	if limit == 0 {
		limit = 50
	}
	list := h.registry.List(m.StatusFilter, limit)
	items := make([]map[string]any, 0, len(list))
	for _, p := range list {
		entry := map[string]any{
			"process_id":     p.ID,
			"name":           p.Name,
			"cmd":            p.Cmd,
			"status":         p.Status,
			"started_at":     p.StartedAt.Unix(),
			"last_active_at": p.LastActiveAt.Unix(),
		}
		if p.ExitCode != nil {
			entry["exit_code"] = *p.ExitCode
		}
		items = append(items, entry)
	}
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{"processes": items}})
}
