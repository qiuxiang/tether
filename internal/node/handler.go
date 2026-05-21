package node

import (
	"context"
	"sync"

	"github.com/qiuxiang/tether/internal/protocol"
)

type ProcessHandler struct {
	registry       *ProcessRegistry
	logDir         string
	mu             sync.Mutex
	attachSubs     map[string]attachRec
	fileHandler    *FileHandler
	editHandler    *EditHandler
	forwardHandler *ForwardHandler
}

func NewProcessHandler(logDir string, cap int) *ProcessHandler {
	return &ProcessHandler{
		registry:       NewProcessRegistry(cap),
		logDir:         logDir,
		attachSubs:     make(map[string]attachRec),
		fileHandler:    NewFileHandler(),
		editHandler:    NewEditHandler(),
		forwardHandler: NewForwardHandler(),
	}
}

func (h *ProcessHandler) Handle(ctx context.Context, send Sender, msg protocol.Message) {
	switch m := msg.(type) {
	case *protocol.Exec:
		go h.handleExec(send, m)
	case *protocol.Start:
		h.handleStart(send, m)
	case *protocol.Kill:
		h.handleKill(send, m)
	case *protocol.Stdin:
		h.handleStdin(m)
	case *protocol.CaptureScreen:
		h.handleCaptureScreen(send, m)
	case *protocol.List:
		h.handleList(send, m)
	case *protocol.Attach:
		go h.handleAttach(send, m)
	case *protocol.Detach:
		h.handleDetach(m)
	case *protocol.FilePutOpen, *protocol.FileChunk, *protocol.FileAbort,
		*protocol.FileGetOpen, *protocol.FileLocalCopy:
		h.fileHandler.Handle(send, msg)
	case *protocol.ReadFileReq, *protocol.WriteFileReq, *protocol.EditFileReq:
		h.editHandler.Handle(send, msg)
	case *protocol.ForwardListen:
		h.forwardHandler.Listen(send, m)
	case *protocol.ForwardUnlisten:
		h.forwardHandler.Unlisten(send, m)
	case *protocol.ForwardDial:
		h.forwardHandler.Dial(send, m)
	case *protocol.ForwardData:
		h.forwardHandler.Data(send, m)
	case *protocol.ForwardClose:
		h.forwardHandler.Close(send, m)
	case *protocol.Event:
		if m.Kind == "device_online" {
			h.forwardHandler.OnDeviceOnline(m.Device, send)
		}
	case *protocol.Reply:
		h.forwardHandler.OnReply(send, m)
	}
}

// ForwardHandler returns the embedded forward handler so callers (e.g. the
// `tether join` CLI) can seed it with rules at startup.
func (h *ProcessHandler) ForwardHandler() *ForwardHandler { return h.forwardHandler }

func (h *ProcessHandler) handleExec(send Sender, m *protocol.Exec) {
	res, err := runExec(context.Background(), m)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"stdout":    res.Stdout,
		"stderr":    res.Stderr,
		"exit_code": res.ExitCode,
		"timed_out": res.TimedOut,
		"truncated": res.Truncated,
	}})
}

func (h *ProcessHandler) handleStart(send Sender, m *protocol.Start) {
	p := &Process{ID: m.ProcessID, Description: m.Description, Cmd: m.Cmd}
	err := p.Start(context.Background(), h.logDir, m.Env, m.Cwd, func(code int) {
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

func (h *ProcessHandler) handleCaptureScreen(send Sender, m *protocol.CaptureScreen) {
	p, ok := h.registry.Get(m.ProcessID)
	if !ok {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "process not found"})
		return
	}
	lines, row, col, total := p.CaptureScreen(m.StartLine, m.EndLine)
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"lines":       lines,
		"cursor":      map[string]any{"row": row, "col": col},
		"cols":        vtCols,
		"total_lines": total,
	}})
}

type attachRec struct {
	proc *Process
	sub  *busSub
}

func (h *ProcessHandler) handleAttach(send Sender, m *protocol.Attach) {
	p, ok := h.registry.Get(m.ProcessID)
	if !ok {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "process not found"})
		return
	}
	if p.bus == nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "process has no output stream"})
		return
	}
	sub := p.bus.Subscribe(m.FromOffset)
	h.registerAttach(m.MsgID, p, sub)
	defer h.unregisterAttach(m.MsgID)

	// Initial ok-reply so client knows the subscription is live and any first
	// ProcessOutput is genuine, not a routing artifact.
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true})

	offset := m.FromOffset
	if offset < 0 {
		offset = 0
	}
loop:
	for {
		select {
		case chunk := <-sub.Ch():
			send.Send(&protocol.ProcessOutput{MsgID: m.MsgID, Offset: offset, Data: chunk})
			offset += int64(len(chunk))
		case <-sub.Done():
			// Drain any chunks already buffered so output is not truncated when
			// the bus closes between two pty writes.
			for {
				select {
				case chunk := <-sub.Ch():
					send.Send(&protocol.ProcessOutput{MsgID: m.MsgID, Offset: offset, Data: chunk})
					offset += int64(len(chunk))
				default:
					break loop
				}
			}
		}
	}
	// Done signaled → either the bus closed (process exit) or this Attach was
	// detached. Send terminal ProcessExit so the client's stream is unblocked
	// either way; for Detach this is harmless since the client has already
	// stopped reading.
	code := 0
	p.mu.Lock()
	if p.ExitCode != nil {
		code = *p.ExitCode
	}
	p.mu.Unlock()
	send.Send(&protocol.ProcessExit{MsgID: m.MsgID, Code: code})
}

func (h *ProcessHandler) handleDetach(m *protocol.Detach) {
	h.mu.Lock()
	rec, ok := h.attachSubs[m.MsgID]
	if ok {
		delete(h.attachSubs, m.MsgID)
	}
	h.mu.Unlock()
	if ok && rec.proc != nil && rec.sub != nil {
		rec.proc.bus.Unsubscribe(rec.sub)
	}
}

func (h *ProcessHandler) registerAttach(msgID string, p *Process, sub *busSub) {
	h.mu.Lock()
	h.attachSubs[msgID] = attachRec{proc: p, sub: sub}
	h.mu.Unlock()
}

func (h *ProcessHandler) unregisterAttach(msgID string) {
	h.mu.Lock()
	delete(h.attachSubs, msgID)
	h.mu.Unlock()
}

// Shutdown sends SIGTERM to all running process groups and closes all
// forward listeners and streams. Idempotent.
func (h *ProcessHandler) Shutdown() {
	for _, p := range h.registry.List("running", 0) {
		p.mu.Lock()
		pid := p.Pid
		p.mu.Unlock()
		if pid > 0 {
			killGroup(pid)
		}
	}
	h.forwardHandler.Shutdown()
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
			"description":    snap.Description,
			"cmd":            snap.Cmd,
			"status":         snap.Status,
			"started_at":     snap.StartedAt.Unix(),
			"last_active_at": snap.LastActiveAt.Unix(),
			"log_path":       snap.LogPath,
		}
		if snap.ExitCode != nil {
			entry["exit_code"] = *snap.ExitCode
		}
		items = append(items, entry)
	}
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{"processes": items}})
}
