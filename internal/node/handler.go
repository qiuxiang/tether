package node

import (
	"context"

	"github.com/qiuxiang/tether/internal/protocol"
)

type NodeHandler struct {
	fileHandler    *FileHandler
	editHandler    *EditHandler
	forwardHandler *ForwardHandler
}

func NewHandler() *NodeHandler {
	return &NodeHandler{
		fileHandler:    NewFileHandler(),
		editHandler:    NewEditHandler(),
		forwardHandler: NewForwardHandler(),
	}
}

func (h *NodeHandler) Handle(ctx context.Context, send Sender, msg protocol.Message) {
	switch m := msg.(type) {
	case *protocol.Exec:
		go h.handleExec(send, m)
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
func (h *NodeHandler) ForwardHandler() *ForwardHandler { return h.forwardHandler }

func (h *NodeHandler) handleExec(send Sender, m *protocol.Exec) {
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

// Shutdown closes all forward listeners and streams. Idempotent.
func (h *NodeHandler) Shutdown() {
	h.forwardHandler.Shutdown()
}
