package node

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/qiuxiang/tether/internal/protocol"
)

// editMaxBytes caps file size for read_file/write_file/edit_file to bound
// node memory. Files bigger than this must use file_transfer.
const editMaxBytes = 10 * 1024 * 1024 // 10 MiB

type EditHandler struct{}

func NewEditHandler() *EditHandler { return &EditHandler{} }

func (h *EditHandler) Handle(send Sender, msg protocol.Message) {
	switch m := msg.(type) {
	case *protocol.ReadFileReq:
		h.handleRead(send, m)
	case *protocol.WriteFileReq:
		h.handleWrite(send, m)
	case *protocol.EditFileReq:
		h.handleEdit(send, m)
	}
}

// handleWrite and handleEdit are placeholders implemented in Tasks 3 and 4.
func (h *EditHandler) handleWrite(send Sender, m *protocol.WriteFileReq) {
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "not_implemented"})
}

func (h *EditHandler) handleEdit(send Sender, m *protocol.EditFileReq) {
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "not_implemented"})
}

func (h *EditHandler) handleRead(send Sender, m *protocol.ReadFileReq) {
	path, err := expandPath(m.Path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	info, err := os.Lstat(path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: classifyErr(err)})
		return
	}
	if info.IsDir() {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "is_directory"})
		return
	}
	if info.Size() > editMaxBytes {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "too_large"})
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: classifyErr(err)})
		return
	}
	sum := sha256.Sum256(data)
	binary := !utf8.Valid(data)

	var allLines []string
	if len(data) > 0 {
		allLines = strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	}
	total := len(allLines)

	limit := m.Limit
	if limit <= 0 {
		limit = 2000
	}
	offset := m.Offset
	if offset < 0 {
		offset = 0
	}
	end := offset + limit
	if end > total {
		end = total
	}
	var window []string
	if offset < total {
		window = allLines[offset:end]
	}
	truncated := end < total

	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"lines":       window,
		"total_lines": total,
		"truncated":   truncated,
		"sha256":      hex.EncodeToString(sum[:]),
		"binary":      binary,
	}})
}

// classifyErr maps common os errors to the spec's error codes.
func classifyErr(err error) string {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "not_found"
	case errors.Is(err, fs.ErrPermission):
		return "permission_denied"
	default:
		return err.Error()
	}
}
