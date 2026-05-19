package node

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
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

func (h *EditHandler) handleWrite(send Sender, m *protocol.WriteFileReq) {
	path, err := expandPath(m.Path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	if int64(len(m.Content)) > editMaxBytes {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "too_large"})
		return
	}
	bytes, sum, err := atomicWrite(path, m.Content, m.Overwrite, m.CreateDirs)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"bytes":  bytes,
		"sha256": sum,
	}})
}

// atomicWrite writes data to path via a same-directory temp file, fsyncs,
// then renames over the destination. Returns (bytes, sha256-hex, err).
// err.Error() is the spec error code when possible ("exists", "not_found",
// "permission_denied", "is_directory").
func atomicWrite(path string, data []byte, overwrite, createDirs bool) (int64, string, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.IsDir() {
			return 0, "", errors.New("is_directory")
		}
		if !overwrite {
			return 0, "", errors.New("exists")
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return 0, "", errors.New(classifyErr(err))
	}

	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return 0, "", errors.New(classifyErr(err))
		}
		if !createDirs {
			return 0, "", errors.New("not_found")
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return 0, "", errors.New(classifyErr(err))
		}
	}

	tmp, err := os.CreateTemp(dir, ".tether-edit-*.tmp")
	if err != nil {
		return 0, "", errors.New(classifyErr(err))
	}
	tmpPath := tmp.Name()
	cleanup := func() { os.Remove(tmpPath) }

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		cleanup()
		return 0, "", err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return 0, "", err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return 0, "", err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		cleanup()
		return 0, "", errors.New(classifyErr(err))
	}
	sum := sha256.Sum256(data)
	return int64(len(data)), hex.EncodeToString(sum[:]), nil
}

// handleEdit is a placeholder implemented in Task 4.
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
