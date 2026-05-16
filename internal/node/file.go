package node

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/qiuxiang/tether/internal/protocol"
)

// FileHandler manages in-flight uploads and downloads on a node.
type FileHandler struct {
	mu        sync.Mutex
	uploads   map[string]*uploadState
	downloads map[string]*downloadState

	MaxFileSize int64 // 0 = no limit
}

type uploadState struct {
	path      string
	tmpPath   string
	f         *os.File
	h         hash.Hash
	wantSHA   string
	written   int64
	overwrite bool
	finalMode os.FileMode
}

type downloadState struct {
	cancel chan struct{}
}

func NewFileHandler() *FileHandler {
	return &FileHandler{
		uploads:     make(map[string]*uploadState),
		downloads:   make(map[string]*downloadState),
		MaxFileSize: 5 << 30, // 5 GiB
	}
}

func (h *FileHandler) Handle(send Sender, msg protocol.Message) {
	switch m := msg.(type) {
	case *protocol.FilePutOpen:
		h.handlePutOpen(send, m)
	case *protocol.FileChunk:
		h.handleChunk(send, m)
	case *protocol.FileAbort:
		h.handleAbort(m)
	case *protocol.FileGetOpen:
		h.handleGetOpen(send, m)
	case *protocol.FileLocalCopy:
		h.handleLocalCopy(send, m)
	}
}

func expandPath(p string) (string, error) {
	if strings.HasPrefix(p, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		p = home + p[1:]
	}
	if !filepath.IsAbs(p) {
		return "", fmt.Errorf("path must be absolute: %s", p)
	}
	return p, nil
}

func (h *FileHandler) handlePutOpen(send Sender, m *protocol.FilePutOpen) {
	path, err := expandPath(m.Path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	if h.MaxFileSize > 0 && m.Size > h.MaxFileSize {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "size_limit_exceeded"})
		return
	}
	if _, err := os.Stat(path); err == nil && !m.Overwrite {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "destination_exists"})
		return
	}
	tmp := path + ".tether-tmp-" + m.MsgID
	mode := os.FileMode(0o644)
	if m.Mode != 0 {
		mode = os.FileMode(m.Mode)
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	st := &uploadState{
		path: path, tmpPath: tmp, f: f, h: sha256.New(),
		wantSHA: m.SHA256, overwrite: m.Overwrite, finalMode: mode,
	}
	h.mu.Lock()
	h.uploads[m.MsgID] = st
	h.mu.Unlock()
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true})
}

func (h *FileHandler) handleChunk(send Sender, m *protocol.FileChunk) {
	h.mu.Lock()
	st, ok := h.uploads[m.MsgID]
	h.mu.Unlock()
	if !ok {
		// No matching upload — drop. (Downloads receive chunks at the other side.)
		return
	}
	if len(m.Data) > 0 {
		if _, err := st.f.Write(m.Data); err != nil {
			h.failUpload(send, m.MsgID, st, "disk_full")
			return
		}
		st.h.Write(m.Data)
		st.written += int64(len(m.Data))
		if h.MaxFileSize > 0 && st.written > h.MaxFileSize {
			h.failUpload(send, m.MsgID, st, "size_limit_exceeded")
			return
		}
	}
	if m.EOF {
		h.finishUpload(send, m.MsgID, st)
	}
}

func (h *FileHandler) failUpload(send Sender, msgID string, st *uploadState, errStr string) {
	st.f.Close()
	os.Remove(st.tmpPath)
	h.mu.Lock()
	delete(h.uploads, msgID)
	h.mu.Unlock()
	send.Send(&protocol.Reply{MsgID: msgID, OK: false, Error: errStr})
}

func (h *FileHandler) finishUpload(send Sender, msgID string, st *uploadState) {
	if err := st.f.Sync(); err != nil {
		h.failUpload(send, msgID, st, err.Error())
		return
	}
	st.f.Close()
	gotSHA := hex.EncodeToString(st.h.Sum(nil))
	if st.wantSHA != "" && !strings.EqualFold(gotSHA, st.wantSHA) {
		os.Remove(st.tmpPath)
		h.mu.Lock()
		delete(h.uploads, msgID)
		h.mu.Unlock()
		send.Send(&protocol.Reply{MsgID: msgID, OK: false, Error: "hash_mismatch"})
		return
	}
	if err := os.Rename(st.tmpPath, st.path); err != nil {
		os.Remove(st.tmpPath)
		h.mu.Lock()
		delete(h.uploads, msgID)
		h.mu.Unlock()
		send.Send(&protocol.Reply{MsgID: msgID, OK: false, Error: err.Error()})
		return
	}
	h.mu.Lock()
	delete(h.uploads, msgID)
	h.mu.Unlock()
	send.Send(&protocol.Reply{MsgID: msgID, OK: true, Data: map[string]any{
		"bytes": st.written, "sha256": gotSHA,
	}})
}

func (h *FileHandler) handleAbort(m *protocol.FileAbort) {
	h.mu.Lock()
	if st, ok := h.uploads[m.MsgID]; ok {
		st.f.Close()
		os.Remove(st.tmpPath)
		delete(h.uploads, m.MsgID)
	}
	if ds, ok := h.downloads[m.MsgID]; ok {
		close(ds.cancel)
		delete(h.downloads, m.MsgID)
	}
	h.mu.Unlock()
}

// handleGetOpen and handleLocalCopy are added in P2-T3.
func (h *FileHandler) handleGetOpen(send Sender, m *protocol.FileGetOpen)     {}
func (h *FileHandler) handleLocalCopy(send Sender, m *protocol.FileLocalCopy) {}

// Silence unused-import errors until P2-T3 wires io/errors uses.
var _ = io.Copy
var _ = errors.New
