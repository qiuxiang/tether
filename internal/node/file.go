package node

import (
	"crypto/sha256"
	"encoding/hex"
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
	mu        sync.Mutex // guards f, h, written during concurrent chunk delivery
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
	st.mu.Lock()
	if len(m.Data) > 0 {
		if _, err := st.f.Write(m.Data); err != nil {
			st.mu.Unlock()
			h.failUpload(send, m.MsgID, st, "disk_full")
			return
		}
		st.h.Write(m.Data)
		st.written += int64(len(m.Data))
		if h.MaxFileSize > 0 && st.written > h.MaxFileSize {
			st.mu.Unlock()
			h.failUpload(send, m.MsgID, st, "size_limit_exceeded")
			return
		}
	}
	eof := m.EOF
	st.mu.Unlock()
	if eof {
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
	st.mu.Lock()
	syncErr := st.f.Sync()
	var written int64
	var gotSHA string
	if syncErr == nil {
		st.f.Close()
		gotSHA = hex.EncodeToString(st.h.Sum(nil))
		written = st.written
	}
	st.mu.Unlock()
	if syncErr != nil {
		h.failUpload(send, msgID, st, syncErr.Error())
		return
	}
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
		"bytes": written, "sha256": gotSHA,
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

const fileChunkSize = 256 * 1024

func (h *FileHandler) handleGetOpen(send Sender, m *protocol.FileGetOpen) {
	path, err := expandPath(m.Path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "path_not_found"})
			return
		}
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	if h.MaxFileSize > 0 && fi.Size() > h.MaxFileSize {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "size_limit_exceeded"})
		return
	}
	f, err := os.Open(path)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}

	// Send metadata reply first (receiver computes its own sha).
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"size": fi.Size(),
		"mode": uint32(fi.Mode().Perm()),
	}})

	// Register cancel chan for handleAbort.
	cancelCh := make(chan struct{})
	h.mu.Lock()
	h.downloads[m.MsgID] = &downloadState{cancel: cancelCh}
	h.mu.Unlock()

	go func() {
		defer f.Close()
		defer func() {
			h.mu.Lock()
			delete(h.downloads, m.MsgID)
			h.mu.Unlock()
		}()
		buf := make([]byte, fileChunkSize)
		var seq int64
		for {
			select {
			case <-cancelCh:
				send.Send(&protocol.FileAbort{MsgID: m.MsgID, Error: "cancelled"})
				return
			default:
			}
			n, rerr := f.Read(buf)
			if n > 0 {
				eof := rerr == io.EOF
				err := send.Send(&protocol.FileChunk{
					MsgID: m.MsgID, Seq: seq, Data: append([]byte(nil), buf[:n]...), EOF: eof,
				})
				if err != nil {
					return
				}
				seq++
				if eof {
					return
				}
			}
			if rerr == io.EOF {
				// Zero-byte file: still need to send EOF frame.
				_ = send.Send(&protocol.FileChunk{MsgID: m.MsgID, Seq: seq, EOF: true})
				return
			}
			if rerr != nil {
				send.Send(&protocol.FileAbort{MsgID: m.MsgID, Error: rerr.Error()})
				return
			}
		}
	}()
}

func (h *FileHandler) handleLocalCopy(send Sender, m *protocol.FileLocalCopy) {
	from, err := expandPath(m.FromPath)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	to, err := expandPath(m.ToPath)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	if from == to {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "from equals to"})
		return
	}
	if _, err := os.Stat(to); err == nil && !m.Overwrite {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "destination_exists"})
		return
	}
	src, err := os.Open(from)
	if err != nil {
		if os.IsNotExist(err) {
			send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "path_not_found"})
			return
		}
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	defer src.Close()
	fi, err := src.Stat()
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	tmp := to + ".tether-tmp-" + m.MsgID
	dst, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fi.Mode().Perm())
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(dst, hash), src)
	dst.Close()
	if err != nil {
		os.Remove(tmp)
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	if err := os.Rename(tmp, to); err != nil {
		os.Remove(tmp)
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"bytes": n, "sha256": hex.EncodeToString(hash.Sum(nil)),
	}})
}
