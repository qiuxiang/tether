package node

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/require"
)

type capSender struct {
	mu   sync.Mutex
	msgs []protocol.Message
}

func (c *capSender) Send(m protocol.Message) error {
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()
	return nil
}

func (c *capSender) last() protocol.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.msgs[len(c.msgs)-1]
}

func TestUploadHappyPath(t *testing.T) {
	h := NewFileHandler()
	s := &capSender{}
	dir := t.TempDir()
	dst := filepath.Join(dir, "out.bin")
	payload := []byte("hello world")
	sum := sha256.Sum256(payload)
	sumHex := hex.EncodeToString(sum[:])

	h.Handle(s, &protocol.FilePutOpen{
		MsgID: "u1", Path: dst, Size: int64(len(payload)), SHA256: sumHex,
	})
	require.True(t, s.last().(*protocol.Reply).OK)

	h.Handle(s, &protocol.FileChunk{MsgID: "u1", Seq: 0, Data: payload, EOF: true})
	final := s.last().(*protocol.Reply)
	require.True(t, final.OK, "final reply error: %s", final.Error)

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestUploadHashMismatch(t *testing.T) {
	h := NewFileHandler()
	s := &capSender{}
	dst := filepath.Join(t.TempDir(), "out.bin")
	h.Handle(s, &protocol.FilePutOpen{MsgID: "u2", Path: dst, Size: 5, SHA256: "deadbeef"})
	require.True(t, s.last().(*protocol.Reply).OK)
	h.Handle(s, &protocol.FileChunk{MsgID: "u2", Data: []byte("hello"), EOF: true})
	final := s.last().(*protocol.Reply)
	require.False(t, final.OK)
	require.Equal(t, "hash_mismatch", final.Error)
	_, err := os.Stat(dst)
	require.True(t, os.IsNotExist(err), "destination must not exist on mismatch")
}

func TestUploadDestExists(t *testing.T) {
	h := NewFileHandler()
	s := &capSender{}
	dst := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(dst, []byte("old"), 0o644))

	h.Handle(s, &protocol.FilePutOpen{MsgID: "u3", Path: dst, Size: 5})
	final := s.last().(*protocol.Reply)
	require.False(t, final.OK)
	require.Equal(t, "destination_exists", final.Error)
}

func TestUploadOverwrite(t *testing.T) {
	h := NewFileHandler()
	s := &capSender{}
	dst := filepath.Join(t.TempDir(), "out.bin")
	require.NoError(t, os.WriteFile(dst, []byte("old"), 0o644))

	payload := []byte("new")
	sum := sha256.Sum256(payload)
	h.Handle(s, &protocol.FilePutOpen{MsgID: "u4", Path: dst, Size: int64(len(payload)),
		SHA256: hex.EncodeToString(sum[:]), Overwrite: true})
	require.True(t, s.last().(*protocol.Reply).OK)
	h.Handle(s, &protocol.FileChunk{MsgID: "u4", Data: payload, EOF: true})
	require.True(t, s.last().(*protocol.Reply).OK)

	got, _ := os.ReadFile(dst)
	require.Equal(t, payload, got)
}

func TestUploadAbortCleansTempFile(t *testing.T) {
	h := NewFileHandler()
	s := &capSender{}
	dst := filepath.Join(t.TempDir(), "out.bin")
	h.Handle(s, &protocol.FilePutOpen{MsgID: "u5", Path: dst, Size: 100})
	require.True(t, s.last().(*protocol.Reply).OK)

	h.Handle(s, &protocol.FileAbort{MsgID: "u5", Error: "cancelled"})
	matches, _ := filepath.Glob(dst + ".tether-tmp-*")
	require.Empty(t, matches, "temp file should be cleaned up")
}
