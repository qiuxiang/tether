package node

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/require"
)

// sliceSender records every Send into a slice for assertion.
type sliceSender struct{ out []protocol.Message }

func (c *sliceSender) Send(m protocol.Message) error { c.out = append(c.out, m); return nil }

func lastEditReply(t *testing.T, s *sliceSender) *protocol.Reply {
	t.Helper()
	require.NotEmpty(t, s.out)
	r, ok := s.out[len(s.out)-1].(*protocol.Reply)
	require.True(t, ok, "last message was not a Reply: %T", s.out[len(s.out)-1])
	return r
}

func TestReadFileWholeFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(p, []byte("line1\nline2\nline3\n"), 0o644))

	h := NewEditHandler()
	s := &sliceSender{}
	h.Handle(s, &protocol.ReadFileReq{MsgID: "1", Path: p, Offset: 0, Limit: 100})

	r := lastEditReply(t, s)
	require.True(t, r.OK, "error: %s", r.Error)
	require.Equal(t, []string{"line1", "line2", "line3"}, toLines(r.Data["lines"]))
	require.Equal(t, 3, r.Data["total_lines"])
	require.Equal(t, false, r.Data["truncated"])
	want := sha256.Sum256([]byte("line1\nline2\nline3\n"))
	require.Equal(t, hex.EncodeToString(want[:]), r.Data["sha256"])
}

func TestReadFileOffsetLimit(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(p, []byte("a\nb\nc\nd\ne\n"), 0o644))
	h := NewEditHandler()
	s := &sliceSender{}
	h.Handle(s, &protocol.ReadFileReq{MsgID: "1", Path: p, Offset: 1, Limit: 2})
	r := lastEditReply(t, s)
	require.True(t, r.OK)
	require.Equal(t, []string{"b", "c"}, toLines(r.Data["lines"]))
	require.Equal(t, 5, r.Data["total_lines"])
	require.Equal(t, true, r.Data["truncated"])
}

func TestReadFileMissing(t *testing.T) {
	h := NewEditHandler()
	s := &sliceSender{}
	h.Handle(s, &protocol.ReadFileReq{MsgID: "1", Path: "/nonexistent/x"})
	r := lastEditReply(t, s)
	require.False(t, r.OK)
	require.Equal(t, "not_found", r.Error)
}

func TestReadFileTooLarge(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "big")
	require.NoError(t, os.WriteFile(p, make([]byte, editMaxBytes+1), 0o644))
	h := NewEditHandler()
	s := &sliceSender{}
	h.Handle(s, &protocol.ReadFileReq{MsgID: "1", Path: p})
	r := lastEditReply(t, s)
	require.False(t, r.OK)
	require.Equal(t, "too_large", r.Error)
}

func TestReadFileBinary(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "bin")
	require.NoError(t, os.WriteFile(p, []byte{0xff, 0xfe, 0x00, 0x7f}, 0o644))
	h := NewEditHandler()
	s := &sliceSender{}
	h.Handle(s, &protocol.ReadFileReq{MsgID: "1", Path: p})
	r := lastEditReply(t, s)
	require.True(t, r.OK)
	require.Equal(t, true, r.Data["binary"])
}

func toLines(v any) []string {
	if s, ok := v.([]string); ok {
		return s
	}
	a := v.([]any)
	out := make([]string, len(a))
	for i, x := range a {
		out[i] = x.(string)
	}
	return out
}

func TestWriteFileNew(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out.txt")
	h := NewEditHandler()
	s := &sliceSender{}
	h.Handle(s, &protocol.WriteFileReq{MsgID: "1", Path: p, Content: []byte("hello")})
	r := lastEditReply(t, s)
	require.True(t, r.OK, "err: %s", r.Error)
	got, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))
	entries, _ := os.ReadDir(dir)
	require.Len(t, entries, 1)
}

func TestWriteFileNoOverwrite(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out.txt")
	require.NoError(t, os.WriteFile(p, []byte("old"), 0o644))
	h := NewEditHandler()
	s := &sliceSender{}
	h.Handle(s, &protocol.WriteFileReq{MsgID: "1", Path: p, Content: []byte("new")})
	r := lastEditReply(t, s)
	require.False(t, r.OK)
	require.Equal(t, "exists", r.Error)
	got, _ := os.ReadFile(p)
	require.Equal(t, "old", string(got))
}

func TestWriteFileOverwrite(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out.txt")
	require.NoError(t, os.WriteFile(p, []byte("old"), 0o644))
	h := NewEditHandler()
	s := &sliceSender{}
	h.Handle(s, &protocol.WriteFileReq{MsgID: "1", Path: p, Content: []byte("new"), Overwrite: true})
	r := lastEditReply(t, s)
	require.True(t, r.OK)
	got, _ := os.ReadFile(p)
	require.Equal(t, "new", string(got))
}

func TestWriteFileMissingParent(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "sub", "out.txt")
	h := NewEditHandler()
	s := &sliceSender{}
	h.Handle(s, &protocol.WriteFileReq{MsgID: "1", Path: p, Content: []byte("x")})
	r := lastEditReply(t, s)
	require.False(t, r.OK)
}

func TestWriteFileCreateDirs(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "sub", "out.txt")
	h := NewEditHandler()
	s := &sliceSender{}
	h.Handle(s, &protocol.WriteFileReq{MsgID: "1", Path: p, Content: []byte("x"), CreateDirs: true})
	r := lastEditReply(t, s)
	require.True(t, r.OK, "err: %s", r.Error)
	got, _ := os.ReadFile(p)
	require.Equal(t, "x", string(got))
}

func TestWriteFileTooLarge(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "big")
	h := NewEditHandler()
	s := &sliceSender{}
	h.Handle(s, &protocol.WriteFileReq{MsgID: "1", Path: p, Content: make([]byte, editMaxBytes+1)})
	r := lastEditReply(t, s)
	require.False(t, r.OK)
	require.Equal(t, "too_large", r.Error)
}
