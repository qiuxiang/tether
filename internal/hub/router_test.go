package hub

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeConn struct {
	sent   [][]byte
	closed bool
}

func (f *fakeConn) SendRaw(raw []byte) error { f.sent = append(f.sent, raw); return nil }
func (f *fakeConn) Close()                   { f.closed = true }

func TestRouterOneShotClient(t *testing.T) {
	r := NewRouter()
	c := &fakeConn{}
	r.Register("m1", c, false)
	require.True(t, r.ForwardToClient("m1", []byte("hello")))
	require.Len(t, c.sent, 1)
	assert.False(t, r.ForwardToClient("m1", []byte("again")))
}

func TestRouterStickyClient(t *testing.T) {
	r := NewRouter()
	c := &fakeConn{}
	r.Register("m2", c, true)
	require.True(t, r.ForwardToClient("m2", []byte("a")))
	require.True(t, r.ForwardToClient("m2", []byte("b")))
	require.Len(t, c.sent, 2)

	r.Unregister("m2")
	assert.False(t, r.ForwardToClient("m2", []byte("c")))
}

func TestRouterMissingMsgID(t *testing.T) {
	r := NewRouter()
	assert.False(t, r.ForwardToClient("nope", []byte("x")))
	assert.False(t, r.ForwardToNode("nope", []byte("x")))
}

func TestRouterNodeSide(t *testing.T) {
	r := NewRouter()
	cliC := &fakeConn{}
	nodeC := &fakeConn{}
	r.Register("m1", cliC, true)
	r.RegisterNode("m1", nodeC)

	require.True(t, r.ForwardToNode("m1", []byte("chunk")))
	require.Len(t, nodeC.sent, 1)
	require.True(t, r.ForwardToClient("m1", []byte("reply")))
	require.Len(t, cliC.sent, 1)
}
