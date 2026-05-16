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

func TestRouterOneShot(t *testing.T) {
	r := NewRouter()
	c := &fakeConn{}
	r.Register("m1", c, false)
	ok := r.Forward("m1", []byte("hello"))
	require.True(t, ok)
	require.Len(t, c.sent, 1)

	// Second Forward should miss — route removed.
	ok = r.Forward("m1", []byte("again"))
	assert.False(t, ok)
}

func TestRouterSticky(t *testing.T) {
	r := NewRouter()
	c := &fakeConn{}
	r.Register("m2", c, true)
	require.True(t, r.Forward("m2", []byte("a")))
	require.True(t, r.Forward("m2", []byte("b")))
	require.Len(t, c.sent, 2)

	r.Unregister("m2")
	assert.False(t, r.Forward("m2", []byte("c")))
}

func TestRouterMissingMsgID(t *testing.T) {
	r := NewRouter()
	assert.False(t, r.Forward("nope", []byte("x")))
}
