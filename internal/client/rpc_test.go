package client

import (
	"testing"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestRPCReply(t *testing.T) {
	r := NewRPC()
	ch := r.Register("m1")
	r.Deliver(&protocol.Reply{MsgID: "m1", OK: true})
	got := <-ch
	require.True(t, got.OK)
}

func TestRPCStream(t *testing.T) {
	r := NewRPC()
	ch := r.RegisterStream("s1")
	r.Deliver(&protocol.FileChunk{MsgID: "s1", Seq: 0, Data: []byte("x")})
	r.Deliver(&protocol.FileChunk{MsgID: "s1", Seq: 1, Data: []byte("y"), EOF: true})
	a := <-ch
	require.IsType(t, &protocol.FileChunk{}, a)
	b := <-ch
	require.IsType(t, &protocol.FileChunk{}, b)
	require.True(t, b.(*protocol.FileChunk).EOF, "second chunk should be EOF")
	_, ok := <-ch
	require.False(t, ok, "channel should be closed after EOF chunk")
}
