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
	r.Deliver(&protocol.ExecOutput{MsgID: "s1", Stream: "stdout", Data: []byte("x")})
	r.Deliver(&protocol.ExecExit{MsgID: "s1", Code: 0})
	a := <-ch
	require.IsType(t, &protocol.ExecOutput{}, a)
	b := <-ch
	require.IsType(t, &protocol.ExecExit{}, b)
	_, ok := <-ch
	require.False(t, ok, "channel should be closed after ExecExit")
}
