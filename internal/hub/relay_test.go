package hub

import (
	"testing"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/require"
)

// TestReplyInterceptorDecodes verifies the interceptor pushes a decoded
// Reply to its channel and ignores non-Reply frames.
func TestReplyInterceptorDecodes(t *testing.T) {
	i := &replyInterceptor{ch: make(chan *protocol.Reply, 1)}
	raw, _ := protocol.Encode(&protocol.Reply{MsgID: "x", OK: true})
	require.NoError(t, i.SendRaw(raw))
	select {
	case r := <-i.ch:
		require.True(t, r.OK)
		require.Equal(t, "x", r.MsgID)
	default:
		t.Fatal("no reply received")
	}

	// Non-reply frame: still no error, no panic, no value.
	raw2, _ := protocol.Encode(&protocol.FileChunk{MsgID: "y", Data: []byte("x"), EOF: true})
	require.NoError(t, i.SendRaw(raw2))
}

func TestChunkRewriterRewritesMsgID(t *testing.T) {
	dst := &fakeConn{}
	cr := &chunkRewriter{toConn: dst, toMsgID: "NEW"}

	in := &protocol.FileChunk{MsgID: "OLD", Seq: 1, Data: []byte("hi"), EOF: false}
	raw, _ := protocol.Encode(in)
	require.NoError(t, cr.SendRaw(raw))
	require.Len(t, dst.sent, 1)

	decoded, err := protocol.Decode(dst.sent[0])
	require.NoError(t, err)
	out := decoded.(*protocol.FileChunk)
	require.Equal(t, "NEW", out.MsgID)
	require.Equal(t, int64(1), out.Seq)
	require.Equal(t, []byte("hi"), out.Data)
}

func TestFinalDelivererRewritesAndInvokesOnDone(t *testing.T) {
	cli := &fakeConn{}
	var doneCalled bool
	fd := &finalDeliverer{client: cli, clientMsgID: "CLIENT-X", onDone: func() { doneCalled = true }}

	raw, _ := protocol.Encode(&protocol.Reply{MsgID: "INNER", OK: true, Data: map[string]any{"bytes": int64(10)}})
	require.NoError(t, fd.SendRaw(raw))
	require.Len(t, cli.sent, 1)
	require.True(t, doneCalled)

	decoded, _ := protocol.Decode(cli.sent[0])
	rep := decoded.(*protocol.Reply)
	require.Equal(t, "CLIENT-X", rep.MsgID)
	require.True(t, rep.OK)
}
