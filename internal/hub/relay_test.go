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

// TestRelaySourceInterceptorBuffersChunksUntilDestinationReady covers the
// race that previously dropped FileChunks: a fast source pushes the metadata
// Reply and chunks back-to-back before the hub has learned the destination's
// msg_id. Frames that arrive in that window must be buffered and replayed
// once SetDestination is called.
func TestRelaySourceInterceptorBuffersChunksUntilDestinationReady(t *testing.T) {
	dst := &fakeConn{}
	meta := make(chan *protocol.Reply, 1)
	itcp := &relaySourceInterceptor{metaReady: meta, toConn: dst}

	// 1. Metadata Reply arrives first.
	metaRaw, _ := protocol.Encode(&protocol.Reply{MsgID: "GET", OK: true, Data: map[string]any{"size": int64(5)}})
	require.NoError(t, itcp.SendRaw(metaRaw))
	select {
	case r := <-meta:
		require.True(t, r.OK)
	default:
		t.Fatal("metadata Reply not pushed to metaReady")
	}

	// 2. Chunks arrive BEFORE SetDestination — they must be buffered, not dropped,
	//    and not forwarded yet.
	ch1Raw, _ := protocol.Encode(&protocol.FileChunk{MsgID: "GET", Seq: 0, Data: []byte("hel"), EOF: false})
	ch2Raw, _ := protocol.Encode(&protocol.FileChunk{MsgID: "GET", Seq: 1, Data: []byte("lo"), EOF: true})
	require.NoError(t, itcp.SendRaw(ch1Raw))
	require.NoError(t, itcp.SendRaw(ch2Raw))
	require.Empty(t, dst.sent, "chunks must not be forwarded before SetDestination")

	// 3. SetDestination drains the buffer and rewrites msg_ids to PUT.
	itcp.SetDestination("PUT")
	require.Len(t, dst.sent, 2, "buffered chunks must be replayed after SetDestination")

	c0, err := protocol.Decode(dst.sent[0])
	require.NoError(t, err)
	got0 := c0.(*protocol.FileChunk)
	require.Equal(t, "PUT", got0.MsgID)
	require.Equal(t, int64(0), got0.Seq)
	require.Equal(t, []byte("hel"), got0.Data)
	require.False(t, got0.EOF)

	c1, err := protocol.Decode(dst.sent[1])
	require.NoError(t, err)
	got1 := c1.(*protocol.FileChunk)
	require.Equal(t, "PUT", got1.MsgID)
	require.Equal(t, []byte("lo"), got1.Data)
	require.True(t, got1.EOF)

	// 4. Subsequent chunks (after SetDestination) are forwarded inline, not buffered.
	ch3Raw, _ := protocol.Encode(&protocol.FileChunk{MsgID: "GET", Seq: 2, Data: []byte("!"), EOF: true})
	require.NoError(t, itcp.SendRaw(ch3Raw))
	require.Len(t, dst.sent, 3)
	c2, _ := protocol.Decode(dst.sent[2])
	require.Equal(t, "PUT", c2.(*protocol.FileChunk).MsgID)
}

// TestRelaySourceInterceptorAbortBeforeDestinationReady ensures FileAbort
// frames that arrive in the pre-SetDestination window are also buffered and
// then forwarded with rewritten msg_id.
func TestRelaySourceInterceptorAbortBeforeDestinationReady(t *testing.T) {
	dst := &fakeConn{}
	itcp := &relaySourceInterceptor{metaReady: make(chan *protocol.Reply, 1), toConn: dst}

	abortRaw, _ := protocol.Encode(&protocol.FileAbort{MsgID: "GET", Error: "boom"})
	require.NoError(t, itcp.SendRaw(abortRaw))
	require.Empty(t, dst.sent)

	itcp.SetDestination("PUT")
	require.Len(t, dst.sent, 1)
	decoded, _ := protocol.Decode(dst.sent[0])
	a := decoded.(*protocol.FileAbort)
	require.Equal(t, "PUT", a.MsgID)
	require.Equal(t, "boom", a.Error)
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
