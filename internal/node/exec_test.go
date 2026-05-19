package node

import (
	"context"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type captureSender struct {
	msgs chan protocol.Message
}

func (c *captureSender) Send(m protocol.Message) error {
	c.msgs <- m
	return nil
}

// awaitReplySkipping waits for the next *protocol.Reply, skipping over any
// non-Reply messages (e.g. Event frames from process exit). Useful when a
// fast-exiting process can race the reply onto the channel.
func awaitReplySkipping(t *testing.T, ch <-chan protocol.Message) *protocol.Reply {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case m := <-ch:
			if r, ok := m.(*protocol.Reply); ok {
				return r
			}
			// skip non-Reply frames (e.g. Event, ProcessOutput, ProcessExit)
		case <-deadline:
			t.Fatalf("timeout waiting for Reply")
			return nil
		}
	}
}

// TestStartAttachStreamsOutputAndExit verifies that a process started via
// protocol.Start and then subscribed with protocol.Attach delivers
// ProcessOutput frames and a terminal ProcessExit frame.
func TestStartAttachStreamsOutputAndExit(t *testing.T) {
	send := &captureSender{msgs: make(chan protocol.Message, 32)}
	h := NewProcessHandler(t.TempDir(), 50)

	// Start the process.
	h.Handle(context.Background(), send, &protocol.Start{
		MsgID:     "s1",
		ProcessID: "p-exec",
		Cmd:       []string{"sh", "-c", "echo hi; echo bye 1>&2; exit 3"},
	})

	// Consume the Reply from Start (may have an Event{exit} before or after).
	startReply := awaitReplySkipping(t, send.msgs)
	require.True(t, startReply.OK, "Start failed: %v", startReply.Error)

	// Attach to the running (or just-exited) process.
	go h.Handle(context.Background(), send, &protocol.Attach{
		MsgID:     "a1",
		ProcessID: "p-exec",
	})

	// Consume the Reply from Attach (skip any interleaved Event frames).
	attachReply := awaitReplySkipping(t, send.msgs)
	require.True(t, attachReply.OK, "Attach failed: %v", attachReply.Error)

	var sawOutput bool
	var exit *protocol.ProcessExit
	deadline := time.After(5 * time.Second)
	for exit == nil {
		select {
		case m := <-send.msgs:
			switch v := m.(type) {
			case *protocol.ProcessOutput:
				if len(v.Data) > 0 {
					sawOutput = true
				}
			case *protocol.ProcessExit:
				exit = v
			}
		case <-deadline:
			t.Fatal("timeout waiting for ProcessExit")
		}
	}
	require.NotNil(t, exit)
	assert.Equal(t, 3, exit.Code)
	assert.True(t, sawOutput, "expected at least one ProcessOutput frame")
}
