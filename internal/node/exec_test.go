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

func TestExecStreamsOutputAndExit(t *testing.T) {
	send := &captureSender{msgs: make(chan protocol.Message, 16)}
	h := NewProcessHandler(t.TempDir(), 50)
	h.Handle(context.Background(), send, &protocol.Exec{MsgID: "e1", Cmd: []string{"sh", "-c", "echo hi; echo bye 1>&2; exit 3"}})

	// PTY merges stdout+stderr into a single "stdout" stream.
	var sawStdout bool
	var exit *protocol.ExecExit
	deadline := time.After(3 * time.Second)
	for exit == nil {
		select {
		case m := <-send.msgs:
			switch v := m.(type) {
			case *protocol.ExecOutput:
				if v.Stream == "stdout" {
					sawStdout = true
				}
			case *protocol.ExecExit:
				exit = v
			}
		case <-deadline:
			t.Fatal("timeout")
		}
	}
	require.NotNil(t, exit)
	assert.Equal(t, 3, exit.Code)
	assert.True(t, sawStdout)
}
