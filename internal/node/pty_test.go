package node

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/assert"
)

func TestExecPTYTtyDetected(t *testing.T) {
	send := &captureSender{msgs: make(chan protocol.Message, 16)}
	h := NewProcessHandler(t.TempDir(), 50)
	h.Handle(context.Background(), send, &protocol.Exec{
		MsgID: "e-tty",
		Cmd:   []string{"sh", "-c", "if [ -t 0 ]; then echo TTY; else echo PIPE; fi"},
		TTY:   true,
	})

	var out []byte
	deadline := time.After(3 * time.Second)
	for {
		select {
		case m := <-send.msgs:
			switch v := m.(type) {
			case *protocol.ExecOutput:
				out = append(out, v.Data...)
			case *protocol.ExecExit:
				assert.Contains(t, strings.ToUpper(string(out)), "TTY")
				return
			}
		case <-deadline:
			t.Fatal("timeout")
		}
	}
}
