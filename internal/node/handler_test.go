package node

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
)

type captureSender struct {
	msgs chan protocol.Message
}

func (c *captureSender) Send(m protocol.Message) error {
	c.msgs <- m
	return nil
}

func awaitReply(t *testing.T, ch <-chan protocol.Message) *protocol.Reply {
	t.Helper()
	select {
	case m := <-ch:
		r, ok := m.(*protocol.Reply)
		if !ok {
			t.Fatalf("expected *protocol.Reply, got %T", m)
		}
		return r
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for reply")
		return nil
	}
}

func TestHandleExec(t *testing.T) {
	h := NewHandler()
	s := &captureSender{msgs: make(chan protocol.Message, 8)}
	h.Handle(context.Background(), s, &protocol.Exec{
		MsgID: "e1",
		Cmd:   []string{"sh", "-c", "echo hi; echo bad 1>&2; exit 2"},
	})
	r := awaitReply(t, s.msgs)
	if !r.OK {
		t.Fatalf("exec reply not OK: %+v", r)
	}
	if got, _ := r.Data["stdout"].(string); !strings.Contains(got, "hi") {
		t.Fatalf("stdout = %q, want it to contain hi", got)
	}
	if got, _ := r.Data["stderr"].(string); !strings.Contains(got, "bad") {
		t.Fatalf("stderr = %q, want it to contain bad", got)
	}
	if r.Data["exit_code"] != 2 {
		t.Fatalf("exit_code = %v, want 2", r.Data["exit_code"])
	}
}
