package hub

import (
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/assert"
)

func TestRouterRoundtrip(t *testing.T) {
	r := NewRouter()
	ch := r.Register("abc")
	defer r.Unregister("abc")

	go func() {
		r.Deliver(&protocol.Reply{MsgID: "abc", OK: true, Data: map[string]any{"x": 1}})
	}()

	select {
	case msg := <-ch:
		assert.Equal(t, "abc", msg.MsgID)
		assert.True(t, msg.OK)
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestRouterUnknownMsgID(t *testing.T) {
	r := NewRouter()
	// Should not panic.
	r.Deliver(&protocol.Reply{MsgID: "nope"})
}
