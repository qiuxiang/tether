package node

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/hub"
	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeConnectsAndRegisters(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	wsURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := New(Config{HubURL: wsURL, Token: "secret", Hostname: "testhost"})
	go cli.Run(ctx)

	assert.Eventually(t, func() bool {
		_, ok := s.Registry().Get("testhost")
		return ok
	}, 2*time.Second, 20*time.Millisecond)
}

func TestNodeReconnects(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	wsURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := New(Config{HubURL: wsURL, Token: "secret", Hostname: "testhost", ReconnectMin: 50 * time.Millisecond})
	go cli.Run(ctx)

	// Wait until registered.
	assert.Eventually(t, func() bool {
		_, ok := s.Registry().Get("testhost")
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	// Restart the server (simulates network drop + recovery).
	ts.Close()
	ts2 := httptest.NewServer(s.Handler())
	defer ts2.Close()
	// Replace URL by rebuilding client — keep simple by reconfiguring to the new addr.
	cli.SetURL(strings.Replace(ts2.URL, "http", "ws", 1) + "/device")

	assert.Eventually(t, func() bool {
		_, ok := s.Registry().Get("testhost")
		return ok
	}, 3*time.Second, 50*time.Millisecond)
}

type echoHandler struct{}

func (echoHandler) Handle(ctx context.Context, send Sender, msg protocol.Message) {
	if list, ok := msg.(*protocol.List); ok {
		send.Send(&protocol.Reply{MsgID: list.MsgID, OK: true, Data: map[string]any{"echo": "list"}})
	}
}

func TestRequestReplyRoundtrip(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	wsURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := New(Config{HubURL: wsURL, Token: "secret", Hostname: "h1"})
	cli.SetHandler(echoHandler{})
	go cli.Run(ctx)

	// Wait registration.
	var dev *hub.Device
	assert.Eventually(t, func() bool {
		d, ok := s.Registry().Get("h1")
		if ok {
			dev = d
		}
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	// Send a List request via the deviceSession.
	msgID := "test-1"
	reply := s.Router().Register(msgID)
	defer s.Router().Unregister(msgID)
	require.NoError(t, dev.Conn.Send(&protocol.List{MsgID: msgID}))

	select {
	case r := <-reply:
		assert.True(t, r.OK)
		assert.Equal(t, "list", r.Data["echo"])
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for reply")
	}
}
