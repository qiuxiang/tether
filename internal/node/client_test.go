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

func TestClientOnConnectedFired(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "x"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	wsURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"

	called := make(chan struct{}, 1)
	cli := New(Config{
		HubURL:   wsURL,
		Token:    "x",
		Hostname: "h1",
		OnConnected: func(_ Sender) {
			select {
			case called <- struct{}{}:
			default:
			}
		},
		ReconnectMin: 10 * time.Millisecond,
		ReconnectMax: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go cli.Run(ctx)

	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("OnConnected not called")
	}
}

type echoHandler struct{}

func (echoHandler) Handle(ctx context.Context, send Sender, msg protocol.Message) {
	if req, ok := msg.(*protocol.Exec); ok {
		send.Send(&protocol.Reply{MsgID: req.MsgID, OK: true, Data: map[string]any{"echo": "list"}})
	}
}

// replyCapture implements hub.PeerConn for tests — decodes Reply frames
// and pushes them onto a channel.
type replyCapture struct {
	ch chan *protocol.Reply
}

func (r *replyCapture) SendRaw(raw []byte) error {
	msg, err := protocol.Decode(raw)
	if err != nil {
		return nil
	}
	if reply, ok := msg.(*protocol.Reply); ok {
		select {
		case r.ch <- reply:
		default:
		}
	}
	return nil
}
func (r *replyCapture) Close() {}

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

	msgID := "test-1"
	capture := &replyCapture{ch: make(chan *protocol.Reply, 1)}
	s.Router().Register(msgID, capture, false)
	defer s.Router().Unregister(msgID)

	req := &protocol.Exec{MsgID: msgID, Cmd: "echo hello"}
	raw, err := protocol.Encode(req)
	require.NoError(t, err)
	require.NoError(t, dev.Conn.SendRaw(raw))

	select {
	case r := <-capture.ch:
		assert.True(t, r.OK)
		assert.Equal(t, "list", r.Data["echo"])
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for reply")
	}
}
