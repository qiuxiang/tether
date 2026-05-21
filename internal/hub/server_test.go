package hub

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startTestServer(t *testing.T, token string) (string, *Server) {
	t.Helper()
	s := NewServer(Options{Token: token})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	url := strings.Replace(ts.URL, "http", "ws", 1)
	return url, s
}

func dialDevice(t *testing.T, base string) *websocket.Conn {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, base+"/device", nil)
	require.NoError(t, err)
	return c
}

func TestHelloHandshakeOK(t *testing.T) {
	base, s := startTestServer(t, "secret")
	c := dialDevice(t, base)
	defer c.Close(websocket.StatusNormalClosure, "")

	hello := &protocol.Hello{Hostname: "mac", OS: "darwin", Arch: "arm64", AgentVersion: "0.1.0", Token: "secret"}
	data, _ := protocol.Encode(hello)
	require.NoError(t, c.Write(context.Background(), websocket.MessageBinary, data))

	assert.Eventually(t, func() bool {
		_, ok := s.Registry().Get("mac")
		return ok
	}, time.Second, 10*time.Millisecond)
}

func TestHelloHandshakeBadToken(t *testing.T) {
	base, _ := startTestServer(t, "secret")
	c := dialDevice(t, base)

	hello := &protocol.Hello{Hostname: "mac", Token: "wrong"}
	data, _ := protocol.Encode(hello)
	c.Write(context.Background(), websocket.MessageBinary, data)

	// Server should close the connection.
	_, _, err := c.Read(context.Background())
	assert.Error(t, err)
}

func TestHelloHandshakeDuplicateHostnameTakeover(t *testing.T) {
	// A node that lost its network silently leaves a stale registration on
	// the hub (the dead TCP socket can take minutes to surface). The
	// reconnecting node must be able to take over its own hostname instead
	// of being rejected forever.
	base, s := startTestServer(t, "secret")
	c1 := dialDevice(t, base)
	defer c1.Close(websocket.StatusNormalClosure, "")

	hello, _ := protocol.Encode(&protocol.Hello{Hostname: "mac", Token: "secret"})
	c1.Write(context.Background(), websocket.MessageBinary, hello)
	assert.Eventually(t, func() bool {
		_, ok := s.Registry().Get("mac")
		return ok
	}, time.Second, 10*time.Millisecond)

	c2 := dialDevice(t, base)
	defer c2.Close(websocket.StatusNormalClosure, "")
	c2.Write(context.Background(), websocket.MessageBinary, hello)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, err := c1.Read(ctx)
	assert.Error(t, err, "stale session must be closed by takeover")

	// New session must remain registered.
	assert.Eventually(t, func() bool {
		_, ok := s.Registry().Get("mac")
		return ok
	}, time.Second, 10*time.Millisecond)
}

func TestClientHandshakeWithCompression(t *testing.T) {
	s := NewServer(Options{Token: "tk"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	wsURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	})
	require.NoError(t, err)
	defer c.Close(websocket.StatusNormalClosure, "")

	// Verify the server negotiated permessage-deflate by inspecting the
	// upgrade response header.
	require.Contains(t, resp.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate",
		"expected server to negotiate permessage-deflate")

	hello := &protocol.Hello{Hostname: "smoke", Token: "tk", Role: "client"}
	raw, _ := protocol.Encode(hello)
	require.NoError(t, c.Write(ctx, websocket.MessageBinary, raw))

	// Sanity round-trip: ListDevices → Reply
	raw, _ = protocol.Encode(&protocol.ListDevices{MsgID: "1"})
	require.NoError(t, c.Write(ctx, websocket.MessageBinary, raw))
	_, data, err := c.Read(ctx)
	require.NoError(t, err)
	msg, err := protocol.Decode(data)
	require.NoError(t, err)
	reply := msg.(*protocol.Reply)
	require.True(t, reply.OK)
}

func TestClientHandshake(t *testing.T) {
	s := NewServer(Options{Token: "tk"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	wsURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer c.Close(websocket.StatusNormalClosure, "")

	hello := &protocol.Hello{Hostname: "claude-host", Token: "tk", Role: "client"}
	raw, _ := protocol.Encode(hello)
	require.NoError(t, c.Write(ctx, websocket.MessageBinary, raw))

	// Send ListDevices, expect a Reply back.
	req := &protocol.ListDevices{MsgID: "1"}
	rraw, _ := protocol.Encode(req)
	require.NoError(t, c.Write(ctx, websocket.MessageBinary, rraw))

	_, data, err := c.Read(ctx)
	require.NoError(t, err)
	msg, err := protocol.Decode(data)
	require.NoError(t, err)
	reply, ok := msg.(*protocol.Reply)
	require.True(t, ok)
	require.Equal(t, "1", reply.MsgID)
	require.True(t, reply.OK)
}
