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

func TestHelloHandshakeDuplicateHostname(t *testing.T) {
	base, _ := startTestServer(t, "secret")
	c1 := dialDevice(t, base)
	defer c1.Close(websocket.StatusNormalClosure, "")

	hello, _ := protocol.Encode(&protocol.Hello{Hostname: "mac", Token: "secret"})
	c1.Write(context.Background(), websocket.MessageBinary, hello)
	time.Sleep(50 * time.Millisecond)

	c2 := dialDevice(t, base)
	c2.Write(context.Background(), websocket.MessageBinary, hello)
	_, _, err := c2.Read(context.Background())
	assert.Error(t, err)
}
