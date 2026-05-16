package node

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/hub"
	"github.com/stretchr/testify/assert"
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
