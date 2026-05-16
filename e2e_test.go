package main

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/hub"
	"github.com/qiuxiang/tether/internal/node"
	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2EExec(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	wsURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli := node.New(node.Config{HubURL: wsURL, Token: "secret", Hostname: "e2e-host"})
	cli.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
	go cli.Run(ctx)

	require.Eventually(t, func() bool {
		_, ok := s.Registry().Get("e2e-host")
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	stdout, stderr, code, timedOut, err := s.CallExecForTest(ctx, "e2e-host", &protocol.Exec{Cmd: []string{"sh", "-c", "echo hello"}, TimeoutMs: 10000}, 10*time.Second)
	require.NoError(t, err)
	assert.False(t, timedOut)
	assert.Equal(t, 0, code)
	assert.Contains(t, string(stdout), "hello")
	_ = stderr
}
