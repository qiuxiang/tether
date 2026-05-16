package main

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/client"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Node
	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host"})
	nc.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
	go nc.Run(ctx)
	require.Eventually(t, func() bool {
		_, ok := s.Registry().Get("e2e-host")
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	// Client (uses internal/client directly without running the stdio MCP server)
	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	c := client.NewConn(client.Config{HubURL: cliURL, Token: "secret"})
	go c.Run(ctx)
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, c.WaitReady(cctx))
	ccancel()

	// Send an Exec, collect output, expect "hello".
	id := client.NewMsgID()
	ch := c.RPC().RegisterStream(id)
	require.NoError(t, c.Send(&protocol.Exec{
		MsgID: id, Target: "e2e-host",
		Cmd:       []string{"sh", "-c", "echo hello"},
		TimeoutMs: 5000,
	}))
	var stdout []byte
	deadline := time.After(3 * time.Second)
	for {
		select {
		case m, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed before ExecExit; stdout=%q", stdout)
			}
			switch v := m.(type) {
			case *protocol.ExecOutput:
				stdout = append(stdout, v.Data...)
			case *protocol.ExecExit:
				assert.Equal(t, 0, v.Code)
				assert.Contains(t, string(stdout), "hello")
				return
			}
		case <-deadline:
			t.Fatal("exec timed out")
		}
	}
}
