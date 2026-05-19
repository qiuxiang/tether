package client

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/hub"
	"github.com/qiuxiang/tether/internal/node"
	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/require"
)

func setupClusterWithClient(t *testing.T) (*Conn, *hub.Server, func()) {
	t.Helper()
	s := hub.NewServer(hub.Options{Token: "tk"})
	ts := httptest.NewServer(s.Handler())

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "tk", Hostname: "n1"})
	nc.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
	ctx, cancel := context.WithCancel(context.Background())
	go nc.Run(ctx)
	require.Eventually(t, func() bool {
		_, ok := s.Registry().Get("n1")
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	c := NewConn(Config{HubURL: cliURL, Token: "tk"})
	go c.Run(ctx)
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, c.WaitReady(cctx))
	ccancel()

	cleanup := func() { cancel(); ts.Close() }
	return c, s, cleanup
}

func TestListDevicesEndToEnd(t *testing.T) {
	c, _, cleanup := setupClusterWithClient(t)
	defer cleanup()

	id := NewMsgID()
	ch := c.rpc.Register(id)
	require.NoError(t, c.Send(&protocol.ListDevices{MsgID: id}))
	select {
	case reply := <-ch:
		require.True(t, reply.OK)
		b, _ := json.Marshal(reply.Data["devices"])
		require.Contains(t, string(b), "n1")
	case <-time.After(2 * time.Second):
		t.Fatal("no reply")
	}
}

func TestExecEndToEnd(t *testing.T) {
	c, _, cleanup := setupClusterWithClient(t)
	defer cleanup()

	pid := NewMsgID()

	// Step 1: Start the process.
	startID := NewMsgID()
	startCh := c.rpc.Register(startID)
	require.NoError(t, c.Send(&protocol.Start{
		MsgID: startID, Target: "n1", ProcessID: pid,
		Cmd: []string{"sh", "-c", "echo hello"},
	}))
	select {
	case r := <-startCh:
		require.True(t, r.OK, "Start failed: %s", r.Error)
	case <-time.After(3 * time.Second):
		t.Fatal("Start timed out")
	}
	c.rpc.Unregister(startID)

	// Step 2: Attach to receive output.
	attachID := NewMsgID()
	attachReplyCh := c.rpc.Register(attachID)
	streamCh := c.rpc.RegisterStream(attachID)
	defer c.rpc.Unregister(attachID)
	require.NoError(t, c.Send(&protocol.Attach{
		MsgID: attachID, Target: "n1", ProcessID: pid,
	}))
	select {
	case r := <-attachReplyCh:
		require.True(t, r.OK, "Attach failed: %s", r.Error)
	case <-time.After(3 * time.Second):
		t.Fatal("Attach reply timed out")
	}

	// Step 3: Collect output until ProcessExit.
	var output []byte
	deadline := time.After(3 * time.Second)
	for {
		select {
		case m, ok := <-streamCh:
			if !ok {
				t.Fatalf("channel closed before ProcessExit; output=%q", output)
			}
			switch v := m.(type) {
			case *protocol.ProcessOutput:
				output = append(output, v.Data...)
			case *protocol.ProcessExit:
				require.Equal(t, 0, v.Code)
				require.Contains(t, string(output), "hello")
				return
			}
		case <-deadline:
			t.Fatalf("exec timed out; output so far=%q", output)
		}
	}
}

func TestStartAndListProcesses(t *testing.T) {
	c, _, cleanup := setupClusterWithClient(t)
	defer cleanup()

	pid := NewMsgID()
	id := NewMsgID()
	ch := c.rpc.Register(id)
	require.NoError(t, c.Send(&protocol.Start{
		MsgID: id, Target: "n1", ProcessID: pid,
		Cmd: []string{"sh", "-c", "sleep 0.2"},
	}))
	select {
	case r := <-ch:
		require.True(t, r.OK)
	case <-time.After(2 * time.Second):
		t.Fatal("start timed out")
	}
	c.rpc.Unregister(id)

	id = NewMsgID()
	ch = c.rpc.Register(id)
	require.NoError(t, c.Send(&protocol.List{MsgID: id, Target: "n1"}))
	select {
	case r := <-ch:
		require.True(t, r.OK)
		b, _ := json.Marshal(r.Data)
		require.Contains(t, string(b), pid)
	case <-time.After(2 * time.Second):
		t.Fatal("list timed out")
	}
}
