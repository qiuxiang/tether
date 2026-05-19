package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/client"
	"github.com/qiuxiang/tether/internal/forward"
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

func TestE2EFileTransfer(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host"})
	nc.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
	go nc.Run(ctx)
	require.Eventually(t, func() bool {
		_, ok := s.Registry().Get("e2e-host")
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	c := client.NewConn(client.Config{HubURL: cliURL, Token: "secret"})
	go c.Run(ctx)
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, c.WaitReady(cctx))
	ccancel()

	// Round-trip: local file → node (via FilePutOpen + FileChunk + final Reply)
	dir := t.TempDir()
	src := filepath.Join(dir, "src.bin")
	payload := bytes.Repeat([]byte("xyz"), 5000) // 15 KB
	require.NoError(t, os.WriteFile(src, payload, 0o644))

	remote := filepath.Join(dir, "remote.bin")

	id := client.NewMsgID()
	sum := sha256.Sum256(payload)
	sumHex := hex.EncodeToString(sum[:])
	ch := c.RPC().Register(id)
	require.NoError(t, c.Send(&protocol.FilePutOpen{
		MsgID: id, Target: "e2e-host", Path: remote,
		Size: int64(len(payload)), SHA256: sumHex,
	}))
	r := <-ch
	require.True(t, r.OK, "open reply error: %s", r.Error)
	c.RPC().Unregister(id)
	finalCh := c.RPC().Register(id)
	// Push as a single chunk for simplicity.
	require.NoError(t, c.Send(&protocol.FileChunk{
		MsgID: id, Seq: 0, Data: payload, EOF: true,
	}))
	final := <-finalCh
	require.True(t, final.OK, "final reply error: %s", final.Error)
	c.RPC().Unregister(id)

	got, err := os.ReadFile(remote)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestE2ECaptureScreen(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host"})
	nc.SetHandler(node.NewProcessHandler(t.TempDir(), 50))
	go nc.Run(ctx)
	require.Eventually(t, func() bool {
		_, ok := s.Registry().Get("e2e-host")
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	c := client.NewConn(client.Config{HubURL: cliURL, Token: "secret"})
	go c.Run(ctx)
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, c.WaitReady(cctx))
	ccancel()

	// Start a short non-PTY process that emits two lines.
	startID := client.NewMsgID()
	startCh := c.RPC().Register(startID)
	require.NoError(t, c.Send(&protocol.Start{
		MsgID:     startID,
		Target:    "e2e-host",
		ProcessID: "e2e-cap",
		Cmd:       []string{"sh", "-c", "printf 'foo\\nbar\\n'"},
	}))
	select {
	case r := <-startCh:
		require.True(t, r.OK, "start reply: %+v", r)
	case <-time.After(2 * time.Second):
		t.Fatal("start reply timeout")
	}

	// Poll capture_screen until output is rendered or timeout.
	var lines []string
	var totalLines int
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		capID := client.NewMsgID()
		capCh := c.RPC().Register(capID)
		require.NoError(t, c.Send(&protocol.CaptureScreen{
			MsgID:     capID,
			Target:    "e2e-host",
			ProcessID: "e2e-cap",
		}))
		select {
		case r := <-capCh:
			if !r.OK {
				c.RPC().Unregister(capID)
				time.Sleep(50 * time.Millisecond)
				continue
			}
			lines = asStrings(r.Data["lines"])
			totalLines = asInt(r.Data["total_lines"])
			if asInt(r.Data["cols"]) != 200 {
				t.Fatalf("cols=%v want 200", r.Data["cols"])
			}
			if totalLines >= 2 {
				goto have
			}
		case <-time.After(500 * time.Millisecond):
			// try again
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("capture_screen never reached total_lines>=2 (last lines=%q total=%d)", lines, totalLines)
have:
	if len(lines) != 2 || lines[0] != "foo" || lines[1] != "bar" {
		t.Fatalf("lines=%q", lines)
	}

	// Verify list_processes surfaces a working log_path.
	listID := client.NewMsgID()
	listCh := c.RPC().Register(listID)
	require.NoError(t, c.Send(&protocol.List{MsgID: listID, Target: "e2e-host", Limit: 10}))
	var listReply *protocol.Reply
	select {
	case listReply = <-listCh:
	case <-time.After(2 * time.Second):
		t.Fatal("list reply timeout")
	}
	require.True(t, listReply.OK)
	procs := asMapSlice(listReply.Data["processes"])
	var logPath string
	for _, p := range procs {
		if id, _ := p["process_id"].(string); id == "e2e-cap" {
			logPath, _ = p["log_path"].(string)
		}
	}
	if logPath == "" {
		t.Fatalf("log_path missing for e2e-cap; procs=%+v", procs)
	}
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("log_path does not exist: %v", err)
	}
}

func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case uint64:
		return int(n)
	case float64:
		return int(n)
	}
	return -1
}

func asStrings(v any) []string {
	if ss, ok := v.([]string); ok {
		return ss
	}
	if anys, ok := v.([]any); ok {
		out := make([]string, len(anys))
		for i, x := range anys {
			out[i], _ = x.(string)
		}
		return out
	}
	return nil
}

func asMapSlice(v any) []map[string]any {
	if ms, ok := v.([]map[string]any); ok {
		return ms
	}
	if anys, ok := v.([]any); ok {
		out := make([]map[string]any, 0, len(anys))
		for _, x := range anys {
			switch mm := x.(type) {
			case map[string]any:
				out = append(out, mm)
			case map[any]any:
				conv := make(map[string]any, len(mm))
				for k, vv := range mm {
					if ks, ok := k.(string); ok {
						conv[ks] = vv
					}
				}
				out = append(out, conv)
			}
		}
		return out
	}
	return nil
}

// TestE2EForwardLocalSelfLoop exercises the L (local) forward rule end-to-end.
// Node A holds the rule and listens locally; the hub routes each accepted
// connection to Node B which dials the echo server.
func TestE2EForwardLocalSelfLoop(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go echoLoop(ln)
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	rule := forward.Rule{
		Raw: "L 0:e2e-host-b:127.0.0.1:" + portStr,
		Dir: forward.DirLocal, Bind: "127.0.0.1", ListenPort: 0,
		Device: "e2e-host-b", DestHost: "127.0.0.1", DestPort: port,
	}

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"

	// Node A: holds the L rule; its local listener is what the test connects to.
	phA := node.NewProcessHandler(t.TempDir(), 50)
	phA.ForwardHandler().InitRules([]forward.Rule{rule})
	ncA := node.New(node.Config{
		HubURL: nodeURL, Token: "secret", Hostname: "e2e-host-a",
		OnConnected: func(send node.Sender) {
			phA.ForwardHandler().Start(context.Background(), send)
		},
	})
	ncA.SetHandler(phA)
	go ncA.Run(ctx)

	// Node B: target of the L rule; dials the echo server when ForwardDial arrives.
	phB := node.NewProcessHandler(t.TempDir(), 50)
	ncB := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host-b"})
	ncB.SetHandler(phB)
	go ncB.Run(ctx)

	require.Eventually(t, func() bool { _, ok := s.Registry().Get("e2e-host-a"); return ok },
		2*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { _, ok := s.Registry().Get("e2e-host-b"); return ok },
		2*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return phA.ForwardHandler().LocalAddr(rule) != "" },
		2*time.Second, 20*time.Millisecond)

	addr := phA.ForwardHandler().LocalAddr(rule)
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("hello"))
	require.NoError(t, err)
	buf := make([]byte, 5)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(buf))

	phA.Shutdown()
	phB.Shutdown()
}

// TestE2EForwardRemoteSelfLoop exercises the R (remote) forward rule end-to-end.
// Node A holds the rule and dials the echo server; Node B opens the listener
// and the test connects to it.
func TestE2EForwardRemoteSelfLoop(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go echoLoop(ln)
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	rule := forward.Rule{
		Raw: "R e2e-host-b:0:127.0.0.1:" + portStr,
		Dir: forward.DirRemote, Device: "e2e-host-b",
		Bind: "127.0.0.1", ListenPort: 0,
		DestHost: "127.0.0.1", DestPort: port,
	}

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"

	// Node A: holds the R rule; dials the echo server on each accepted connection.
	phA := node.NewProcessHandler(t.TempDir(), 50)
	phA.ForwardHandler().InitRules([]forward.Rule{rule})

	addrCh := make(chan string, 1)
	tapA := &replyTapHandler{inner: phA, addr: addrCh}

	ncA := node.New(node.Config{
		HubURL: nodeURL, Token: "secret", Hostname: "e2e-host-a",
		OnConnected: func(send node.Sender) {
			phA.ForwardHandler().Start(context.Background(), send)
		},
	})
	ncA.SetHandler(tapA)
	go ncA.Run(ctx)

	// Node B: target of the R rule; opens the TCP listener when ForwardListen arrives.
	phB := node.NewProcessHandler(t.TempDir(), 50)
	ncB := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host-b"})
	ncB.SetHandler(phB)
	go ncB.Run(ctx)

	require.Eventually(t, func() bool { _, ok := s.Registry().Get("e2e-host-a"); return ok },
		2*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { _, ok := s.Registry().Get("e2e-host-b"); return ok },
		2*time.Second, 20*time.Millisecond)

	var addr string
	select {
	case addr = <-addrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no listen_addr captured")
	}

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("world"))
	require.NoError(t, err)
	buf := make([]byte, 5)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, "world", string(buf))

	phA.Shutdown()
	phB.Shutdown()
}

// replyTapHandler wraps a node.Handler and snoops inbound Reply frames for a
// "listen_addr" field. After surfacing it once, all frames continue to flow
// to the wrapped handler.
type replyTapHandler struct {
	inner node.Handler
	addr  chan<- string
}

func (t *replyTapHandler) Handle(ctx context.Context, send node.Sender, msg protocol.Message) {
	if r, ok := msg.(*protocol.Reply); ok && r.OK && r.Data != nil {
		if v, ok := r.Data["listen_addr"].(string); ok {
			select {
			case t.addr <- v:
			default:
			}
		}
	}
	t.inner.Handle(ctx, send, msg)
}

func echoLoop(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer c.Close()
			io.Copy(c, c)
		}(c)
	}
}
