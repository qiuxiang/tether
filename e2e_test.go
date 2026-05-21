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

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host"})
	nc.SetHandler(node.NewHandler())
	go nc.Run(ctx)
	require.Eventually(t, func() bool {
		_, ok := s.Registry().Get("e2e-host")
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	c := client.NewConn(cliURL, "secret")
	go c.Run(ctx)
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, c.WaitReady(cctx))
	ccancel()

	id := client.NewMsgID()
	ch := c.RPC().Register(id)
	defer c.RPC().Unregister(id)
	require.NoError(t, c.Send(&protocol.Exec{
		MsgID:  id,
		Target: "e2e-host",
		Cmd:    []string{"sh", "-c", "echo hello; echo oops 1>&2; exit 3"},
	}))

	var reply *protocol.Reply
	select {
	case reply = <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("exec reply timeout")
	}
	require.True(t, reply.OK, "exec reply: %+v", reply)
	assert.Contains(t, reply.Data["stdout"].(string), "hello")
	assert.Contains(t, reply.Data["stderr"].(string), "oops")
	assert.EqualValues(t, 3, reply.Data["exit_code"])
	assert.Equal(t, false, reply.Data["timed_out"])
}

func TestE2EExecTimeout(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host"})
	nc.SetHandler(node.NewHandler())
	go nc.Run(ctx)
	require.Eventually(t, func() bool {
		_, ok := s.Registry().Get("e2e-host")
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	c := client.NewConn(cliURL, "secret")
	go c.Run(ctx)
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, c.WaitReady(cctx))
	ccancel()

	id := client.NewMsgID()
	ch := c.RPC().Register(id)
	defer c.RPC().Unregister(id)
	start := time.Now()
	require.NoError(t, c.Send(&protocol.Exec{
		MsgID:   id,
		Target:  "e2e-host",
		Cmd:     []string{"sh", "-c", "echo started; sleep 30"},
		Timeout: 1,
	}))

	var reply *protocol.Reply
	select {
	case reply = <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("exec reply timeout")
	}
	require.True(t, reply.OK, "exec reply: %+v", reply)
	assert.Equal(t, true, reply.Data["timed_out"])
	assert.Contains(t, reply.Data["stdout"].(string), "started")
	assert.Less(t, time.Since(start), 10*time.Second, "exec must return shortly after the node-side timeout")
}

func TestE2EFileTransfer(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host"})
	nc.SetHandler(node.NewHandler())
	go nc.Run(ctx)
	require.Eventually(t, func() bool {
		_, ok := s.Registry().Get("e2e-host")
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	c := client.NewConn(cliURL, "secret")
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
	phA := node.NewHandler()
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
	phB := node.NewHandler()
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
	phA := node.NewHandler()
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
	phB := node.NewHandler()
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

func TestE2ERemoteFileEdit(t *testing.T) {
	s := hub.NewServer(hub.Options{Token: "secret"})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeURL := strings.Replace(ts.URL, "http", "ws", 1) + "/device"
	nc := node.New(node.Config{HubURL: nodeURL, Token: "secret", Hostname: "e2e-host"})
	nc.SetHandler(node.NewHandler())
	go nc.Run(ctx)
	require.Eventually(t, func() bool {
		_, ok := s.Registry().Get("e2e-host")
		return ok
	}, 2*time.Second, 20*time.Millisecond)

	cliURL := strings.Replace(ts.URL, "http", "ws", 1) + "/client"
	c := client.NewConn(cliURL, "secret")
	go c.Run(ctx)
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, c.WaitReady(cctx))
	ccancel()

	dir := t.TempDir()
	target := filepath.Join(dir, "file.txt")

	// 1. write_file
	id := client.NewMsgID()
	ch := c.RPC().Register(id)
	require.NoError(t, c.Send(&protocol.WriteFileReq{
		MsgID: id, Target: "e2e-host", Path: target,
		Content: []byte("alpha\nbeta\ngamma\n"),
	}))
	r := <-ch
	c.RPC().Unregister(id)
	require.True(t, r.OK, "write err: %s", r.Error)

	// 2. read_file
	id = client.NewMsgID()
	ch = c.RPC().Register(id)
	require.NoError(t, c.Send(&protocol.ReadFileReq{MsgID: id, Target: "e2e-host", Path: target}))
	r = <-ch
	c.RPC().Unregister(id)
	require.True(t, r.OK, "read err: %s", r.Error)
	switch v := r.Data["total_lines"].(type) {
	case int:
		require.Equal(t, 3, v)
	case int64:
		require.Equal(t, int64(3), v)
	case uint64:
		require.Equal(t, uint64(3), v)
	default:
		t.Fatalf("unexpected total_lines type: %T = %v", v, v)
	}

	// 3. edit_file
	id = client.NewMsgID()
	ch = c.RPC().Register(id)
	require.NoError(t, c.Send(&protocol.EditFileReq{
		MsgID: id, Target: "e2e-host", Path: target,
		OldString: []byte("beta"), NewString: []byte("BETA"),
	}))
	r = <-ch
	c.RPC().Unregister(id)
	require.True(t, r.OK, "edit err: %s", r.Error)

	// 4. Verify on disk
	got, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "alpha\nBETA\ngamma\n", string(got))
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
