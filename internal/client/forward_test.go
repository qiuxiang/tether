package client

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/forward"
	"github.com/qiuxiang/tether/internal/protocol"
)

// captureSender records every sent message for inspection in tests.
type captureSender struct {
	mu   sync.Mutex
	msgs []protocol.Message
}

func (s *captureSender) Send(msg protocol.Message) error {
	s.mu.Lock()
	s.msgs = append(s.msgs, msg)
	s.mu.Unlock()
	return nil
}

func (s *captureSender) take() []protocol.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.msgs
	s.msgs = nil
	return out
}

// waitFor polls cond every 10 ms up to d.
func waitFor(t *testing.T, cond func() bool, d time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// portStr returns the port portion of a net.Addr string.
func portStr(addr string) string {
	_, p, _ := net.SplitHostPort(addr)
	return p
}

// startEchoServer starts a TCP echo server and returns its listener and port.
func startEchoServer(t *testing.T) (net.Listener, int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 256)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					c.Write(buf[:n])
				}
			}(c)
		}
	}()
	_, ps, _ := net.SplitHostPort(ln.Addr().String())
	p, _ := strconv.Atoi(ps)
	return ln, p
}

// TestForwardL_LocalBind verifies that startLocal binds a listener and that
// accepting a connection emits a ForwardDial to the sender.
func TestForwardL_LocalBind(t *testing.T) {
	sender := &captureSender{}
	rule := forward.Rule{
		Raw:        "L 0:mydevice:127.0.0.1:9999",
		Dir:        forward.DirLocal,
		Bind:       "127.0.0.1",
		ListenPort: 0, // OS picks port
		Device:     "mydevice",
		DestHost:   "127.0.0.1",
		DestPort:   9999,
	}
	fm := NewForwardManager(sender, []forward.Rule{rule})
	ctx := t.Context()
	fm.Start(ctx)
	defer fm.Stop()

	addr := fm.LocalAddr(rule)
	if addr == "" {
		t.Fatal("LocalAddr returned empty — listener not bound")
	}

	// Dial the local listener so the accept loop fires.
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial local listener: %v", err)
	}
	defer c.Close()

	// Wait for a ForwardDial message to be sent.
	var dial *protocol.ForwardDial
	if !waitFor(t, func() bool {
		sender.mu.Lock()
		defer sender.mu.Unlock()
		for _, m := range sender.msgs {
			if d, ok := m.(*protocol.ForwardDial); ok {
				dial = d
				return true
			}
		}
		return false
	}, 2*time.Second) {
		t.Fatal("did not receive ForwardDial")
	}

	if dial.Target != "mydevice" {
		t.Errorf("ForwardDial.Target = %q, want %q", dial.Target, "mydevice")
	}
	if dial.DestHost != "127.0.0.1" {
		t.Errorf("ForwardDial.DestHost = %q, want %q", dial.DestHost, "127.0.0.1")
	}
	if dial.DestPort != 9999 {
		t.Errorf("ForwardDial.DestPort = %d, want %d", dial.DestPort, 9999)
	}
}

// TestForwardL_DataDelivery verifies that Deliver(ForwardData) writes bytes to
// the accepted connection.
func TestForwardL_DataDelivery(t *testing.T) {
	sender := &captureSender{}
	rule := forward.Rule{
		Raw:        "L 0:mydevice:127.0.0.1:9998",
		Dir:        forward.DirLocal,
		Bind:       "127.0.0.1",
		ListenPort: 0,
		Device:     "mydevice",
		DestHost:   "127.0.0.1",
		DestPort:   9998,
	}
	fm := NewForwardManager(sender, []forward.Rule{rule})
	ctx := t.Context()
	fm.Start(ctx)
	defer fm.Stop()

	addr := fm.LocalAddr(rule)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial local listener: %v", err)
	}
	defer c.Close()

	// Wait for the stream to be registered (ForwardDial sent).
	var sid string
	if !waitFor(t, func() bool {
		sender.mu.Lock()
		defer sender.mu.Unlock()
		for _, m := range sender.msgs {
			if d, ok := m.(*protocol.ForwardDial); ok {
				sid = d.StreamID
				return true
			}
		}
		return false
	}, 2*time.Second) {
		t.Fatal("did not receive ForwardDial")
	}

	// Deliver data into the local conn via the manager.
	payload := []byte("hello from hub")
	fm.Deliver(&protocol.ForwardData{StreamID: sid, Data: payload})

	// Read it back from the client side.
	buf := make([]byte, len(payload))
	c.SetDeadline(time.Now().Add(2 * time.Second))
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("reading data: %v", err)
	}
	if string(buf[:n]) != string(payload) {
		t.Errorf("got %q, want %q", buf[:n], payload)
	}
}

// TestForwardL_CloseStream verifies that Deliver(ForwardClose{Half:"both"})
// closes the local conn so the client side observes EOF.
func TestForwardL_CloseStream(t *testing.T) {
	sender := &captureSender{}
	rule := forward.Rule{
		Raw:        "L 0:mydevice:127.0.0.1:9997",
		Dir:        forward.DirLocal,
		Bind:       "127.0.0.1",
		ListenPort: 0,
		Device:     "mydevice",
		DestHost:   "127.0.0.1",
		DestPort:   9997,
	}
	fm := NewForwardManager(sender, []forward.Rule{rule})
	ctx := t.Context()
	fm.Start(ctx)
	defer fm.Stop()

	addr := fm.LocalAddr(rule)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial local listener: %v", err)
	}
	defer c.Close()

	var sid string
	if !waitFor(t, func() bool {
		sender.mu.Lock()
		defer sender.mu.Unlock()
		for _, m := range sender.msgs {
			if d, ok := m.(*protocol.ForwardDial); ok {
				sid = d.StreamID
				return true
			}
		}
		return false
	}, 2*time.Second) {
		t.Fatal("did not receive ForwardDial")
	}

	fm.Deliver(&protocol.ForwardClose{StreamID: sid, Half: "both"})

	c.SetDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	n, err := c.Read(buf)
	if n != 0 || err == nil {
		t.Errorf("expected EOF/error, got n=%d err=%v", n, err)
	}
}

// TestForwardR_StartRemote verifies that startRemote sends a ForwardListen.
func TestForwardR_StartRemote(t *testing.T) {
	sender := &captureSender{}
	rule := forward.Rule{
		Raw:        "R mydevice:8080:127.0.0.1:3000",
		Dir:        forward.DirRemote,
		Bind:       "127.0.0.1",
		ListenPort: 8080,
		Device:     "mydevice",
		DestHost:   "127.0.0.1",
		DestPort:   3000,
	}
	fm := NewForwardManager(sender, []forward.Rule{rule})
	ctx := t.Context()
	fm.Start(ctx)
	defer fm.Stop()

	if !waitFor(t, func() bool {
		sender.mu.Lock()
		defer sender.mu.Unlock()
		for _, m := range sender.msgs {
			if _, ok := m.(*protocol.ForwardListen); ok {
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatal("did not receive ForwardListen")
	}
}

// TestForwardDeviceOnline verifies that a device_online event re-issues
// ForwardListen for R rules targeting that device.
func TestForwardDeviceOnline(t *testing.T) {
	sender := &captureSender{}
	rule := forward.Rule{
		Raw:        "R mydevice:8081:127.0.0.1:3001",
		Dir:        forward.DirRemote,
		Bind:       "127.0.0.1",
		ListenPort: 8081,
		Device:     "mydevice",
		DestHost:   "127.0.0.1",
		DestPort:   3001,
	}
	fm := NewForwardManager(sender, []forward.Rule{rule})
	ctx := t.Context()
	fm.Start(ctx)
	defer fm.Stop()

	// First ForwardListen from Start.
	if !waitFor(t, func() bool {
		sender.mu.Lock()
		defer sender.mu.Unlock()
		count := 0
		for _, m := range sender.msgs {
			if _, ok := m.(*protocol.ForwardListen); ok {
				count++
			}
		}
		return count >= 1
	}, time.Second) {
		t.Fatal("initial ForwardListen not received")
	}

	// Simulate device_online event.
	fm.Deliver(&protocol.Event{Kind: "device_online", Device: "mydevice"})

	// Should produce a second ForwardListen.
	if !waitFor(t, func() bool {
		sender.mu.Lock()
		defer sender.mu.Unlock()
		count := 0
		for _, m := range sender.msgs {
			if _, ok := m.(*protocol.ForwardListen); ok {
				count++
			}
		}
		return count >= 2
	}, time.Second) {
		t.Fatal("device_online did not re-issue ForwardListen")
	}
}

// Compile-time check: *Conn satisfies Sender.
var _ Sender = (*Conn)(nil)

// Keep portStr used.
var _ = portStr
