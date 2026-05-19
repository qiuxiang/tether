package node

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
)

type fwdCaptureSender struct {
	mu   sync.Mutex
	msgs []protocol.Message
}

func (s *fwdCaptureSender) Send(msg protocol.Message) error {
	s.mu.Lock()
	s.msgs = append(s.msgs, msg)
	s.mu.Unlock()
	return nil
}

func (s *fwdCaptureSender) take() []protocol.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.msgs
	s.msgs = nil
	return out
}

func TestForwardDialEcho(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 256)
		for {
			n, err := c.Read(buf)
			if err != nil {
				return
			}
			c.Write(buf[:n])
		}
	}()
	host, port := splitHostPortTest(t, ln.Addr().String())

	send := &fwdCaptureSender{}
	h := NewForwardHandler()
	h.Dial(send, &protocol.ForwardDial{MsgID: "m1", StreamID: "s1",
		DestHost: host, DestPort: port})

	if !fwdWaitFor(t, func() bool {
		send.mu.Lock()
		defer send.mu.Unlock()
		for _, m := range send.msgs {
			if r, ok := m.(*protocol.Reply); ok && r.OK {
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("did not get ok reply")
	}

	h.Data(send, &protocol.ForwardData{StreamID: "s1", Data: []byte("ping")})
	if !fwdWaitFor(t, func() bool {
		send.mu.Lock()
		defer send.mu.Unlock()
		for _, m := range send.msgs {
			if d, ok := m.(*protocol.ForwardData); ok && string(d.Data) == "ping" {
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("did not echo")
	}

	h.Close(send, &protocol.ForwardClose{StreamID: "s1", Half: "both"})
	h.Shutdown()
}

func TestForwardListenAcceptDialsBack(t *testing.T) {
	send := &fwdCaptureSender{}
	h := NewForwardHandler()
	h.Listen(send, &protocol.ForwardListen{MsgID: "m1", ForwardID: "f1",
		ListenAddr: "127.0.0.1:0", DestHost: "ignored", DestPort: 0})

	var addr string
	if !fwdWaitFor(t, func() bool {
		send.mu.Lock()
		defer send.mu.Unlock()
		for _, m := range send.msgs {
			r, ok := m.(*protocol.Reply)
			if !ok || !r.OK {
				continue
			}
			if v, ok := r.Data["listen_addr"].(string); ok {
				addr = v
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("no listen reply")
	}

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if !fwdWaitFor(t, func() bool {
		send.mu.Lock()
		defer send.mu.Unlock()
		for _, m := range send.msgs {
			if d, ok := m.(*protocol.ForwardDial); ok && d.ForwardID == "f1" {
				return true
			}
		}
		return false
	}, time.Second) {
		t.Fatalf("no dial frame")
	}

	h.Unlisten(send, &protocol.ForwardUnlisten{MsgID: "u1", ForwardID: "f1"})
	h.Shutdown()
}

func fwdWaitFor(t *testing.T, cond func() bool, d time.Duration) bool {
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

func splitHostPortTest(t *testing.T, addr string) (string, int) {
	t.Helper()
	h, ps, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	p := 0
	for _, c := range ps {
		p = p*10 + int(c-'0')
	}
	return h, p
}

var _ = io.EOF
