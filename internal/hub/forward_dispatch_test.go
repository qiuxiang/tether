package hub

import (
	"testing"

	"github.com/qiuxiang/tether/internal/protocol"
)

func TestClientForwardListenRegisters(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	node := &fakePeer{}
	s.Registry().Register(&Device{Hostname: "mac", Conn: node})
	c := &fakePeer{}
	cs := &clientSession{id: "c1", server: s, pending: map[string]struct{}{}}

	msg := &protocol.ForwardListen{MsgID: "m1", Target: "mac", ForwardID: "f1",
		ListenAddr: "127.0.0.1:0", DestHost: "x", DestPort: 1}
	raw, _ := protocol.Encode(msg)
	cs.dispatchForward(raw, msg, c)

	if got, ok := s.Forwards().LookupListener("f1"); !ok || got != c {
		t.Fatalf("listener not registered")
	}
	if len(node.sent) != 1 {
		t.Fatalf("node should have received the frame")
	}
}

func TestClientForwardDialRegistersStream(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	node := &fakePeer{}
	s.Registry().Register(&Device{Hostname: "mac", Conn: node})
	c := &fakePeer{}
	cs := &clientSession{id: "c1", server: s, pending: map[string]struct{}{}}

	msg := &protocol.ForwardDial{MsgID: "m2", Target: "mac", StreamID: "s1",
		DestHost: "h", DestPort: 22}
	raw, _ := protocol.Encode(msg)
	cs.dispatchForward(raw, msg, c)

	gc, gn, ok := s.Forwards().LookupStream("s1")
	if !ok || gc != c || gn != node {
		t.Fatalf("stream not opened correctly: %v %v %v", gc, gn, ok)
	}
}

func TestClientForwardDataRouted(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	c, n := &fakePeer{}, &fakePeer{}
	s.Forwards().OpenStream("s1", c, n)
	cs := &clientSession{id: "c1", server: s, pending: map[string]struct{}{}}

	msg := &protocol.ForwardData{StreamID: "s1", Data: []byte("hi")}
	raw, _ := protocol.Encode(msg)
	cs.dispatchForward(raw, msg, c)
	if len(n.sent) != 1 {
		t.Fatalf("node should have received forward_data")
	}
}

func TestClientForwardCloseClearsStream(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	c, n := &fakePeer{}, &fakePeer{}
	s.Forwards().OpenStream("s1", c, n)
	cs := &clientSession{id: "c1", server: s, pending: map[string]struct{}{}}

	msg := &protocol.ForwardClose{StreamID: "s1"}
	raw, _ := protocol.Encode(msg)
	cs.dispatchForward(raw, msg, c)
	if _, _, ok := s.Forwards().LookupStream("s1"); ok {
		t.Fatalf("stream should be closed (Half default 'both')")
	}
}
