package hub

import (
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
)

func TestDeviceOnlineBroadcast(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	c := &fakePeer{}
	s.Clients().Register(&Client{ID: "c1", ConnectedAt: time.Now(), Conn: c})

	s.broadcastDeviceEvent("device_online", "mac")

	if len(c.sent) != 1 {
		t.Fatalf("client should have received 1 event, got %d", len(c.sent))
	}
	msg, err := protocol.Decode(c.sent[0])
	if err != nil {
		t.Fatal(err)
	}
	ev, ok := msg.(*protocol.Event)
	if !ok || ev.Kind != "device_online" || ev.Device != "mac" {
		t.Fatalf("wrong event: %+v", msg)
	}
}

func TestDeviceOfflineBroadcast(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	c := &fakePeer{}
	s.Clients().Register(&Client{ID: "c1", ConnectedAt: time.Now(), Conn: c})

	s.broadcastDeviceEvent("device_offline", "mac")

	if len(c.sent) != 1 {
		t.Fatalf("client should have received 1 event, got %d", len(c.sent))
	}
	msg, err := protocol.Decode(c.sent[0])
	if err != nil {
		t.Fatal(err)
	}
	ev, ok := msg.(*protocol.Event)
	if !ok || ev.Kind != "device_offline" || ev.Device != "mac" {
		t.Fatalf("wrong event: %+v", msg)
	}
}

func TestNodeForwardDialRoutedToClient(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	client := &fakePeer{}
	s.Forwards().AddListener("f1", client)

	sess := &deviceSession{
		device: &Device{Hostname: "mac"},
		conn:   nil,
		router: s.router,
		server: s,
	}

	msg := &protocol.ForwardDial{MsgID: "m1", StreamID: "s1", ForwardID: "f1"}
	raw, _ := protocol.Encode(msg)

	// Simulate forward frame dispatch from node read loop.
	v := msg
	clientConn, ok := s.forwards.LookupListener(v.ForwardID)
	if !ok {
		t.Fatal("listener not found")
	}
	s.forwards.OpenStream(v.StreamID, clientConn, sess)
	s.router.Register(v.MsgID, clientConn, false)
	_ = clientConn.SendRaw(raw)

	if len(client.sent) != 1 {
		t.Fatalf("client should have received ForwardDial, got %d frames", len(client.sent))
	}
	gc, gn, ok := s.forwards.LookupStream("s1")
	if !ok || gc != client || gn != sess {
		t.Fatalf("stream not registered correctly")
	}
}

func TestNodeForwardDataRoutedToClient(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	client := &fakePeer{}
	sess := &deviceSession{
		device: &Device{Hostname: "mac"},
		conn:   nil,
		router: s.router,
		server: s,
	}
	s.forwards.OpenStream("s1", client, sess)

	msg := &protocol.ForwardData{StreamID: "s1", Data: []byte("hello")}
	raw, _ := protocol.Encode(msg)

	v := msg
	cl, _, ok := s.forwards.LookupStream(v.StreamID)
	if !ok {
		t.Fatal("stream not found")
	}
	_ = cl.SendRaw(raw)

	if len(client.sent) != 1 {
		t.Fatalf("client should have received ForwardData, got %d", len(client.sent))
	}
}

func TestNodeDisconnectEvictsStreams(t *testing.T) {
	s := NewServer(Options{Token: "x"})
	client := &fakePeer{}
	s.Clients().Register(&Client{ID: "c1", ConnectedAt: time.Now(), Conn: client})

	sess := &fakePeer{}
	s.forwards.OpenStream("s1", client, sess)
	s.forwards.OpenStream("s2", client, sess)

	// Simulate the eviction logic from the defer in handleDevice.
	for _, sid := range s.forwards.EvictStreamsForNode(sess) {
		cl := &protocol.ForwardClose{StreamID: sid, Half: "both"}
		raw, _ := protocol.Encode(cl)
		for _, c := range s.clients.List() {
			if c.Conn != nil {
				_ = c.Conn.SendRaw(raw)
			}
		}
	}

	if len(client.sent) != 2 {
		t.Fatalf("client should have received 2 ForwardClose frames, got %d", len(client.sent))
	}
	if _, _, ok := s.forwards.LookupStream("s1"); ok {
		t.Fatal("stream s1 should have been evicted")
	}
	if _, _, ok := s.forwards.LookupStream("s2"); ok {
		t.Fatal("stream s2 should have been evicted")
	}
}
