package hub

import (
	"sync"
	"testing"
)

type fakePeer struct {
	mu   sync.Mutex
	sent [][]byte
}

func (p *fakePeer) SendRaw(b []byte) error {
	p.mu.Lock()
	p.sent = append(p.sent, append([]byte(nil), b...))
	p.mu.Unlock()
	return nil
}
func (p *fakePeer) Close() {}

func TestForwardTableListeners(t *testing.T) {
	ft := NewForwardTable()
	c := &fakePeer{}
	ft.AddListener("f1", c)
	got, ok := ft.LookupListener("f1")
	if !ok || got != c {
		t.Fatalf("lookup miss")
	}
	ft.RemoveListenersForClient(c)
	if _, ok := ft.LookupListener("f1"); ok {
		t.Fatalf("listener not removed")
	}
}

func TestForwardTableStreams(t *testing.T) {
	ft := NewForwardTable()
	c, n := &fakePeer{}, &fakePeer{}
	ft.OpenStream("s1", c, n)
	gc, gn, ok := ft.LookupStream("s1")
	if !ok || gc != c || gn != n {
		t.Fatalf("lookup mismatch")
	}
	ft.CloseStream("s1")
	if _, _, ok := ft.LookupStream("s1"); ok {
		t.Fatalf("stream not closed")
	}
}

func TestForwardTableEvictByPeer(t *testing.T) {
	ft := NewForwardTable()
	c, n1, n2 := &fakePeer{}, &fakePeer{}, &fakePeer{}
	ft.OpenStream("s1", c, n1)
	ft.OpenStream("s2", c, n2)
	evicted := ft.EvictStreamsForNode(n1)
	if len(evicted) != 1 {
		t.Fatalf("expected 1 evicted, got %v", evicted)
	}
	if opp, ok := evicted["s1"]; !ok || opp != c {
		t.Fatalf("expected s1→c, got %v", evicted)
	}
	if _, _, ok := ft.LookupStream("s1"); ok {
		t.Fatalf("s1 should be gone")
	}
	if _, _, ok := ft.LookupStream("s2"); !ok {
		t.Fatalf("s2 should remain")
	}
}
