package hub

import "sync"

// ForwardTable tracks port-forward listener registrations and active TCP
// streams brokered by the hub.
//
// listeners: forward_id → client peer (registered via forward_listen)
// streams:   stream_id  → (client, node) pair for an active TCP proxy stream
type ForwardTable struct {
	mu        sync.Mutex
	listeners map[string]PeerConn
	streams   map[string]streamRoute
}

type streamRoute struct {
	Client PeerConn
	Node   PeerConn
}

func NewForwardTable() *ForwardTable {
	return &ForwardTable{
		listeners: make(map[string]PeerConn),
		streams:   make(map[string]streamRoute),
	}
}

func (t *ForwardTable) AddListener(forwardID string, client PeerConn) {
	t.mu.Lock()
	t.listeners[forwardID] = client
	t.mu.Unlock()
}

func (t *ForwardTable) RemoveListener(forwardID string) {
	t.mu.Lock()
	delete(t.listeners, forwardID)
	t.mu.Unlock()
}

func (t *ForwardTable) LookupListener(forwardID string) (PeerConn, bool) {
	t.mu.Lock()
	p, ok := t.listeners[forwardID]
	t.mu.Unlock()
	return p, ok
}

// RemoveListenersForClient removes all listener entries owned by the given
// client and returns the removed forward IDs.
func (t *ForwardTable) RemoveListenersForClient(client PeerConn) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []string
	for fid, p := range t.listeners {
		if p == client {
			out = append(out, fid)
			delete(t.listeners, fid)
		}
	}
	return out
}

func (t *ForwardTable) OpenStream(streamID string, client, node PeerConn) {
	t.mu.Lock()
	t.streams[streamID] = streamRoute{Client: client, Node: node}
	t.mu.Unlock()
}

func (t *ForwardTable) LookupStream(streamID string) (PeerConn, PeerConn, bool) {
	t.mu.Lock()
	r, ok := t.streams[streamID]
	t.mu.Unlock()
	if !ok {
		return nil, nil, false
	}
	return r.Client, r.Node, true
}

func (t *ForwardTable) CloseStream(streamID string) {
	t.mu.Lock()
	delete(t.streams, streamID)
	t.mu.Unlock()
}

// EvictStreamsForNode removes all streams whose node peer matches n and
// returns the removed stream IDs.
func (t *ForwardTable) EvictStreamsForNode(n PeerConn) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []string
	for sid, r := range t.streams {
		if r.Node == n {
			out = append(out, sid)
			delete(t.streams, sid)
		}
	}
	return out
}

// EvictStreamsForClient removes all streams whose client peer matches c and
// returns the removed stream IDs.
func (t *ForwardTable) EvictStreamsForClient(c PeerConn) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []string
	for sid, r := range t.streams {
		if r.Client == c {
			out = append(out, sid)
			delete(t.streams, sid)
		}
	}
	return out
}
