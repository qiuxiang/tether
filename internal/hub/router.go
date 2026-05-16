package hub

import "sync"

// Router maps msg_id → destination peer conn. Used to route replies and
// streamed messages back from a node to the originating client.
//
// One-shot routes are removed after the first delivery; sticky routes stay
// until explicitly unregistered (used for streamed RPCs and file transfers).
type Router struct {
	mu     sync.Mutex
	routes map[string]route
}

type route struct {
	Conn   PeerConn
	Sticky bool
}

func NewRouter() *Router {
	return &Router{routes: make(map[string]route)}
}

// Register associates msg_id with the peer that should receive the reply.
// sticky=true keeps the route alive after Forward; sticky=false removes it
// on first Forward.
func (r *Router) Register(msgID string, conn PeerConn, sticky bool) {
	r.mu.Lock()
	r.routes[msgID] = route{Conn: conn, Sticky: sticky}
	r.mu.Unlock()
}

func (r *Router) Unregister(msgID string) {
	r.mu.Lock()
	delete(r.routes, msgID)
	r.mu.Unlock()
}

// Forward writes raw bytes to the peer registered under msg_id. Returns
// true if a route existed. Removes the route if it was one-shot.
func (r *Router) Forward(msgID string, raw []byte) bool {
	r.mu.Lock()
	rt, ok := r.routes[msgID]
	if ok && !rt.Sticky {
		delete(r.routes, msgID)
	}
	r.mu.Unlock()
	if !ok {
		return false
	}
	_ = rt.Conn.SendRaw(raw)
	return true
}

// Lookup returns the conn for msg_id without removing it (used to inspect
// sticky routes when deciding what to do).
func (r *Router) Lookup(msgID string) (PeerConn, bool) {
	r.mu.Lock()
	rt, ok := r.routes[msgID]
	r.mu.Unlock()
	if !ok {
		return nil, false
	}
	return rt.Conn, true
}
