package hub

import (
	"log"
	"sync"
)

// Router stores msg_id ↔ (Client, Node) PeerConn pairs used to route
// streaming messages in either direction. Use Register for the client
// (one-shot reply or sticky stream) and RegisterNode for the node side
// (when chunks/aborts flow client → node and need to be forwarded by
// msg_id).
type Router struct {
	mu     sync.Mutex
	routes map[string]route
}

type route struct {
	Client PeerConn // the originating client
	Node   PeerConn // the target node (for upload-direction forwarding)
	Sticky bool
}

func NewRouter() *Router {
	return &Router{routes: make(map[string]route)}
}

// Register associates msg_id with the client peer that will receive replies.
// One-shot unless sticky=true.
func (r *Router) Register(msgID string, client PeerConn, sticky bool) {
	r.mu.Lock()
	cur := r.routes[msgID]
	cur.Client = client
	cur.Sticky = sticky
	r.routes[msgID] = cur
	r.mu.Unlock()
}

// RegisterNode marks the node side of a sticky route so chunks/aborts
// flowing from the client can be forwarded to the right node by msg_id.
func (r *Router) RegisterNode(msgID string, node PeerConn) {
	r.mu.Lock()
	cur := r.routes[msgID]
	cur.Node = node
	r.routes[msgID] = cur
	r.mu.Unlock()
}

func (r *Router) Unregister(msgID string) {
	r.mu.Lock()
	delete(r.routes, msgID)
	r.mu.Unlock()
}

// ForwardToClient delivers raw bytes from a node back to the originating
// client. Returns true if a route was found. One-shot routes are removed
// after delivery; sticky routes are also removed if the send errored.
func (r *Router) ForwardToClient(msgID string, raw []byte) bool {
	r.mu.Lock()
	rt, ok := r.routes[msgID]
	if ok && !rt.Sticky {
		delete(r.routes, msgID)
	}
	r.mu.Unlock()
	if !ok || rt.Client == nil {
		return false
	}
	if err := rt.Client.SendRaw(raw); err != nil {
		log.Printf("router: send to client %s failed: %v", msgID, err)
		if rt.Sticky {
			r.mu.Lock()
			delete(r.routes, msgID)
			r.mu.Unlock()
		}
	}
	return true
}

// ForwardToNode delivers raw bytes from a client to the node side of the
// route. Non-removing — used for chunk streams; route cleanup happens on
// EOF/abort elsewhere.
func (r *Router) ForwardToNode(msgID string, raw []byte) bool {
	r.mu.Lock()
	rt, ok := r.routes[msgID]
	r.mu.Unlock()
	if !ok || rt.Node == nil {
		return false
	}
	if err := rt.Node.SendRaw(raw); err != nil {
		log.Printf("router: send to node %s failed: %v", msgID, err)
	}
	return true
}
