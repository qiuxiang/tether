package hub

import (
	"sync"

	"github.com/qiuxiang/tether/internal/protocol"
)

// Router correlates Reply messages back to the goroutine that sent the request.
// It also fans ExecOutput/ExecExit chunks for streaming RPCs.
type Router struct {
	mu      sync.Mutex
	replies map[string]chan *protocol.Reply
	streams map[string]chan protocol.Message // ExecOutput, ExecExit
}

func NewRouter() *Router {
	return &Router{
		replies: make(map[string]chan *protocol.Reply),
		streams: make(map[string]chan protocol.Message),
	}
}

func (r *Router) Register(msgID string) chan *protocol.Reply {
	ch := make(chan *protocol.Reply, 1)
	r.mu.Lock()
	r.replies[msgID] = ch
	r.mu.Unlock()
	return ch
}

func (r *Router) RegisterStream(msgID string) chan protocol.Message {
	ch := make(chan protocol.Message, 32)
	r.mu.Lock()
	r.streams[msgID] = ch
	r.mu.Unlock()
	return ch
}

func (r *Router) Unregister(msgID string) {
	r.mu.Lock()
	delete(r.replies, msgID)
	delete(r.streams, msgID)
	r.mu.Unlock()
}

func (r *Router) Deliver(msg protocol.Message) {
	switch m := msg.(type) {
	case *protocol.Reply:
		r.mu.Lock()
		ch, ok := r.replies[m.MsgID]
		r.mu.Unlock()
		if ok {
			select {
			case ch <- m:
			default:
			}
		}
	case *protocol.ExecOutput:
		r.mu.Lock()
		ch, ok := r.streams[m.MsgID]
		r.mu.Unlock()
		if ok {
			ch <- m
		}
	case *protocol.ExecExit:
		r.mu.Lock()
		ch, ok := r.streams[m.MsgID]
		r.mu.Unlock()
		if ok {
			ch <- m
			close(ch)
			r.Unregister(m.MsgID)
		}
	}
}
