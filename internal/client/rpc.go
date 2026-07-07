package client

import (
	"sync"

	"github.com/qiuxiang/tether/internal/protocol"
)

type RPC struct {
	mu      sync.Mutex
	replies map[string]chan *protocol.Reply
	streams map[string]chan protocol.Message
}

func NewRPC() *RPC {
	return &RPC{
		replies: make(map[string]chan *protocol.Reply),
		streams: make(map[string]chan protocol.Message),
	}
}

func NewMsgID() string { return protocol.NewID(8) }

func (r *RPC) Register(msgID string) chan *protocol.Reply {
	return r.RegisterBuf(msgID, 1)
}

// RegisterBuf is like Register but allows specifying the channel buffer size.
// Use size ≥ 2 when the hub will deliver multiple Reply frames on the same
// msg_id (e.g. upload: ok-to-send + final).
func (r *RPC) RegisterBuf(msgID string, size int) chan *protocol.Reply {
	ch := make(chan *protocol.Reply, size)
	r.mu.Lock()
	r.replies[msgID] = ch
	r.mu.Unlock()
	return ch
}

func (r *RPC) RegisterStream(msgID string) chan protocol.Message {
	ch := make(chan protocol.Message, 32)
	r.mu.Lock()
	r.streams[msgID] = ch
	r.mu.Unlock()
	return ch
}

func (r *RPC) Unregister(msgID string) {
	r.mu.Lock()
	delete(r.replies, msgID)
	delete(r.streams, msgID)
	r.mu.Unlock()
}

func (r *RPC) Deliver(msg protocol.Message) {
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
	case *protocol.FileChunk:
		r.mu.Lock()
		ch, ok := r.streams[m.MsgID]
		r.mu.Unlock()
		if ok {
			ch <- m
			if m.EOF {
				close(ch)
				r.Unregister(m.MsgID)
			}
		}
	case *protocol.FileAbort:
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
