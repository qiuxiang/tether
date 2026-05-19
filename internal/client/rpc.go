package client

import (
	"crypto/rand"
	"encoding/hex"
	"sync"

	"github.com/qiuxiang/tether/internal/protocol"
)

type RPC struct {
	mu      sync.Mutex
	replies map[string]chan *protocol.Reply
	streams map[string]chan protocol.Message
	forward func(protocol.Message)
}

// SetForwardHandler registers a callback that receives forward-related frames
// (ForwardDial, ForwardData, ForwardClose, Event) and Reply frames that have
// no registered reply channel.
func (r *RPC) SetForwardHandler(f func(protocol.Message)) {
	r.mu.Lock()
	r.forward = f
	r.mu.Unlock()
}

func NewRPC() *RPC {
	return &RPC{
		replies: make(map[string]chan *protocol.Reply),
		streams: make(map[string]chan protocol.Message),
	}
}

func NewMsgID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

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
	case *protocol.ForwardDial, *protocol.ForwardData, *protocol.ForwardClose, *protocol.Event:
		r.mu.Lock()
		f := r.forward
		r.mu.Unlock()
		if f != nil {
			f(m)
		}
	case *protocol.Reply:
		r.mu.Lock()
		ch, ok := r.replies[m.MsgID]
		f := r.forward
		r.mu.Unlock()
		if ok {
			select {
			case ch <- m:
			default:
			}
			return
		}
		// No registered reply channel — fan to forward handler (e.g. dial-back replies).
		if f != nil {
			f(m)
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

// RegisterStreamRaw is an alias for RegisterStream — kept for naming
// clarity in callers that consume FileChunk/FileAbort streams.
func (r *RPC) RegisterStreamRaw(msgID string) chan protocol.Message {
	return r.RegisterStream(msgID)
}
