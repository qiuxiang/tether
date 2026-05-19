package node

import "sync"

// byteBus is an append-only byte buffer with live subscribers. Each new write
// appends to buf and is fanned out to every active busSub. New subscribers
// can ask for the existing backlog starting from a given offset; subsequent
// writes are delivered live.
//
// Bus.Close() ends every subscriber's channel; further Writes are no-ops.
// Unsubscribe removes a single subscriber without closing the bus.
type byteBus struct {
	mu     sync.Mutex
	buf    []byte
	subs   map[*busSub]struct{}
	closed bool
}

type busSub struct {
	ch chan []byte
}

func (s *busSub) Ch() <-chan []byte { return s.ch }

func newByteBus() *byteBus {
	return &byteBus{subs: make(map[*busSub]struct{})}
}

// Write appends p to the buffer and fans out a copy to every active subscriber.
// Returns len(p), nil to satisfy io.Writer-shaped callers.
func (b *byteBus) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return len(p), nil
	}
	b.buf = append(b.buf, cp...)
	subs := make([]*busSub, 0, len(b.subs))
	for s := range b.subs {
		subs = append(subs, s)
	}
	b.mu.Unlock()
	for _, s := range subs {
		// Non-blocking-ish: if a slow subscriber backs up we still want to
		// deliver because bytes are append-only and dropping silently would
		// corrupt the agent's view. Use a generous buffer (Subscribe) and
		// accept that a stuck consumer blocks Write here.
		s.ch <- cp
	}
	return len(p), nil
}

// Subscribe returns a subscriber that first receives any buffered bytes from
// fromOffset onward, then receives every subsequent Write.
//
// fromOffset is clamped: negative → 0, beyond buffer end → buffer end.
func (b *byteBus) Subscribe(fromOffset int64) *busSub {
	sub := &busSub{ch: make(chan []byte, 64)}
	b.mu.Lock()
	if fromOffset < 0 {
		fromOffset = 0
	}
	if fromOffset > int64(len(b.buf)) {
		fromOffset = int64(len(b.buf))
	}
	backlog := b.buf[fromOffset:]
	if len(backlog) > 0 {
		cp := make([]byte, len(backlog))
		copy(cp, backlog)
		// Buffered channel sized to absorb a single backlog chunk.
		select {
		case sub.ch <- cp:
		default:
			// Should not happen for a fresh channel; fall through.
			go func() { sub.ch <- cp }()
		}
	}
	if b.closed {
		close(sub.ch)
		b.mu.Unlock()
		return sub
	}
	b.subs[sub] = struct{}{}
	b.mu.Unlock()
	return sub
}

// Unsubscribe removes sub and closes its channel. Safe to call once.
func (b *byteBus) Unsubscribe(sub *busSub) {
	b.mu.Lock()
	if _, ok := b.subs[sub]; ok {
		delete(b.subs, sub)
		close(sub.ch)
	}
	b.mu.Unlock()
}

// Close ends the bus: subscribers' channels are closed, future Writes drop.
func (b *byteBus) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	for s := range b.subs {
		close(s.ch)
	}
	b.subs = nil
	b.mu.Unlock()
}

// Len returns the current buffer length (bytes written so far).
func (b *byteBus) Len() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return int64(len(b.buf))
}
