package node

import "sync"

// byteBus is an append-only byte buffer with live subscribers. Each new write
// appends to buf and is fanned out to every active busSub. New subscribers
// can ask for the existing backlog starting from a given offset; subsequent
// writes are delivered live.
//
// Cancellation signal is sub.done — never the data channel. Consumers must
// select on both sub.Ch() and sub.Done(); a closed sub.done means the bus
// is finished (process exit) or this sub was cancelled (Unsubscribe). The
// data channel ch is intentionally never closed so a concurrent Write
// cannot panic with send-on-closed-channel.
//
// Note: buf is never truncated — memory grows with total process output.
type byteBus struct {
	mu     sync.Mutex
	buf    []byte
	subs   map[*busSub]struct{}
	closed bool
}

type busSub struct {
	ch   chan []byte
	done chan struct{}
}

func (s *busSub) Ch() <-chan []byte     { return s.ch }
func (s *busSub) Done() <-chan struct{} { return s.done }

func newByteBus() *byteBus {
	return &byteBus{subs: make(map[*busSub]struct{})}
}

// Write appends p to the buffer and fans out a copy to every active subscriber.
// Returns len(p), nil to satisfy io.Writer-shaped callers.
// Slow consumers still block Write; cancelled subscribers are dropped immediately.
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
		select {
		case s.ch <- cp:
		case <-s.done:
		}
	}
	return len(p), nil
}

// Subscribe returns a subscriber that first receives any buffered bytes from
// fromOffset onward, then receives every subsequent Write.
//
// fromOffset is clamped: negative → 0, beyond buffer end → buffer end.
func (b *byteBus) Subscribe(fromOffset int64) *busSub {
	sub := &busSub{
		ch:   make(chan []byte, 64),
		done: make(chan struct{}),
	}
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
		sub.ch <- cp
	}
	if b.closed {
		close(sub.done)
		b.mu.Unlock()
		return sub
	}
	b.subs[sub] = struct{}{}
	b.mu.Unlock()
	return sub
}

// Unsubscribe signals cancellation on sub.done. The data channel is not
// closed (a concurrent Write may still be mid-send). Safe to call multiple
// times — the second call is a no-op.
func (b *byteBus) Unsubscribe(sub *busSub) {
	b.mu.Lock()
	if _, ok := b.subs[sub]; ok {
		delete(b.subs, sub)
		close(sub.done)
	}
	b.mu.Unlock()
}

// Close ends the bus: every subscriber's done is closed, future Writes drop.
// Data channels are deliberately not closed.
func (b *byteBus) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	for s := range b.subs {
		close(s.done)
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
