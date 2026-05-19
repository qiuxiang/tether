package node

import (
	"testing"
	"time"
)

func TestByteBus_NewSubscriberGetsBacklog(t *testing.T) {
	b := newByteBus()
	b.Write([]byte("hello "))
	b.Write([]byte("world"))

	sub := b.Subscribe(0)
	defer b.Unsubscribe(sub)

	got := drain(t, sub, 11, 200*time.Millisecond)
	if string(got) != "hello world" {
		t.Fatalf("backlog got %q, want %q", got, "hello world")
	}
}

func TestByteBus_LiveAppendDelivered(t *testing.T) {
	b := newByteBus()
	sub := b.Subscribe(0)
	defer b.Unsubscribe(sub)

	b.Write([]byte("ab"))
	b.Write([]byte("cd"))

	got := drain(t, sub, 4, 200*time.Millisecond)
	if string(got) != "abcd" {
		t.Fatalf("live got %q, want %q", got, "abcd")
	}
}

func TestByteBus_FromOffsetSkipsBacklog(t *testing.T) {
	b := newByteBus()
	b.Write([]byte("ignore-this-"))
	sub := b.Subscribe(int64(len("ignore-this-")))
	defer b.Unsubscribe(sub)

	b.Write([]byte("keep"))

	got := drain(t, sub, 4, 200*time.Millisecond)
	if string(got) != "keep" {
		t.Fatalf("offset got %q, want %q", got, "keep")
	}
}

func TestByteBus_CloseSignalsDone(t *testing.T) {
	b := newByteBus()
	sub := b.Subscribe(0)
	b.Close()
	select {
	case <-sub.Done():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("sub.Done() not signaled after bus close")
	}
}

func TestByteBus_UnsubscribeSignalsDone(t *testing.T) {
	b := newByteBus()
	sub := b.Subscribe(0)
	b.Unsubscribe(sub)
	select {
	case <-sub.Done():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("sub.Done() not signaled after Unsubscribe")
	}
}

func TestByteBus_ConcurrentWriteAndUnsubscribe(t *testing.T) {
	b := newByteBus()
	subs := make([]*busSub, 50)
	for i := range subs {
		subs[i] = b.Subscribe(0)
		// Drain in the background so Writes don't block forever.
		go func(s *busSub) {
			for {
				select {
				case <-s.Ch():
				case <-s.Done():
					return
				}
			}
		}(subs[i])
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			b.Write([]byte("x"))
		}
		close(done)
	}()
	for _, s := range subs {
		b.Unsubscribe(s)
	}
	<-done
	b.Close()
}

func drain(t *testing.T, sub *busSub, n int, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.After(timeout)
	var out []byte
	for len(out) < n {
		select {
		case chunk := <-sub.Ch():
			out = append(out, chunk...)
		case <-sub.Done():
			return out
		case <-deadline:
			return out
		}
	}
	return out
}
