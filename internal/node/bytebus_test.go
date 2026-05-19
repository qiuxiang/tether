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

func TestByteBus_CloseEndsSubscribers(t *testing.T) {
	b := newByteBus()
	sub := b.Subscribe(0)
	b.Close()
	select {
	case _, ok := <-sub.Ch():
		if ok {
			// allow drained backlog frames; loop until channel closes
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("subscriber not closed after bus close")
	}
}

func drain(t *testing.T, sub *busSub, n int, timeout time.Duration) []byte {
	t.Helper()
	deadline := time.After(timeout)
	var out []byte
	for len(out) < n {
		select {
		case chunk, ok := <-sub.Ch():
			if !ok {
				return out
			}
			out = append(out, chunk...)
		case <-deadline:
			return out
		}
	}
	return out
}
