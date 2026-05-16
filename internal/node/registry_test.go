package node

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProcessRegistryCapEviction(t *testing.T) {
	reg := NewProcessRegistry(3)
	// Add 3 exited processes with distinct last_active_at.
	for i := 0; i < 3; i++ {
		p := &Process{ID: string(rune('a' + i)), Status: "exited", LastActiveAt: time.Unix(int64(i), 0)}
		reg.Add(p)
	}
	// Add a 4th — oldest exited ('a') should be evicted.
	reg.Add(&Process{ID: "d", Status: "exited", LastActiveAt: time.Unix(10, 0)})
	_, ok := reg.Get("a")
	assert.False(t, ok)
	_, ok = reg.Get("d")
	assert.True(t, ok)
}

func TestProcessRegistryRunningNeverEvicted(t *testing.T) {
	reg := NewProcessRegistry(2)
	reg.Add(&Process{ID: "old-running", Status: "running", LastActiveAt: time.Unix(0, 0)})
	reg.Add(&Process{ID: "new-exited", Status: "exited", LastActiveAt: time.Unix(5, 0)})
	reg.Add(&Process{ID: "newer-exited", Status: "exited", LastActiveAt: time.Unix(10, 0)})
	// Limit hit. Adding another exited should evict old-exited, NOT old-running.
	reg.Add(&Process{ID: "newest-exited", Status: "exited", LastActiveAt: time.Unix(20, 0)})

	_, ok := reg.Get("old-running")
	assert.True(t, ok, "running process must not be evicted")
	_, ok = reg.Get("new-exited")
	assert.False(t, ok, "oldest exited should have been evicted")
}
