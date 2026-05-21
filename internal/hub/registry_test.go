package hub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegistryRegisterAndGet(t *testing.T) {
	r := NewRegistry()
	d := &Device{Hostname: "mac", OS: "darwin", Arch: "arm64"}
	replaced := r.Register(d)
	assert.Nil(t, replaced)

	got, ok := r.Get("mac")
	assert.True(t, ok)
	assert.Equal(t, d, got)
}

func TestRegistryDuplicateHostnameReplaces(t *testing.T) {
	r := NewRegistry()
	old := &fakeConn{}
	r.Register(&Device{Hostname: "mac", Conn: old})
	newConn := &fakeConn{}
	replaced := r.Register(&Device{Hostname: "mac", Conn: newConn})
	assert.Equal(t, PeerConn(old), replaced)
	got, _ := r.Get("mac")
	assert.Equal(t, PeerConn(newConn), got.Conn)
}

func TestRegistryUnregister(t *testing.T) {
	r := NewRegistry()
	d := &Device{Hostname: "mac"}
	r.Register(d)
	r.Unregister("mac")
	_, ok := r.Get("mac")
	assert.False(t, ok)
}

func TestRegistryUnregisterIfMismatch(t *testing.T) {
	r := NewRegistry()
	stale := &fakeConn{}
	current := &fakeConn{}
	r.Register(&Device{Hostname: "mac", Conn: current})
	assert.False(t, r.UnregisterIf("mac", stale))
	_, ok := r.Get("mac")
	assert.True(t, ok, "current entry must survive stale cleanup")
	assert.True(t, r.UnregisterIf("mac", current))
	_, ok = r.Get("mac")
	assert.False(t, ok)
}

func TestRegistryList(t *testing.T) {
	r := NewRegistry()
	r.Register(&Device{Hostname: "a"})
	r.Register(&Device{Hostname: "b"})
	got := r.List()
	assert.Len(t, got, 2)
}
