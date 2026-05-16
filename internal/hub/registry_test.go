package hub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegistryRegisterAndGet(t *testing.T) {
	r := NewRegistry()
	d := &Device{Hostname: "mac", OS: "darwin", Arch: "arm64"}
	err := r.Register(d)
	assert.NoError(t, err)

	got, ok := r.Get("mac")
	assert.True(t, ok)
	assert.Equal(t, d, got)
}

func TestRegistryDuplicateHostname(t *testing.T) {
	r := NewRegistry()
	r.Register(&Device{Hostname: "mac"})
	err := r.Register(&Device{Hostname: "mac"})
	assert.ErrorContains(t, err, "already")
}

func TestRegistryUnregister(t *testing.T) {
	r := NewRegistry()
	d := &Device{Hostname: "mac"}
	r.Register(d)
	r.Unregister("mac")
	_, ok := r.Get("mac")
	assert.False(t, ok)
}

func TestRegistryList(t *testing.T) {
	r := NewRegistry()
	r.Register(&Device{Hostname: "a"})
	r.Register(&Device{Hostname: "b"})
	got := r.List()
	assert.Len(t, got, 2)
}
