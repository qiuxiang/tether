package hub

import (
	"errors"
	"sync"
	"time"
)

type Device struct {
	Hostname     string
	OS           string
	Arch         string
	AgentVersion string
	ConnectedAt  time.Time
	LastSeen     time.Time
	// conn is set by device_ws layer; nil if offline (but we Unregister on disconnect so this is always non-nil in registry)
	Conn DeviceConn
}

// DeviceConn is implemented by hub.deviceSession; declared here so Registry doesn't import device_ws.
type DeviceConn interface {
	Send(msg any) error
	Close()
}

type Registry struct {
	mu      sync.RWMutex
	devices map[string]*Device
}

func NewRegistry() *Registry {
	return &Registry{devices: make(map[string]*Device)}
}

func (r *Registry) Register(d *Device) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.devices[d.Hostname]; exists {
		return errors.New("hostname already registered")
	}
	r.devices[d.Hostname] = d
	return nil
}

func (r *Registry) Unregister(hostname string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.devices, hostname)
}

func (r *Registry) Get(hostname string) (*Device, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.devices[hostname]
	return d, ok
}

func (r *Registry) List() []*Device {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Device, 0, len(r.devices))
	for _, d := range r.devices {
		out = append(out, d)
	}
	return out
}
