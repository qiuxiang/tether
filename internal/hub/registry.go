package hub

import (
	"errors"
	"sync"
	"time"
)

// PeerConn is a generic send-back interface implemented by both nodes
// (device sessions) and clients (client sessions).
type PeerConn interface {
	SendRaw(raw []byte) error
	Close()
}

// DeviceConn is kept as an alias for backwards compatibility within the package.
// New code should use PeerConn.
type DeviceConn = PeerConn

type Device struct {
	Hostname     string
	OS           string
	Arch         string
	AgentVersion string
	ConnectedAt  time.Time
	LastSeen     time.Time
	// Conn is set by device_ws layer; nil if offline (but we Unregister on disconnect so this is always non-nil in registry)
	Conn PeerConn
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

type Client struct {
	ID          string
	ConnectedAt time.Time
	Conn        PeerConn
}

type ClientRegistry struct {
	mu      sync.RWMutex
	clients map[string]*Client
}

func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{clients: make(map[string]*Client)}
}

func (r *ClientRegistry) Register(c *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[c.ID] = c
}

func (r *ClientRegistry) Unregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, id)
}

func (r *ClientRegistry) List() []*Client {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Client, 0, len(r.clients))
	for _, c := range r.clients {
		out = append(out, c)
	}
	return out
}
