package hub

import (
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
	// Conn is the active session for this hostname. Always non-nil while the
	// device is registered; replaced atomically on takeover (see Register).
	Conn PeerConn
}

type Registry struct {
	mu      sync.RWMutex
	devices map[string]*Device
}

func NewRegistry() *Registry {
	return &Registry{devices: make(map[string]*Device)}
}

// Register inserts d, replacing any existing entry for the same hostname.
// If a previous entry was displaced, its Conn is returned so the caller can
// close the stale session. This implements takeover semantics: a node that
// reconnects after a silent network drop is not blocked by the lingering
// registration of its previous TCP session.
func (r *Registry) Register(d *Device) (replaced PeerConn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.devices[d.Hostname]; ok {
		replaced = existing.Conn
	}
	r.devices[d.Hostname] = d
	return replaced
}

func (r *Registry) Unregister(hostname string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.devices, hostname)
}

// UnregisterIf removes the entry for hostname only if it is currently held by
// conn. Returns true if the entry was removed. Used by disconnect cleanup so a
// stale session that lost a takeover race does not evict the new owner.
func (r *Registry) UnregisterIf(hostname string, conn PeerConn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.devices[hostname]; ok && existing.Conn == conn {
		delete(r.devices, hostname)
		return true
	}
	return false
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
