package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"net"
	"sync"

	"github.com/qiuxiang/tether/internal/forward"
	"github.com/qiuxiang/tether/internal/protocol"
)

// Sender is satisfied by *Conn (which has Send(protocol.Message) error).
type Sender interface {
	Send(msg protocol.Message) error
}

// ForwardManager binds local TCP listeners for L rules and asks nodes to bind
// listeners for R rules, routing data between the hub and local connections.
type ForwardManager struct {
	sender Sender
	rules  []forward.Rule

	mu        sync.Mutex
	listeners map[string]net.Listener // forward_id → listener (L rules)
	streams   map[string]net.Conn     // stream_id → local conn
	byForward map[string]forward.Rule // forward_id → rule (R dial-backs)
	addrs     map[string]string       // rule.Raw → local bound addr (L rules, for tests)
}

// NewForwardManager creates a manager that routes traffic according to rules.
// s must satisfy Sender (e.g. *Conn).
func NewForwardManager(s Sender, rules []forward.Rule) *ForwardManager {
	return &ForwardManager{
		sender:    s,
		rules:     rules,
		listeners: map[string]net.Listener{},
		streams:   map[string]net.Conn{},
		byForward: map[string]forward.Rule{},
		addrs:     map[string]string{},
	}
}

func newForwardID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func newStreamID() string { return newForwardID() }

// Start binds L listeners and sends ForwardListen for R rules. Safe to call
// after device_online (re-issues ForwardListen for remote rules).
func (m *ForwardManager) Start(ctx context.Context) {
	for i := range m.rules {
		r := &m.rules[i]
		switch r.Dir {
		case forward.DirLocal:
			m.startLocal(ctx, r)
		case forward.DirRemote:
			m.startRemote(ctx, r)
		}
	}
}

func (m *ForwardManager) startLocal(ctx context.Context, r *forward.Rule) {
	fid := newForwardID()
	m.mu.Lock()
	m.byForward[fid] = *r
	m.mu.Unlock()

	ln, err := net.Listen("tcp", r.ListenAddr())
	if err != nil {
		log.Printf("forward L %s: bind failed: %v", r.Raw, err)
		return
	}
	m.mu.Lock()
	m.listeners[fid] = ln
	m.addrs[r.Raw] = ln.Addr().String()
	m.mu.Unlock()
	log.Printf("forward L %s: listening on %s", r.Raw, ln.Addr())

	go m.acceptLoop(ctx, fid, ln, *r)
}

func (m *ForwardManager) acceptLoop(_ context.Context, _ string, ln net.Listener, r forward.Rule) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		sid := newStreamID()
		m.mu.Lock()
		m.streams[sid] = c
		m.mu.Unlock()
		// Ask node to dial the destination.
		if err := m.sender.Send(&protocol.ForwardDial{
			MsgID:    sid,
			Target:   r.Device,
			StreamID: sid,
			DestHost: r.DestHost,
			DestPort: r.DestPort,
		}); err != nil {
			m.closeStream(sid)
			continue
		}
		go m.readPump(sid, c, r.Device)
	}
}

func (m *ForwardManager) startRemote(_ context.Context, r *forward.Rule) {
	fid := newForwardID()
	m.mu.Lock()
	m.byForward[fid] = *r
	m.mu.Unlock()
	_ = m.sender.Send(&protocol.ForwardListen{
		MsgID:      fid,
		Target:     r.Device,
		ForwardID:  fid,
		ListenAddr: r.ListenAddr(),
		DestHost:   r.DestHost,
		DestPort:   r.DestPort,
	})
}

// LocalAddr returns the bound address for an L rule (useful in tests with port 0).
func (m *ForwardManager) LocalAddr(r forward.Rule) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.addrs[r.Raw]
}

// Deliver dispatches a forward frame received from the hub/RPC layer.
func (m *ForwardManager) Deliver(msg protocol.Message) {
	switch v := msg.(type) {
	case *protocol.ForwardDial:
		// R direction: node accepted a conn, asks us to dial locally.
		m.mu.Lock()
		r, ok := m.byForward[v.ForwardID]
		m.mu.Unlock()
		if !ok {
			_ = m.sender.Send(&protocol.Reply{MsgID: v.MsgID, OK: false, Error: "unknown forward_id"})
			return
		}
		c, err := net.Dial("tcp", r.DestAddr())
		if err != nil {
			_ = m.sender.Send(&protocol.Reply{MsgID: v.MsgID, OK: false, Error: "dial: " + err.Error()})
			// Also close the node-side conn that's waiting for this dial-back.
			_ = m.sender.Send(&protocol.ForwardClose{Target: r.Device, StreamID: v.StreamID, Half: "both"})
			return
		}
		m.mu.Lock()
		m.streams[v.StreamID] = c
		m.mu.Unlock()
		_ = m.sender.Send(&protocol.Reply{MsgID: v.MsgID, OK: true})
		go m.readPump(v.StreamID, c, r.Device)

	case *protocol.ForwardData:
		m.mu.Lock()
		c := m.streams[v.StreamID]
		m.mu.Unlock()
		if c == nil {
			return
		}
		if _, err := c.Write(v.Data); err != nil {
			m.closeStream(v.StreamID)
		}

	case *protocol.ForwardClose:
		switch v.Half {
		case "write":
			m.mu.Lock()
			c := m.streams[v.StreamID]
			m.mu.Unlock()
			if tcp, ok := c.(*net.TCPConn); ok {
				_ = tcp.CloseWrite()
			}
		case "read":
			m.mu.Lock()
			c := m.streams[v.StreamID]
			m.mu.Unlock()
			if tcp, ok := c.(*net.TCPConn); ok {
				_ = tcp.CloseRead()
			}
		default: // "both" or empty
			m.closeStream(v.StreamID)
		}

	case *protocol.Event:
		if v.Kind == "device_online" {
			m.onDeviceOnline(v.Device)
		}

	case *protocol.Reply:
		if !v.OK {
			// Dial/bind failure — close any matching stream identified by msg_id.
			m.closeStream(v.MsgID)
		}
	}
}

// onDeviceOnline re-issues ForwardListen for every R rule targeting device.
func (m *ForwardManager) onDeviceOnline(device string) {
	for _, r := range m.rules {
		if r.Dir == forward.DirRemote && r.Device == device {
			r := r
			m.startRemote(context.Background(), &r)
		}
	}
}

func (m *ForwardManager) readPump(sid string, c net.Conn, device string) {
	buf := make([]byte, 32*1024)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			_ = m.sender.Send(&protocol.ForwardData{
				Target:   device,
				StreamID: sid,
				Data:     chunk,
			})
		}
		if err != nil {
			half := "both"
			if errors.Is(err, io.EOF) {
				half = "write"
			}
			_ = m.sender.Send(&protocol.ForwardClose{Target: device, StreamID: sid, Half: half})
			m.closeStream(sid)
			return
		}
	}
}

func (m *ForwardManager) closeStream(sid string) {
	m.mu.Lock()
	c := m.streams[sid]
	delete(m.streams, sid)
	m.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

// Stop closes all listeners and streams. Idempotent.
func (m *ForwardManager) Stop() {
	m.mu.Lock()
	lns := m.listeners
	streams := m.streams
	m.listeners = map[string]net.Listener{}
	m.streams = map[string]net.Conn{}
	m.mu.Unlock()
	for _, ln := range lns {
		ln.Close()
	}
	for _, c := range streams {
		c.Close()
	}
}
