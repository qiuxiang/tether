package node

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/qiuxiang/tether/internal/forward"
	"github.com/qiuxiang/tether/internal/protocol"
)

const forwardReadBufSize = 32 * 1024

type forwardListener struct {
	ln        net.Listener
	forwardID string
	destHost  string
	destPort  int
}

type forwardStream struct {
	conn      net.Conn
	closeOnce sync.Once
}

// ForwardHandler manages TCP port-forwarding listeners and streams.
type ForwardHandler struct {
	mu         sync.Mutex
	listeners  map[string]net.Listener // forward_id or "L:"+rule.Raw → listener
	streams    map[string]*forwardStream
	rules      []forward.Rule
	byForward  map[string]forward.Rule // forward_id → R rule we own
	localAddrs map[string]string       // rule.Raw → bound addr
	streamMsg  map[string]string       // stream_id → msg_id for L flows
	closed     bool
}

// NewForwardHandler creates a new ForwardHandler.
func NewForwardHandler() *ForwardHandler {
	return &ForwardHandler{
		listeners:  map[string]net.Listener{},
		streams:    map[string]*forwardStream{},
		byForward:  map[string]forward.Rule{},
		localAddrs: map[string]string{},
		streamMsg:  map[string]string{},
	}
}

// InitRules stores the forwarding rules and assigns stable forward_ids to R rules.
func (h *ForwardHandler) InitRules(rules []forward.Rule) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rules = rules
	for _, r := range rules {
		if r.Dir == forward.DirRemote {
			h.byForward[newStreamID()] = r
		}
	}
}

// Start binds L listeners and emits forward_listen for R rules. Idempotent.
func (h *ForwardHandler) Start(ctx context.Context, send Sender) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	rules := append([]forward.Rule(nil), h.rules...)
	h.mu.Unlock()

	for _, r := range rules {
		switch r.Dir {
		case forward.DirLocal:
			h.startLocal(ctx, send, r)
		case forward.DirRemote:
			h.resendRemote(send, r)
		}
	}
}

// ResendListens re-emits forward_listen for all R rules.
func (h *ForwardHandler) ResendListens(send Sender) {
	h.mu.Lock()
	rules := append([]forward.Rule(nil), h.rules...)
	h.mu.Unlock()
	for _, r := range rules {
		if r.Dir == forward.DirRemote {
			h.resendRemote(send, r)
		}
	}
}

// OnDeviceOnline re-emits forward_listen for R rules targeting host.
func (h *ForwardHandler) OnDeviceOnline(host string, send Sender) {
	h.mu.Lock()
	rules := append([]forward.Rule(nil), h.rules...)
	h.mu.Unlock()
	for _, r := range rules {
		if r.Dir == forward.DirRemote && r.Device == host {
			h.resendRemote(send, r)
		}
	}
}

// OnReply closes the stream associated with a failed reply.
func (h *ForwardHandler) OnReply(_ Sender, m *protocol.Reply) {
	if m.OK {
		return
	}
	h.mu.Lock()
	var sid string
	for s, mid := range h.streamMsg {
		if mid == m.MsgID {
			sid = s
			break
		}
	}
	h.mu.Unlock()
	if sid == "" {
		return
	}
	h.closeStream(sid)
}

// LocalAddr returns the bound address for an L rule.
func (h *ForwardHandler) LocalAddr(r forward.Rule) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.localAddrs[r.Raw]
}

func (h *ForwardHandler) startLocal(ctx context.Context, send Sender, r forward.Rule) {
	h.mu.Lock()
	if _, dup := h.localAddrs[r.Raw]; dup {
		h.mu.Unlock()
		return
	}
	h.mu.Unlock()

	ln, err := net.Listen("tcp", r.ListenAddr())
	if err != nil {
		log.Printf("forward L %s: bind failed: %v", r.Raw, err)
		return
	}
	h.mu.Lock()
	h.listeners["L:"+r.Raw] = ln
	h.localAddrs[r.Raw] = ln.Addr().String()
	h.mu.Unlock()
	log.Printf("forward L %s: listening on %s", r.Raw, ln.Addr())

	go h.localAcceptLoop(ctx, send, r, ln)
}

func (h *ForwardHandler) localAcceptLoop(_ context.Context, send Sender, r forward.Rule, ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		sid := newStreamID()
		st := &forwardStream{conn: c}
		h.mu.Lock()
		h.streams[sid] = st
		h.streamMsg[sid] = sid
		h.mu.Unlock()
		_ = send.Send(&protocol.ForwardDial{
			MsgID: sid, Target: r.Device, StreamID: sid,
			DestHost: r.DestHost, DestPort: r.DestPort,
		})
		go h.readPump(send, sid, c)
	}
}

func (h *ForwardHandler) resendRemote(send Sender, r forward.Rule) {
	h.mu.Lock()
	var fid string
	for id, rr := range h.byForward {
		if rr.Raw == r.Raw {
			fid = id
			break
		}
	}
	h.mu.Unlock()
	if fid == "" {
		return
	}
	_ = send.Send(&protocol.ForwardListen{
		MsgID: fid, Target: r.Device, ForwardID: fid,
		ListenAddr: r.ListenAddr(),
		DestHost:   r.DestHost, DestPort: r.DestPort,
	})
}

// Listen opens a TCP listener on ListenAddr. On success it replies with
// Reply{OK:true, Data:{"listen_addr": <actual addr>}} and starts an accept loop.
func (h *ForwardHandler) Listen(send Sender, m *protocol.ForwardListen) {
	h.mu.Lock()
	if old, ok := h.listeners[m.ForwardID]; ok {
		// A listener for this forward_id already exists (e.g. duplicate
		// ForwardListen from OnDeviceOnline, or stale from a prior session).
		// Reply with the existing addr — idempotent and safe in both cases.
		addr := old.Addr().String()
		h.mu.Unlock()
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
			"listen_addr": addr,
		}})
		return
	}
	h.mu.Unlock()

	ln, err := net.Listen("tcp", m.ListenAddr)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: err.Error()})
		return
	}
	fl := &forwardListener{
		ln:        ln,
		forwardID: m.ForwardID,
		destHost:  m.DestHost,
		destPort:  m.DestPort,
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		ln.Close()
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "handler shut down"})
		return
	}
	h.listeners[m.ForwardID] = ln
	h.mu.Unlock()

	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true, Data: map[string]any{
		"listen_addr": ln.Addr().String(),
	}})

	go h.acceptLoop(send, fl)
}

func (h *ForwardHandler) acceptLoop(send Sender, fl *forwardListener) {
	for {
		conn, err := fl.ln.Accept()
		if err != nil {
			// Listener was closed (Unlisten or Shutdown)
			return
		}
		sid := newStreamID()
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			conn.Close()
			return
		}
		h.streams[sid] = &forwardStream{conn: conn}
		h.mu.Unlock()

		// Notify the other side that a new inbound connection arrived.
		send.Send(&protocol.ForwardDial{
			MsgID:     sid,
			ForwardID: fl.forwardID,
			StreamID:  sid,
			DestHost:  fl.destHost,
			DestPort:  fl.destPort,
		})

		go h.readPump(send, sid, conn)
	}
}

// Dial opens a TCP connection. If ForwardID matches an R rule we own, dial our
// configured destination; otherwise use m.DestHost:m.DestPort.
func (h *ForwardHandler) Dial(send Sender, m *protocol.ForwardDial) {
	h.mu.Lock()
	r, isDialBack := h.byForward[m.ForwardID]
	h.mu.Unlock()

	var addr string
	if isDialBack {
		addr = net.JoinHostPort(r.DestHost, strconv.Itoa(r.DestPort))
	} else {
		addr = fmt.Sprintf("%s:%d", m.DestHost, m.DestPort)
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "dial: " + err.Error()})
		send.Send(&protocol.ForwardClose{StreamID: m.StreamID, Half: "both"})
		return
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		conn.Close()
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "handler shut down"})
		return
	}
	h.streams[m.StreamID] = &forwardStream{conn: conn}
	h.mu.Unlock()

	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true})

	go h.readPump(send, m.StreamID, conn)
}

// Unlisten stops a listener by forward_id.
func (h *ForwardHandler) Unlisten(send Sender, m *protocol.ForwardUnlisten) {
	h.mu.Lock()
	ln, ok := h.listeners[m.ForwardID]
	if ok {
		delete(h.listeners, m.ForwardID)
	}
	h.mu.Unlock()

	if !ok {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "forward_id not found"})
		return
	}
	ln.Close()
	send.Send(&protocol.Reply{MsgID: m.MsgID, OK: true})
}

// Data writes bytes from the hub/client side into the local TCP connection.
func (h *ForwardHandler) Data(send Sender, m *protocol.ForwardData) {
	h.mu.Lock()
	fs, ok := h.streams[m.StreamID]
	h.mu.Unlock()
	if !ok {
		return
	}
	if _, err := fs.conn.Write(m.Data); err != nil {
		// Notify the peer that this stream is dead, then clean up locally.
		send.Send(&protocol.ForwardClose{StreamID: m.StreamID, Half: "both"})
		h.mu.Lock()
		if cur, still := h.streams[m.StreamID]; still && cur == fs {
			delete(h.streams, m.StreamID)
			delete(h.streamMsg, m.StreamID)
		}
		h.mu.Unlock()
		fs.conn.Close()
	}
}

// Close half-closes or fully closes a stream.
func (h *ForwardHandler) Close(_ Sender, m *protocol.ForwardClose) {
	h.mu.Lock()
	fs, ok := h.streams[m.StreamID]
	if ok && m.Half == "both" {
		delete(h.streams, m.StreamID)
	}
	h.mu.Unlock()
	if !ok {
		return
	}

	switch m.Half {
	case "read":
		if tc, ok := fs.conn.(*net.TCPConn); ok {
			tc.CloseRead()
		}
	case "write":
		if tc, ok := fs.conn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	default: // "both" or empty
		fs.conn.Close()
	}
}

// closeStream closes a stream by ID.
func (h *ForwardHandler) closeStream(sid string) {
	h.mu.Lock()
	fs, ok := h.streams[sid]
	if ok {
		delete(h.streams, sid)
		delete(h.streamMsg, sid)
	}
	h.mu.Unlock()
	if ok {
		fs.closeOnce.Do(func() { fs.conn.Close() })
	}
}

// Shutdown closes all listeners and streams. Idempotent.
func (h *ForwardHandler) Shutdown() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	lns := h.listeners
	streams := h.streams
	h.listeners = map[string]net.Listener{}
	h.streams = map[string]*forwardStream{}
	h.localAddrs = map[string]string{}
	h.mu.Unlock()

	for _, ln := range lns {
		ln.Close()
	}
	for _, st := range streams {
		st.closeOnce.Do(func() { st.conn.Close() })
	}
}

// readPump drains bytes from conn and emits ForwardData frames until EOF or error.
func (h *ForwardHandler) readPump(send Sender, sid string, conn net.Conn) {
	buf := make([]byte, forwardReadBufSize)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			send.Send(&protocol.ForwardData{StreamID: sid, Data: chunk})
		}
		if err != nil {
			// Remove stream entry if it's still ours.
			h.mu.Lock()
			fs, ok := h.streams[sid]
			if ok && fs.conn == conn {
				delete(h.streams, sid)
				delete(h.streamMsg, sid)
			}
			h.mu.Unlock()

			// io.EOF means clean close from remote → signal write-end closed.
			// Any other error → both sides closed.
			half := "both"
			if errors.Is(err, io.EOF) {
				half = "write"
			}
			send.Send(&protocol.ForwardClose{StreamID: sid, Half: half})
			return
		}
	}
}

func newStreamID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}
