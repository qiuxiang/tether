package node

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"

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
	conn net.Conn
}

// ForwardHandler manages TCP port-forwarding listeners and streams.
type ForwardHandler struct {
	mu        sync.Mutex
	listeners map[string]*forwardListener // forward_id → listener
	streams   map[string]*forwardStream   // stream_id → stream
	closed    bool
}

// NewForwardHandler creates a new ForwardHandler.
func NewForwardHandler() *ForwardHandler {
	return &ForwardHandler{
		listeners: make(map[string]*forwardListener),
		streams:   make(map[string]*forwardStream),
	}
}

// Listen opens a TCP listener on ListenAddr. On success it replies with
// Reply{OK:true, Data:{"listen_addr": <actual addr>}} and starts an accept loop.
func (h *ForwardHandler) Listen(send Sender, m *protocol.ForwardListen) {
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
	h.listeners[m.ForwardID] = fl
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

// Dial opens a TCP connection to DestHost:DestPort and attaches it as a stream.
func (h *ForwardHandler) Dial(send Sender, m *protocol.ForwardDial) {
	addr := fmt.Sprintf("%s:%d", m.DestHost, m.DestPort)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "dial: " + err.Error()})
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
	fl, ok := h.listeners[m.ForwardID]
	if ok {
		delete(h.listeners, m.ForwardID)
	}
	h.mu.Unlock()

	if !ok {
		send.Send(&protocol.Reply{MsgID: m.MsgID, OK: false, Error: "forward_id not found"})
		return
	}
	fl.ln.Close()
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

// Shutdown closes all listeners and streams. Idempotent.
func (h *ForwardHandler) Shutdown() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for _, fl := range h.listeners {
		fl.ln.Close()
	}
	for _, fs := range h.streams {
		fs.conn.Close()
	}
	h.listeners = make(map[string]*forwardListener)
	h.streams = make(map[string]*forwardStream)
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
