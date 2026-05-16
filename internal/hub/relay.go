package hub

import (
	"fmt"
	"sync"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
)

// RelayCoordinator orchestrates node↔node file transfers initiated by a
// client via FileRelay.
type RelayCoordinator struct {
	server *Server

	mu       sync.Mutex
	inflight map[string]*relayState // keyed by outer (client) msg_id
}

type relayState struct {
	clientMsgID string
	getMsgID    string
	putMsgID    string
	fromConn    PeerConn
	toConn      PeerConn
	client      PeerConn
	metaReady   chan *protocol.Reply
	putReady    chan *protocol.Reply
	sourceItcp  *relaySourceInterceptor
}

func NewRelayCoordinator(s *Server) *RelayCoordinator {
	return &RelayCoordinator{server: s, inflight: make(map[string]*relayState)}
}

// Start kicks off a relay. Returns an error to be reported as Reply{ok:false}
// on immediate failure; on success it replies asynchronously via the routes.
func (rc *RelayCoordinator) Start(client PeerConn, m *protocol.FileRelay) error {
	from, ok := rc.server.registry.Get(m.FromNode)
	if !ok || from.Conn == nil {
		return fmt.Errorf("device_offline: %s", m.FromNode)
	}
	to, ok := rc.server.registry.Get(m.ToNode)
	if !ok || to.Conn == nil {
		return fmt.Errorf("device_offline: %s", m.ToNode)
	}

	getID := newClientID()
	putID := newClientID()
	st := &relayState{
		clientMsgID: m.MsgID,
		getMsgID:    getID,
		putMsgID:    putID,
		fromConn:    from.Conn,
		toConn:      to.Conn,
		client:      client,
		metaReady:   make(chan *protocol.Reply, 1),
		putReady:    make(chan *protocol.Reply, 1),
	}
	rc.mu.Lock()
	rc.inflight[m.MsgID] = st
	rc.mu.Unlock()

	// Hook routes:
	//   - source side: combined interceptor captures metadata Reply and buffers
	//     FileChunk/FileAbort frames until the destination is ready.
	//   - first Reply for putID lands in putReady (then re-registered for final)
	sourceItcp := &relaySourceInterceptor{
		metaReady: st.metaReady,
		toConn:    st.toConn,
	}
	rc.server.router.Register(getID, sourceItcp, true)
	rc.server.router.Register(putID, &replyInterceptor{ch: st.putReady}, false)
	st.sourceItcp = sourceItcp

	rawGet, _ := protocol.Encode(&protocol.FileGetOpen{MsgID: getID, Path: m.FromPath})
	if err := from.Conn.SendRaw(rawGet); err != nil {
		rc.cleanup(m.MsgID)
		return err
	}

	go rc.coordinate(m, st)
	return nil
}

func (rc *RelayCoordinator) coordinate(m *protocol.FileRelay, st *relayState) {
	// Wait for metadata.
	meta := <-st.metaReady
	if meta == nil || !meta.OK {
		rc.failClient(st.clientMsgID, meta)
		rc.cleanup(st.clientMsgID)
		return
	}
	var size int64
	if v, ok := meta.Data["size"].(int64); ok {
		size = v
	} else if v, ok := meta.Data["size"].(uint64); ok {
		size = int64(v)
	}
	var mode uint32
	if v, ok := meta.Data["mode"].(uint64); ok {
		mode = uint32(v)
	}

	rawPut, _ := protocol.Encode(&protocol.FilePutOpen{
		MsgID: st.putMsgID, Path: m.ToPath, Size: size, Mode: mode, Overwrite: m.Overwrite,
	})
	if err := st.toConn.SendRaw(rawPut); err != nil {
		rc.failClient(st.clientMsgID, &protocol.Reply{Error: err.Error()})
		rc.cleanup(st.clientMsgID)
		return
	}

	// Wait for put_ready.
	var ready *protocol.Reply
	select {
	case ready = <-st.putReady:
	case <-time.After(30 * time.Second):
		rc.failClient(st.clientMsgID, &protocol.Reply{Error: "put_open_timeout"})
		rc.cleanup(st.clientMsgID)
		return
	}
	if ready == nil || !ready.OK {
		rc.failClient(st.clientMsgID, ready)
		rc.cleanup(st.clientMsgID)
		return
	}

	// Unblock the source interceptor: it can now rewrite and forward buffered
	// and future FileChunk/FileAbort frames directly to toConn.
	st.sourceItcp.SetDestination(st.putMsgID)
	// Register the final-reply route so the destination's completion Reply
	// is forwarded back to the client under the client's outer msg_id.
	rc.server.router.Register(st.putMsgID, &finalDeliverer{
		client:      st.client,
		clientMsgID: st.clientMsgID,
		onDone:      func() { rc.cleanup(st.clientMsgID) },
	}, false)
}

func (rc *RelayCoordinator) cleanup(clientMsgID string) {
	rc.mu.Lock()
	st, ok := rc.inflight[clientMsgID]
	delete(rc.inflight, clientMsgID)
	rc.mu.Unlock()
	if ok {
		rc.server.router.Unregister(st.getMsgID)
		rc.server.router.Unregister(st.putMsgID)
	}
}

func (rc *RelayCoordinator) failClient(clientMsgID string, r *protocol.Reply) {
	errStr := "relay failed"
	if r != nil && r.Error != "" {
		errStr = r.Error
	}
	raw, _ := protocol.Encode(&protocol.Reply{MsgID: clientMsgID, OK: false, Error: errStr})
	rc.mu.Lock()
	st, ok := rc.inflight[clientMsgID]
	rc.mu.Unlock()
	if ok {
		_ = st.client.SendRaw(raw)
	}
}

// replyInterceptor implements PeerConn so the router can deliver a Reply
// to a channel inside the relay coordinator.
type replyInterceptor struct {
	ch chan *protocol.Reply
}

func (i *replyInterceptor) SendRaw(raw []byte) error {
	msg, err := protocol.Decode(raw)
	if err != nil {
		return err
	}
	if r, ok := msg.(*protocol.Reply); ok {
		select {
		case i.ch <- r:
		default:
		}
	}
	return nil
}
func (i *replyInterceptor) Close() {}

// relaySourceInterceptor handles the source side of a FileRelay. The first
// Reply goes to metaReady; FileChunk/FileAbort frames are rewritten with
// toMsgID and forwarded to toConn. Frames that arrive before SetDestination
// has been called are buffered so they are not lost.
type relaySourceInterceptor struct {
	mu        sync.Mutex
	metaReady chan *protocol.Reply
	toConn    PeerConn
	toMsgID   string   // empty until SetDestination called
	buffered  [][]byte // raw FileChunk/FileAbort frames waiting for destination
}

func (r *relaySourceInterceptor) SetDestination(toMsgID string) {
	r.mu.Lock()
	r.toMsgID = toMsgID
	pending := r.buffered
	r.buffered = nil
	r.mu.Unlock()
	// Drain buffered frames outside the lock.
	for _, raw := range pending {
		_ = r.forwardFrame(raw)
	}
}

func (r *relaySourceInterceptor) SendRaw(raw []byte) error {
	msg, err := protocol.Decode(raw)
	if err != nil {
		return err
	}
	switch m := msg.(type) {
	case *protocol.Reply:
		select {
		case r.metaReady <- m:
		default:
		}
		return nil
	case *protocol.FileChunk, *protocol.FileAbort:
		r.mu.Lock()
		ready := r.toMsgID != ""
		if !ready {
			r.buffered = append(r.buffered, append([]byte(nil), raw...))
		}
		r.mu.Unlock()
		if ready {
			return r.forwardFrame(raw)
		}
		return nil
	}
	return nil
}

func (r *relaySourceInterceptor) Close() {}

// forwardFrame decodes the raw frame, rewrites MsgID to toMsgID, re-encodes,
// and sends to toConn. Only called after toMsgID is set.
func (r *relaySourceInterceptor) forwardFrame(raw []byte) error {
	msg, err := protocol.Decode(raw)
	if err != nil {
		return err
	}
	switch m := msg.(type) {
	case *protocol.FileChunk:
		m.MsgID = r.toMsgID
		out, err := protocol.Encode(m)
		if err != nil {
			return err
		}
		return r.toConn.SendRaw(out)
	case *protocol.FileAbort:
		m.MsgID = r.toMsgID
		out, _ := protocol.Encode(m)
		return r.toConn.SendRaw(out)
	}
	return nil
}

// chunkRewriter rewrites incoming FileChunk/FileAbort msg_ids and forwards
// them to the destination node.
type chunkRewriter struct {
	toConn  PeerConn
	toMsgID string
}

func (cr *chunkRewriter) SendRaw(raw []byte) error {
	msg, err := protocol.Decode(raw)
	if err != nil {
		return err
	}
	switch m := msg.(type) {
	case *protocol.FileChunk:
		m.MsgID = cr.toMsgID
		out, err := protocol.Encode(m)
		if err != nil {
			return err
		}
		return cr.toConn.SendRaw(out)
	case *protocol.FileAbort:
		m.MsgID = cr.toMsgID
		out, _ := protocol.Encode(m)
		return cr.toConn.SendRaw(out)
	}
	return nil
}
func (cr *chunkRewriter) Close() {}

// finalDeliverer forwards the final upload Reply back to the originating
// client under the client's outer msg_id.
type finalDeliverer struct {
	client      PeerConn
	clientMsgID string
	onDone      func()
}

func (fd *finalDeliverer) SendRaw(raw []byte) error {
	defer fd.onDone()
	msg, err := protocol.Decode(raw)
	if err != nil {
		return err
	}
	if r, ok := msg.(*protocol.Reply); ok {
		r.MsgID = fd.clientMsgID
		out, _ := protocol.Encode(r)
		return fd.client.SendRaw(out)
	}
	return nil
}
func (fd *finalDeliverer) Close() {}
