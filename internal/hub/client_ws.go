package hub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/qiuxiang/tether/internal/protocol"
)

func newClientID() string { return protocol.NewID(8) }

func (s *Server) handleClient(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
		CompressionMode:    websocket.CompressionContextTakeover,
	})
	if err != nil {
		return
	}
	c.SetReadLimit(protocol.WSReadLimit)
	ctx := r.Context()

	hello, err := s.readHello(ctx, c)
	if err != nil {
		log.Printf("client handshake failed: %v", err)
		c.Close(websocket.StatusPolicyViolation, err.Error())
		return
	}
	if hello.Role != "client" {
		c.Close(websocket.StatusPolicyViolation, "role must be client")
		return
	}

	id := newClientID()
	sess := &clientSession{id: id, conn: c, server: s, pending: make(map[string]struct{})}
	s.clients.Register(&Client{ID: id, ConnectedAt: time.Now(), Conn: sess})
	log.Printf("client registered: id=%s", id)

	defer func() {
		log.Printf("client disconnected: id=%s", id)
		s.clients.Unregister(id)
		sess.mu.Lock()
		for msgID := range sess.pending {
			s.router.Unregister(msgID)
		}
		sess.mu.Unlock()
		c.Close(websocket.StatusNormalClosure, "")
	}()
	sess.run(ctx)
}

type clientSession struct {
	id     string
	conn   *websocket.Conn
	server *Server
	mu     sync.Mutex
	// pending tracks sticky-route msg_ids registered by this client so they can
	// be cleaned from the global router when the client disconnects.
	// TODO: entries here are not removed on normal stream completion, so long-
	// lived clients accumulate stale entries. Acceptable for now (each entry is
	// ~16 bytes; thousands of execs ≈ tens of KB) but should be plumbed through
	// device_ws.run() in a follow-up.
	pending map[string]struct{}
}

func (cs *clientSession) SendRaw(raw []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return cs.conn.Write(ctx, websocket.MessageBinary, raw)
}

func (cs *clientSession) Close() { cs.conn.Close(websocket.StatusNormalClosure, "") }

func (cs *clientSession) run(ctx context.Context) {
	for {
		_, raw, err := cs.conn.Read(ctx)
		if err != nil {
			return
		}
		msg, err := protocol.Decode(raw)
		if err != nil {
			log.Printf("client %s decode: %v", cs.id, err)
			continue
		}
		cs.dispatch(raw, msg)
	}
}

// dispatch handles a single client→hub message: either a hub-local op (answer
// inline) or a routable request (register msg_id route and forward to target).
func (cs *clientSession) dispatch(raw []byte, msg protocol.Message) {
	switch m := msg.(type) {
	case *protocol.ListDevices:
		cs.replyListDevices(m.MsgID)
	case *protocol.Exec:
		cs.routeOneShot(m.MsgID, m.Target, raw)
	case *protocol.FilePutOpen:
		cs.routeFileStream(m.MsgID, m.Target, raw)
	case *protocol.FileGetOpen:
		cs.routeFileStream(m.MsgID, m.Target, raw)
	case *protocol.FileLocalCopy:
		cs.routeOneShot(m.MsgID, m.Target, raw)
	case *protocol.FileChunk:
		// Client pushing a chunk to a node mid-upload — forward by msg_id.
		cs.server.router.ForwardToNode(m.MsgID, raw)
	case *protocol.FileAbort:
		cs.server.router.ForwardToNode(m.MsgID, raw)
		cs.server.router.Unregister(m.MsgID)
	case *protocol.FileRelay:
		if err := cs.server.relay.Start(cs, m); err != nil {
			cs.sendErrorReply(m.MsgID, err)
		}
	case *protocol.ReadFileReq:
		cs.routeOneShot(m.MsgID, m.Target, raw)
	case *protocol.WriteFileReq:
		cs.routeOneShot(m.MsgID, m.Target, raw)
	case *protocol.EditFileReq:
		cs.routeOneShot(m.MsgID, m.Target, raw)
	default:
		// Unknown / not-routable from client: drop.
	}
}

func (cs *clientSession) replyListDevices(msgID string) {
	list := cs.server.registry.List()
	items := make([]any, 0, len(list))
	for _, d := range list {
		items = append(items, map[string]any{
			"hostname":      d.Hostname,
			"os":            d.OS,
			"arch":          d.Arch,
			"agent_version": d.AgentVersion,
			"online":        d.Conn != nil,
			"last_seen":     d.LastSeen.Unix(),
		})
	}
	reply := &protocol.Reply{MsgID: msgID, OK: true, Data: map[string]any{"devices": items}}
	out, err := protocol.Encode(reply)
	if err != nil {
		return
	}
	_ = cs.SendRaw(out)
}

func (cs *clientSession) routeOneShot(msgID, target string, raw []byte) {
	if err := cs.routeTo(msgID, target, raw, false); err != nil {
		cs.sendErrorReply(msgID, err)
	}
}

func (cs *clientSession) routeTo(msgID, target string, raw []byte, sticky bool) error {
	if target == "" {
		return errors.New("missing target")
	}
	d, ok := cs.server.registry.Get(target)
	if !ok || d.Conn == nil {
		return fmt.Errorf("device_offline: %s", target)
	}
	cs.server.router.Register(msgID, cs, sticky)
	if sticky {
		cs.trackPending(msgID)
	}
	if err := d.Conn.SendRaw(raw); err != nil {
		cs.server.router.Unregister(msgID)
		if sticky {
			cs.untrackPending(msgID)
		}
		return err
	}
	return nil
}

func (cs *clientSession) sendErrorReply(msgID string, err error) {
	reply := &protocol.Reply{MsgID: msgID, OK: false, Error: err.Error()}
	out, encErr := protocol.Encode(reply)
	if encErr != nil {
		return
	}
	_ = cs.SendRaw(out)
}

func (cs *clientSession) trackPending(msgID string) {
	cs.mu.Lock()
	cs.pending[msgID] = struct{}{}
	cs.mu.Unlock()
}

func (cs *clientSession) untrackPending(msgID string) {
	cs.mu.Lock()
	delete(cs.pending, msgID)
	cs.mu.Unlock()
}

// routeFileStream sets up a sticky bidirectional route for a file put/get:
// Reply frames flow node→client, while FileChunk/FileAbort frames flow
// client→node (RegisterNode). Sticky until EOF; tracked so a client
// disconnect unregisters the route.
func (cs *clientSession) routeFileStream(msgID, target string, raw []byte) {
	d, ok := cs.server.registry.Get(target)
	if !ok || d.Conn == nil {
		cs.sendErrorReply(msgID, fmt.Errorf("device_offline: %s", target))
		return
	}
	cs.server.router.Register(msgID, cs, true)
	cs.server.router.RegisterNode(msgID, d.Conn)
	cs.trackPending(msgID)
	if err := d.Conn.SendRaw(raw); err != nil {
		cs.server.router.Unregister(msgID)
		cs.untrackPending(msgID)
		cs.sendErrorReply(msgID, err)
	}
}
