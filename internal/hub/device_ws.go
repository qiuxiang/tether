package hub

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/qiuxiang/tether/internal/protocol"
)

func (s *Server) handleDevice(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	ctx := r.Context()

	sess, err := s.handshake(ctx, c)
	if err != nil {
		log.Printf("device handshake failed: %v", err)
		c.Close(websocket.StatusPolicyViolation, err.Error())
		return
	}
	log.Printf("device registered: hostname=%s os=%s arch=%s", sess.device.Hostname, sess.device.OS, sess.device.Arch)
	defer func() {
		log.Printf("device disconnected: hostname=%s", sess.device.Hostname)
		s.registry.Unregister(sess.device.Hostname)
		c.Close(websocket.StatusNormalClosure, "")
	}()

	sess.run(ctx)
}

type deviceSession struct {
	device *Device
	conn   *websocket.Conn
	router *Router
}

// readHello reads the initial Hello frame and validates the shared token.
func (s *Server) readHello(ctx context.Context, c *websocket.Conn) (*protocol.Hello, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	if err != nil {
		return nil, err
	}
	msg, err := protocol.Decode(data)
	if err != nil {
		return nil, err
	}
	hello, ok := msg.(*protocol.Hello)
	if !ok {
		return nil, errAuth("first message must be hello")
	}
	if hello.Token != s.opts.Token {
		return nil, errAuth("bad token")
	}
	return hello, nil
}

func (s *Server) handshake(ctx context.Context, c *websocket.Conn) (*deviceSession, error) {
	hello, err := s.readHello(ctx, c)
	if err != nil {
		return nil, err
	}
	if hello.Role != "" && hello.Role != "node" {
		return nil, errAuth("role must be node or empty")
	}
	d := &Device{
		Hostname:     hello.Hostname,
		OS:           hello.OS,
		Arch:         hello.Arch,
		AgentVersion: hello.AgentVersion,
		ConnectedAt:  time.Now(),
		LastSeen:     time.Now(),
	}
	sess := &deviceSession{device: d, conn: c, router: s.router}
	d.Conn = sess
	if err := s.registry.Register(d); err != nil {
		return nil, err
	}
	return sess, nil
}

func (s *deviceSession) SendRaw(raw []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.conn.Write(ctx, websocket.MessageBinary, raw)
}

// Send is retained for in-process call sites in hub package; encodes then
// forwards to SendRaw.
func (s *deviceSession) Send(msg any) error {
	m, ok := msg.(protocol.Message)
	if !ok {
		return errAuth("not a protocol.Message")
	}
	raw, err := protocol.Encode(m)
	if err != nil {
		return err
	}
	return s.SendRaw(raw)
}

func (s *deviceSession) run(ctx context.Context) {
	for {
		_, raw, err := s.conn.Read(ctx)
		if err != nil {
			return
		}
		msg, err := protocol.Decode(raw)
		if err != nil {
			log.Printf("decode from %s: %v", s.device.Hostname, err)
			continue
		}
		s.device.LastSeen = time.Now()
		id := msgID(msg)
		if id != "" {
			s.router.ForwardToClient(id, raw)
			switch v := msg.(type) {
			case *protocol.ExecExit:
				s.router.Unregister(id)
			case *protocol.FileChunk:
				if v.EOF {
					s.router.Unregister(id)
				}
			case *protocol.FileAbort:
				s.router.Unregister(id)
			}
		}
	}
}

// msgID extracts MsgID from messages that carry one (returns "" otherwise).
func msgID(m protocol.Message) string {
	switch v := m.(type) {
	case *protocol.Reply:
		return v.MsgID
	case *protocol.ExecOutput:
		return v.MsgID
	case *protocol.ExecExit:
		return v.MsgID
	case *protocol.FileChunk:
		return v.MsgID
	case *protocol.FileAbort:
		return v.MsgID
	}
	return ""
}

func (s *deviceSession) Close() { s.conn.Close(websocket.StatusNormalClosure, "") }

type authError struct{ msg string }

func (e authError) Error() string { return e.msg }
func errAuth(msg string) error    { return authError{msg} }
