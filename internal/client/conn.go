package client

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/qiuxiang/tether/internal/protocol"
)

// Conn maintains a WSS connection to the hub and demultiplexes incoming
// messages by msg_id into pending requests / streams (see rpc.go).
type Conn struct {
	cfg   Config
	rpc   *RPC

	mu    sync.Mutex
	ws    *websocket.Conn
	ready chan struct{} // closed on each successful (re)connect
}

func NewConn(cfg Config) *Conn {
	return &Conn{cfg: cfg, rpc: NewRPC(), ready: make(chan struct{})}
}

func (c *Conn) RPC() *RPC { return c.rpc }

func (c *Conn) WaitReady(ctx context.Context) error {
	c.mu.Lock()
	ready := c.ready
	c.mu.Unlock()
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Conn) Send(msg protocol.Message) error {
	raw, err := protocol.Encode(msg)
	if err != nil {
		return err
	}
	c.mu.Lock()
	ws := c.ws
	c.mu.Unlock()
	if ws == nil {
		return errors.New("not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ws.Write(ctx, websocket.MessageBinary, raw)
}

func (c *Conn) Run(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := c.dial(ctx); err != nil {
			log.Printf("hub connection lost: %v", err)
		}
		// Reset ready signal for next dial.
		c.mu.Lock()
		c.ws = nil
		c.ready = make(chan struct{})
		c.mu.Unlock()
		wait := backoff + time.Duration(rand.Int63n(int64(backoff/2+1)))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (c *Conn) dial(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(dialCtx, c.cfg.HubURL, &websocket.DialOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	})
	if err != nil {
		return err
	}
	ws.SetReadLimit(protocol.WSReadLimit)
	defer ws.Close(websocket.StatusNormalClosure, "")

	host, _ := os.Hostname()
	hello := &protocol.Hello{
		Hostname:     host,
		Token:        c.cfg.Token,
		Role:         "client",
		AgentVersion: "0.1.0",
	}
	raw, _ := protocol.Encode(hello)
	if err := ws.Write(ctx, websocket.MessageBinary, raw); err != nil {
		return err
	}

	c.mu.Lock()
	c.ws = ws
	close(c.ready)
	c.mu.Unlock()

	// Keep the connection alive across idle reverse-proxy timeouts.
	go pingLoop(ctx, ws)

	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			return err
		}
		msg, err := protocol.Decode(data)
		if err != nil {
			log.Printf("decode: %v", err)
			continue
		}
		c.rpc.Deliver(msg)
	}
}

// pingLoop sends RFC 6455 Ping frames on a 45s interval so reverse proxies
// (which typically idle-out a WS conn after 2-3 min) don't drop the link.
// On ping failure or timeout, it closes the conn — the outer reconnect loop
// dials again on next tick.
func pingLoop(ctx context.Context, ws *websocket.Conn) {
	t := time.NewTicker(45 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := ws.Ping(pctx)
			cancel()
			if err != nil {
				ws.Close(websocket.StatusPolicyViolation, "ping timeout")
				return
			}
		}
	}
}
