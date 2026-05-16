package node

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/qiuxiang/tether/internal/protocol"
)

type Config struct {
	HubURL       string
	Token        string
	Hostname     string
	AgentVersion string
	ReconnectMin time.Duration
	ReconnectMax time.Duration
}

type Handler interface {
	Handle(ctx context.Context, send Sender, msg protocol.Message)
}

type Sender interface {
	Send(msg protocol.Message) error
}

type Client struct {
	mu      sync.Mutex
	cfg     Config
	conn    *websocket.Conn
	url     string
	handler Handler
}

func New(cfg Config) *Client {
	if cfg.ReconnectMin == 0 {
		cfg.ReconnectMin = time.Second
	}
	if cfg.ReconnectMax == 0 {
		cfg.ReconnectMax = 30 * time.Second
	}
	if cfg.AgentVersion == "" {
		cfg.AgentVersion = "0.1.0"
	}
	return &Client{cfg: cfg, url: cfg.HubURL}
}

func (c *Client) SetURL(url string) {
	c.mu.Lock()
	c.url = url
	c.mu.Unlock()
}

func (c *Client) SetHandler(h Handler) { c.handler = h }

// Send implements the Sender interface — handlers use it to reply to the hub.
func (c *Client) Send(msg protocol.Message) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return errors.New("not connected")
	}
	data, err := protocol.Encode(msg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return conn.Write(ctx, websocket.MessageBinary, data)
}

func (c *Client) Run(ctx context.Context) {
	backoff := c.cfg.ReconnectMin
	for {
		if ctx.Err() != nil {
			return
		}
		if err := c.connectAndServe(ctx); err != nil {
			log.Printf("connection lost: %v", err)
		}
		// Jittered exponential backoff.
		wait := backoff + time.Duration(rand.Int63n(int64(backoff/2+1)))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		backoff = min(backoff*2, c.cfg.ReconnectMax)
	}
}

func (c *Client) connectAndServe(ctx context.Context) error {
	c.mu.Lock()
	url := c.url
	c.mu.Unlock()

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dialCtx, url, nil)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	hello := &protocol.Hello{
		Hostname:     c.cfg.Hostname,
		OS:           runtime.GOOS,
		Arch:         runtime.GOARCH,
		AgentVersion: c.cfg.AgentVersion,
		Token:        c.cfg.Token,
	}
	data, _ := protocol.Encode(hello)
	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		msg, err := protocol.Decode(data)
		if err != nil {
			log.Printf("decode: %v", err)
			continue
		}
		if c.handler != nil {
			c.handler.Handle(ctx, c, msg)
		}
	}
}
