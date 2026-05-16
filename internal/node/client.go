package node

import (
	"context"
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

type Client struct {
	mu   sync.Mutex
	cfg  Config
	conn *websocket.Conn
	url  string
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

	// Read loop — Task 6 adds dispatch.
	for {
		_, _, err := conn.Read(ctx)
		if err != nil {
			return err
		}
	}
}
