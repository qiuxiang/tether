package client

import (
	"context"
	"io"

	"github.com/mark3labs/mcp-go/server"
)

// Server wires the WSS Conn into an MCP stdio server.
type Server struct {
	conn *Conn
	mcp  *server.MCPServer
}

func NewMCPServer(c *Conn) *Server {
	s := &Server{conn: c, mcp: server.NewMCPServer("tether", "0.1.0")}
	registerExecTools(s.mcp, c)
	registerFileTool(s.mcp, c)
	registerEditTools(s.mcp, c)
	return s
}

// Serve runs the stdio MCP loop. Returns when stdin closes or ctx is done.
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	return server.NewStdioServer(s.mcp).Listen(ctx, in, out)
}
