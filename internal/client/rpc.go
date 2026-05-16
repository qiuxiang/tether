package client

import "github.com/qiuxiang/tether/internal/protocol"

// RPC stub — will be replaced with full implementation in Task 6 (rpc.go).
type RPC struct{}

func NewRPC() *RPC { return &RPC{} }

// Deliver is called by Conn's read loop for every inbound message.
// The real implementation (Task 6) will route messages to pending callers.
func (r *RPC) Deliver(msg protocol.Message) {}
