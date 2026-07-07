package protocol

import (
	"crypto/rand"
	"encoding/hex"
)

// NewID returns a random hex-encoded identifier of n random bytes. Used for
// msg_ids, client ids, and stream ids across the hub, node, and client.
func NewID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
