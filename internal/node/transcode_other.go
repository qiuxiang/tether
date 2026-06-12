//go:build !windows

package node

// toUTF8 is a no-op off Windows: command output is already UTF-8.
func toUTF8(b []byte) string { return string(b) }
