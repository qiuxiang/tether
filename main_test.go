package main

import (
	"bytes"
	"testing"
)

func TestDispatchUnknownSubcommand(t *testing.T) {
	var stderr bytes.Buffer
	code := run([]string{"tether", "wat"}, &stderr)
	if code == 0 {
		t.Fatalf("expected non-zero exit, got 0")
	}
	if stderr.Len() == 0 {
		t.Fatalf("expected error on stderr")
	}
}

func TestDispatchNoArgsShowsUsage(t *testing.T) {
	var stderr bytes.Buffer
	code := run([]string{"tether"}, &stderr)
	if code == 0 {
		t.Fatalf("expected non-zero exit")
	}
}
