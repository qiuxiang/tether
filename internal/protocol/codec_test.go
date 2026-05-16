package protocol

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeHello(t *testing.T) {
	msg := Hello{Hostname: "mac", OS: "darwin", Arch: "arm64", AgentVersion: "0.1.0", Token: "secret"}
	data, err := Encode(&msg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := decoded.(*Hello)
	if !ok {
		t.Fatalf("expected *Hello, got %T", decoded)
	}
	if got.Hostname != "mac" || got.Token != "secret" {
		t.Fatalf("round-trip lost data: %+v", got)
	}
}

func TestEncodeDecodeExecOutput(t *testing.T) {
	msg := ExecOutput{MsgID: "abc", Stream: "stdout", Data: []byte{0x00, 0xFF, 0x80, 0x7F}}
	data, err := Encode(&msg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	got := decoded.(*ExecOutput)
	if !bytes.Equal(got.Data, msg.Data) {
		t.Fatalf("binary data round-trip failed: %v", got.Data)
	}
}

func TestDecodeUnknownType(t *testing.T) {
	raw, _ := Encode(&rawMsg{Type: "nope"})
	_, err := Decode(raw)
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}
