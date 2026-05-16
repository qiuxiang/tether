package protocol

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestRoundTripListDevices(t *testing.T) {
	in := &ListDevices{MsgID: "abc"}
	raw, err := Encode(in)
	require.NoError(t, err)
	out, err := Decode(raw)
	require.NoError(t, err)
	got, ok := out.(*ListDevices)
	require.True(t, ok)
	require.Equal(t, "abc", got.MsgID)
	require.Equal(t, "list_devices", got.Type)
}

func TestRoundTripExecWithTarget(t *testing.T) {
	in := &Exec{MsgID: "m1", Target: "host-a", Cmd: []string{"sh", "-c", "ls"}}
	raw, err := Encode(in)
	require.NoError(t, err)
	out, err := Decode(raw)
	require.NoError(t, err)
	got, ok := out.(*Exec)
	require.True(t, ok)
	require.Equal(t, "host-a", got.Target)
	require.Equal(t, "m1", got.MsgID)
}

func TestRoundTripHelloRole(t *testing.T) {
	in := &Hello{Hostname: "c1", Token: "t", Role: "client"}
	raw, err := Encode(in)
	require.NoError(t, err)
	out, err := Decode(raw)
	require.NoError(t, err)
	got, ok := out.(*Hello)
	require.True(t, ok)
	require.Equal(t, "client", got.Role)
}
