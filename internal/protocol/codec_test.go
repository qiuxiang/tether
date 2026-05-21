package protocol

import (
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

func TestRoundTripFilePutOpen(t *testing.T) {
	in := &FilePutOpen{MsgID: "m1", Target: "n1", Path: "/tmp/x", Size: 1024, SHA256: "abc", Overwrite: true}
	raw, err := Encode(in)
	require.NoError(t, err)
	out, err := Decode(raw)
	require.NoError(t, err)
	got, ok := out.(*FilePutOpen)
	require.True(t, ok)
	require.Equal(t, "n1", got.Target)
	require.Equal(t, int64(1024), got.Size)
	require.True(t, got.Overwrite)
}

func TestRoundTripFileChunk(t *testing.T) {
	in := &FileChunk{MsgID: "m1", Seq: 3, Data: []byte("hello"), EOF: true}
	raw, err := Encode(in)
	require.NoError(t, err)
	out, err := Decode(raw)
	require.NoError(t, err)
	got, ok := out.(*FileChunk)
	require.True(t, ok)
	require.Equal(t, int64(3), got.Seq)
	require.Equal(t, []byte("hello"), got.Data)
	require.True(t, got.EOF)
}

func TestRoundTripFileRelay(t *testing.T) {
	in := &FileRelay{MsgID: "m1", FromNode: "a", FromPath: "/a", ToNode: "b", ToPath: "/b"}
	raw, err := Encode(in)
	require.NoError(t, err)
	out, err := Decode(raw)
	require.NoError(t, err)
	got, ok := out.(*FileRelay)
	require.True(t, ok)
	require.Equal(t, "a", got.FromNode)
	require.Equal(t, "/b", got.ToPath)
}

func TestCodecRoundTripReadFile(t *testing.T) {
	in := &ReadFileReq{MsgID: "m1", Target: "n1", Path: "/etc/hosts", Offset: 10, Limit: 50}
	b, err := Encode(in)
	require.NoError(t, err)
	out, err := Decode(b)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestCodecRoundTripWriteFile(t *testing.T) {
	in := &WriteFileReq{MsgID: "m2", Target: "n1", Path: "/tmp/x", Content: []byte("hello"), Overwrite: true, CreateDirs: false}
	b, err := Encode(in)
	require.NoError(t, err)
	out, err := Decode(b)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestCodecRoundTripEditFile(t *testing.T) {
	in := &EditFileReq{MsgID: "m3", Target: "n1", Path: "/tmp/x", OldString: []byte("foo"), NewString: []byte("bar"), ReplaceAll: true}
	b, err := Encode(in)
	require.NoError(t, err)
	out, err := Decode(b)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestRoundTripExec(t *testing.T) {
	in := &Exec{
		MsgID:   "m1",
		Target:  "host-a",
		Cmd:     []string{"sh", "-c", "ls"},
		Cwd:     "/tmp",
		Env:     map[string]string{"A": "b"},
		Timeout: 10,
	}
	enc, err := Encode(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := Decode(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got, ok := decoded.(*Exec)
	if !ok {
		t.Fatalf("decoded type = %T, want *Exec", decoded)
	}
	if got.MsgID != in.MsgID || got.Target != in.Target || got.Cwd != in.Cwd || got.Timeout != in.Timeout {
		t.Fatalf("scalar mismatch: %+v vs %+v", got, in)
	}
	if len(got.Cmd) != 3 || got.Cmd[2] != "ls" || got.Env["A"] != "b" {
		t.Fatalf("slice/map mismatch: %+v", got)
	}
	if got.Type != "exec" {
		t.Fatalf("Type = %q, want exec", got.Type)
	}
}
