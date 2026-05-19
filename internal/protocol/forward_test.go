package protocol

import (
	"reflect"
	"testing"
)

func TestForwardListenRoundtrip(t *testing.T) {
	in := &ForwardListen{MsgID: "m1", Target: "mac", ForwardID: "f1",
		ListenAddr: "127.0.0.1:8080", DestHost: "localhost", DestPort: 3000}
	raw, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(*ForwardListen)
	if !ok {
		t.Fatalf("decoded type %T", out)
	}
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("mismatch: %+v vs %+v", in, got)
	}
}

func TestForwardUnlistenRoundtrip(t *testing.T) {
	in := &ForwardUnlisten{MsgID: "m2", Target: "mac", ForwardID: "f1"}
	raw, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(*ForwardUnlisten)
	if !ok {
		t.Fatalf("decoded type %T", out)
	}
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("mismatch: %+v vs %+v", in, got)
	}
}

func TestForwardDialRoundtrip(t *testing.T) {
	in := &ForwardDial{MsgID: "m3", Target: "mac", StreamID: "s1",
		ForwardID: "f1", DestHost: "localhost", DestPort: 5037}
	raw, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(*ForwardDial)
	if !ok {
		t.Fatalf("decoded type %T", out)
	}
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("mismatch: %+v vs %+v", in, got)
	}
}

func TestForwardDataRoundtrip(t *testing.T) {
	in := &ForwardData{Target: "mac", StreamID: "s1", Data: []byte("hello")}
	raw, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(*ForwardData)
	if !ok {
		t.Fatalf("decoded type %T", out)
	}
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("mismatch: %+v vs %+v", in, got)
	}
}

func TestForwardCloseRoundtrip(t *testing.T) {
	in := &ForwardClose{Target: "mac", StreamID: "s1", Half: "write"}
	raw, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(*ForwardClose)
	if !ok {
		t.Fatalf("decoded type %T", out)
	}
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("mismatch: %+v vs %+v", in, got)
	}
}

func TestEventDeviceRoundtrip(t *testing.T) {
	in := &Event{Kind: "device_online", Device: "mac"}
	raw, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(*Event)
	if !ok {
		t.Fatalf("decoded type %T", out)
	}
	if !reflect.DeepEqual(in, got) {
		t.Fatalf("mismatch: %+v vs %+v", in, got)
	}
}
