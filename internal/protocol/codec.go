package protocol

import (
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

type rawMsg struct {
	Type string `cbor:"type"`
}

func (m *rawMsg) msgType() string { return m.Type }

// Encode sets the Type field via msgType() and CBOR-encodes the value.
func Encode(m Message) ([]byte, error) {
	// Type field is part of each struct; ensure it's set.
	setType(m)
	return cbor.Marshal(m)
}

func setType(m Message) {
	switch v := m.(type) {
	case *Exec:
		v.Type = m.msgType()
	case *ExecCancel:
		v.Type = m.msgType()
	case *Start:
		v.Type = m.msgType()
	case *Stdin:
		v.Type = m.msgType()
	case *Kill:
		v.Type = m.msgType()
	case *GetOutput:
		v.Type = m.msgType()
	case *List:
		v.Type = m.msgType()
	case *Ping:
		v.Type = m.msgType()
	case *Hello:
		v.Type = m.msgType()
	case *Reply:
		v.Type = m.msgType()
	case *ExecOutput:
		v.Type = m.msgType()
	case *ExecExit:
		v.Type = m.msgType()
	case *Event:
		v.Type = m.msgType()
	case *Pong:
		v.Type = m.msgType()
	case *rawMsg:
		v.Type = m.msgType()
	}
}

// Decode peeks at "type" and unmarshals into the right concrete struct.
func Decode(data []byte) (Message, error) {
	var hdr rawMsg
	if err := cbor.Unmarshal(data, &hdr); err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}
	var m Message
	switch hdr.Type {
	case "exec":
		m = &Exec{}
	case "exec_cancel":
		m = &ExecCancel{}
	case "start":
		m = &Start{}
	case "stdin":
		m = &Stdin{}
	case "kill":
		m = &Kill{}
	case "get_output":
		m = &GetOutput{}
	case "list":
		m = &List{}
	case "ping":
		m = &Ping{}
	case "hello":
		m = &Hello{}
	case "reply":
		m = &Reply{}
	case "exec_output":
		m = &ExecOutput{}
	case "exec_exit":
		m = &ExecExit{}
	case "event":
		m = &Event{}
	case "pong":
		m = &Pong{}
	default:
		return nil, fmt.Errorf("unknown message type: %q", hdr.Type)
	}
	if err := cbor.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("decode %s: %w", hdr.Type, err)
	}
	return m, nil
}
