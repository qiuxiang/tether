package protocol

import (
	"fmt"
	"reflect"

	"github.com/fxamacker/cbor/v2"
)

// WSReadLimit is the per-message read limit applied to every websocket.Conn.
// File-transfer chunks are 256 KiB; 4 MiB gives comfortable headroom for CBOR
// framing overhead while staying well below any network-layer limit.
const WSReadLimit int64 = 4 << 20 // 4 MiB

// decMode forces untyped nested maps to decode as map[string]any so the result
// round-trips through encoding/json. Without this, fxamacker defaults to
// map[interface{}]interface{}, which json.Marshal silently fails on.
var decMode cbor.DecMode

func init() {
	m, err := cbor.DecOptions{DefaultMapType: reflect.TypeOf(map[string]any{})}.DecMode()
	if err != nil {
		panic(err)
	}
	decMode = m
}

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
	case *Start:
		v.Type = m.msgType()
	case *Stdin:
		v.Type = m.msgType()
	case *Kill:
		v.Type = m.msgType()
	case *CaptureScreen:
		v.Type = m.msgType()
	case *List:
		v.Type = m.msgType()
	case *Attach:
		v.Type = m.msgType()
	case *Detach:
		v.Type = m.msgType()
	case *ListDevices:
		v.Type = m.msgType()
	case *Hello:
		v.Type = m.msgType()
	case *Reply:
		v.Type = m.msgType()
	case *Event:
		v.Type = m.msgType()
	case *ProcessOutput:
		v.Type = m.msgType()
	case *ProcessExit:
		v.Type = m.msgType()
	case *FileGetOpen:
		v.Type = m.msgType()
	case *FilePutOpen:
		v.Type = m.msgType()
	case *FileChunk:
		v.Type = m.msgType()
	case *FileAbort:
		v.Type = m.msgType()
	case *FileRelay:
		v.Type = m.msgType()
	case *FileLocalCopy:
		v.Type = m.msgType()
	case *ReadFileReq:
		v.Type = m.msgType()
	case *WriteFileReq:
		v.Type = m.msgType()
	case *EditFileReq:
		v.Type = m.msgType()
	case *ForwardListen:
		v.Type = m.msgType()
	case *ForwardUnlisten:
		v.Type = m.msgType()
	case *ForwardDial:
		v.Type = m.msgType()
	case *ForwardData:
		v.Type = m.msgType()
	case *ForwardClose:
		v.Type = m.msgType()
	case *rawMsg:
		v.Type = m.msgType()
	}
}

// Decode peeks at "type" and unmarshals into the right concrete struct.
func Decode(data []byte) (Message, error) {
	var hdr rawMsg
	if err := decMode.Unmarshal(data, &hdr); err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}
	var m Message
	switch hdr.Type {
	case "start":
		m = &Start{}
	case "stdin":
		m = &Stdin{}
	case "kill":
		m = &Kill{}
	case "capture_screen":
		m = &CaptureScreen{}
	case "list":
		m = &List{}
	case "attach":
		m = &Attach{}
	case "detach":
		m = &Detach{}
	case "list_devices":
		m = &ListDevices{}
	case "hello":
		m = &Hello{}
	case "reply":
		m = &Reply{}
	case "event":
		m = &Event{}
	case "process_output":
		m = &ProcessOutput{}
	case "process_exit":
		m = &ProcessExit{}
	case "file_get_open":
		m = &FileGetOpen{}
	case "file_put_open":
		m = &FilePutOpen{}
	case "file_chunk":
		m = &FileChunk{}
	case "file_abort":
		m = &FileAbort{}
	case "file_relay":
		m = &FileRelay{}
	case "file_local_copy":
		m = &FileLocalCopy{}
	case "read_file":
		m = &ReadFileReq{}
	case "write_file":
		m = &WriteFileReq{}
	case "edit_file":
		m = &EditFileReq{}
	case "forward_listen":
		m = &ForwardListen{}
	case "forward_unlisten":
		m = &ForwardUnlisten{}
	case "forward_dial":
		m = &ForwardDial{}
	case "forward_data":
		m = &ForwardData{}
	case "forward_close":
		m = &ForwardClose{}
	default:
		return nil, fmt.Errorf("unknown message type: %q", hdr.Type)
	}
	if err := decMode.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("decode %s: %w", hdr.Type, err)
	}
	return m, nil
}
