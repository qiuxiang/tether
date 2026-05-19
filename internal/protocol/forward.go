package protocol

// ForwardListen — client→node: open a TCP listener on `listen_addr`.
type ForwardListen struct {
	Type       string `cbor:"type"`
	MsgID      string `cbor:"msg_id"`
	Target     string `cbor:"target,omitempty"`
	ForwardID  string `cbor:"forward_id"`
	ListenAddr string `cbor:"listen_addr"`
	DestHost   string `cbor:"dest_host"`
	DestPort   int    `cbor:"dest_port"`
}

type ForwardUnlisten struct {
	Type      string `cbor:"type"`
	MsgID     string `cbor:"msg_id"`
	Target    string `cbor:"target,omitempty"`
	ForwardID string `cbor:"forward_id"`
}

// ForwardDial — bidirectional: ForwardID set when node→client; Target set when client→node.
type ForwardDial struct {
	Type      string `cbor:"type"`
	MsgID     string `cbor:"msg_id"`
	Target    string `cbor:"target,omitempty"`
	StreamID  string `cbor:"stream_id"`
	ForwardID string `cbor:"forward_id,omitempty"`
	DestHost  string `cbor:"dest_host,omitempty"`
	DestPort  int    `cbor:"dest_port,omitempty"`
}

type ForwardData struct {
	Type     string `cbor:"type"`
	Target   string `cbor:"target,omitempty"`
	StreamID string `cbor:"stream_id"`
	Data     []byte `cbor:"data"`
}

// ForwardClose — Half ∈ {"read","write","both"} (default "both").
type ForwardClose struct {
	Type     string `cbor:"type"`
	Target   string `cbor:"target,omitempty"`
	StreamID string `cbor:"stream_id"`
	Half     string `cbor:"half,omitempty"`
}

func (m *ForwardListen) msgType() string   { return "forward_listen" }
func (m *ForwardUnlisten) msgType() string { return "forward_unlisten" }
func (m *ForwardDial) msgType() string     { return "forward_dial" }
func (m *ForwardData) msgType() string     { return "forward_data" }
func (m *ForwardClose) msgType() string    { return "forward_close" }
