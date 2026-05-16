package protocol

// Hub → Node
type Exec struct {
	Type      string            `cbor:"type"`
	MsgID     string            `cbor:"msg_id"`
	Target    string            `cbor:"target,omitempty"`
	Cmd       []string          `cbor:"cmd"`
	Cwd       string            `cbor:"cwd,omitempty"`
	Env       map[string]string `cbor:"env,omitempty"`
	Stdin     []byte            `cbor:"stdin,omitempty"`
	TTY       bool              `cbor:"tty,omitempty"`
	TimeoutMs int64             `cbor:"timeout_ms,omitempty"`
}

type ExecCancel struct {
	Type   string `cbor:"type"`
	MsgID  string `cbor:"msg_id"`
	Target string `cbor:"target,omitempty"`
}

type Start struct {
	Type      string            `cbor:"type"`
	MsgID     string            `cbor:"msg_id"`
	Target    string            `cbor:"target,omitempty"`
	ProcessID string            `cbor:"process_id"`
	Cmd       []string          `cbor:"cmd"`
	Cwd       string            `cbor:"cwd,omitempty"`
	Env       map[string]string `cbor:"env,omitempty"`
	TTY       bool              `cbor:"tty,omitempty"`
	Name      string            `cbor:"name,omitempty"`
}

type Stdin struct {
	Type      string `cbor:"type"`
	Target    string `cbor:"target,omitempty"`
	ProcessID string `cbor:"process_id"`
	Data      []byte `cbor:"data"`
}

type Kill struct {
	Type      string `cbor:"type"`
	MsgID     string `cbor:"msg_id"`
	Target    string `cbor:"target,omitempty"`
	ProcessID string `cbor:"process_id"`
	Signal    string `cbor:"signal,omitempty"`
}

type GetOutput struct {
	Type      string `cbor:"type"`
	MsgID     string `cbor:"msg_id"`
	Target    string `cbor:"target,omitempty"`
	ProcessID string `cbor:"process_id"`
	Offset    int64  `cbor:"offset,omitempty"`
	Length    int    `cbor:"length,omitempty"`
}

type List struct {
	Type         string `cbor:"type"`
	MsgID        string `cbor:"msg_id"`
	Target       string `cbor:"target,omitempty"`
	StatusFilter string `cbor:"status_filter,omitempty"`
	Limit        int    `cbor:"limit,omitempty"`
}

type Ping struct {
	Type string `cbor:"type"`
}

// Node → Hub
type Hello struct {
	Type         string `cbor:"type"`
	Hostname     string `cbor:"hostname"`
	OS           string `cbor:"os"`
	Arch         string `cbor:"arch"`
	AgentVersion string `cbor:"agent_version"`
	Token        string `cbor:"token"`
	Role         string `cbor:"role,omitempty"` // "node" (default) | "client"
}

type Reply struct {
	Type  string         `cbor:"type"`
	MsgID string         `cbor:"msg_id"`
	OK    bool           `cbor:"ok"`
	Error string         `cbor:"error,omitempty"`
	Data  map[string]any `cbor:"data,omitempty"`
}

type ExecOutput struct {
	Type   string `cbor:"type"`
	MsgID  string `cbor:"msg_id"`
	Stream string `cbor:"stream"` // "stdout" | "stderr"
	Data   []byte `cbor:"data"`
}

type ExecExit struct {
	Type  string `cbor:"type"`
	MsgID string `cbor:"msg_id"`
	Code  int    `cbor:"code"`
	Error string `cbor:"error,omitempty"`
}

type Event struct {
	Type      string `cbor:"type"`
	Kind      string `cbor:"kind"` // "exit"
	ProcessID string `cbor:"process_id"`
	Code      int    `cbor:"code,omitempty"`
}

type Pong struct {
	Type string `cbor:"type"`
}

// ListDevices is a hub-local request (no Target).
type ListDevices struct {
	Type  string `cbor:"type"`
	MsgID string `cbor:"msg_id"`
}

// Marker interface for any message.
type Message interface {
	msgType() string
}

func (m *Exec) msgType() string        { return "exec" }
func (m *ExecCancel) msgType() string  { return "exec_cancel" }
func (m *Start) msgType() string       { return "start" }
func (m *Stdin) msgType() string       { return "stdin" }
func (m *Kill) msgType() string        { return "kill" }
func (m *GetOutput) msgType() string   { return "get_output" }
func (m *List) msgType() string        { return "list" }
func (m *ListDevices) msgType() string { return "list_devices" }
func (m *Ping) msgType() string        { return "ping" }
func (m *Hello) msgType() string       { return "hello" }
func (m *Reply) msgType() string       { return "reply" }
func (m *ExecOutput) msgType() string  { return "exec_output" }
func (m *ExecExit) msgType() string    { return "exec_exit" }
func (m *Event) msgType() string       { return "event" }
func (m *Pong) msgType() string        { return "pong" }
