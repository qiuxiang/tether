package protocol

// Hub → Node
type Start struct {
	Type        string            `cbor:"type"`
	MsgID       string            `cbor:"msg_id"`
	Target      string            `cbor:"target,omitempty"`
	ProcessID   string            `cbor:"process_id"`
	Cmd         []string          `cbor:"cmd"`
	Cwd         string            `cbor:"cwd,omitempty"`
	Env         map[string]string `cbor:"env,omitempty"`
	Description string            `cbor:"description,omitempty"`
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

// CaptureScreen requests the rendered terminal screen of a process.
// StartLine/EndLine use tmux semantics: negative indices count from the end,
// nil means "extreme" (start = top of scrollback, end = current last line).
type CaptureScreen struct {
	Type      string `cbor:"type"`
	MsgID     string `cbor:"msg_id"`
	Target    string `cbor:"target,omitempty"`
	ProcessID string `cbor:"process_id"`
	StartLine *int   `cbor:"start_line,omitempty"`
	EndLine   *int   `cbor:"end_line,omitempty"`
}

type List struct {
	Type         string `cbor:"type"`
	MsgID        string `cbor:"msg_id"`
	Target       string `cbor:"target,omitempty"`
	StatusFilter string `cbor:"status_filter,omitempty"`
	Limit        int    `cbor:"limit,omitempty"`
}

// Attach subscribes the originating client to a process's raw pty byte stream.
// The hub treats this as a sticky stream (same routing as the old Exec). Node
// replies with a one-shot Reply{ok} first (so failures surface as errors),
// then pushes ProcessOutput frames until the process exits (terminal
// ProcessExit) or the client sends Detach.
type Attach struct {
	Type       string `cbor:"type"`
	MsgID      string `cbor:"msg_id"`
	Target     string `cbor:"target,omitempty"`
	ProcessID  string `cbor:"process_id"`
	FromOffset int64  `cbor:"from_offset,omitempty"`
}

// Detach cancels a prior Attach. The process keeps running.
type Detach struct {
	Type      string `cbor:"type"`
	MsgID     string `cbor:"msg_id"`
	Target    string `cbor:"target,omitempty"`
	ProcessID string `cbor:"process_id"`
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

type Event struct {
	Type      string `cbor:"type"`
	Kind      string `cbor:"kind"` // "exit" | "device_online" | "device_offline"
	ProcessID string `cbor:"process_id,omitempty"`
	Code      int    `cbor:"code,omitempty"`
	Device    string `cbor:"device,omitempty"`
}

// ProcessOutput is a chunk of raw pty bytes streamed to an Attach subscriber.
type ProcessOutput struct {
	Type   string `cbor:"type"`
	MsgID  string `cbor:"msg_id"`
	Offset int64  `cbor:"offset"`
	Data   []byte `cbor:"data"`
}

// ProcessExit terminates an Attach stream when the process has exited.
type ProcessExit struct {
	Type  string `cbor:"type"`
	MsgID string `cbor:"msg_id"`
	Code  int    `cbor:"code"`
}

// ListDevices is a hub-local request (no Target).
type ListDevices struct {
	Type  string `cbor:"type"`
	MsgID string `cbor:"msg_id"`
}

// FileGetOpen — client → hub → node (download). Node replies with metadata
// then pushes FileChunk frames until EOF.
type FileGetOpen struct {
	Type   string `cbor:"type"`
	MsgID  string `cbor:"msg_id"`
	Target string `cbor:"target,omitempty"`
	Path   string `cbor:"path"`
}

// FilePutOpen — client → hub → node (upload). Node replies ok:true when
// ready; client pushes FileChunk frames until EOF=true; node verifies
// sha256 then sends the final Reply.
type FilePutOpen struct {
	Type      string `cbor:"type"`
	MsgID     string `cbor:"msg_id"`
	Target    string `cbor:"target,omitempty"`
	Path      string `cbor:"path"`
	Size      int64  `cbor:"size"`
	Mode      uint32 `cbor:"mode,omitempty"`
	Overwrite bool   `cbor:"overwrite,omitempty"`
	SHA256    string `cbor:"sha256,omitempty"`
}

// FileChunk — bidirectional streaming frame keyed by msg_id.
type FileChunk struct {
	Type  string `cbor:"type"`
	MsgID string `cbor:"msg_id"`
	Seq   int64  `cbor:"seq"`
	Data  []byte `cbor:"data"`
	EOF   bool   `cbor:"eof,omitempty"`
}

// FileAbort — either side cancels a transfer.
type FileAbort struct {
	Type  string `cbor:"type"`
	MsgID string `cbor:"msg_id"`
	Error string `cbor:"error"`
}

// FileRelay — client → hub only. Hub coordinates a streaming copy between
// from_node and to_node.
type FileRelay struct {
	Type      string `cbor:"type"`
	MsgID     string `cbor:"msg_id"`
	FromNode  string `cbor:"from_node"`
	FromPath  string `cbor:"from_path"`
	ToNode    string `cbor:"to_node"`
	ToPath    string `cbor:"to_path"`
	Overwrite bool   `cbor:"overwrite,omitempty"`
}

// FileLocalCopy — client → hub → node. Same-node copy between two paths.
type FileLocalCopy struct {
	Type      string `cbor:"type"`
	MsgID     string `cbor:"msg_id"`
	Target    string `cbor:"target,omitempty"`
	FromPath  string `cbor:"from_path"`
	ToPath    string `cbor:"to_path"`
	Overwrite bool   `cbor:"overwrite,omitempty"`
}

// ReadFileReq — client → hub → node. Reads a slice of a file's lines.
// Reply.Data: {lines: [][]byte, total_lines int, truncated bool, sha256 string, binary bool}.
type ReadFileReq struct {
	Type   string `cbor:"type"`
	MsgID  string `cbor:"msg_id"`
	Target string `cbor:"target,omitempty"`
	Path   string `cbor:"path"`
	Offset int    `cbor:"offset,omitempty"`
	Limit  int    `cbor:"limit,omitempty"`
}

// WriteFileReq — client → hub → node. Atomic write (temp + fsync + rename).
// Reply.Data: {bytes int64, sha256 string}.
type WriteFileReq struct {
	Type       string `cbor:"type"`
	MsgID      string `cbor:"msg_id"`
	Target     string `cbor:"target,omitempty"`
	Path       string `cbor:"path"`
	Content    []byte `cbor:"content"`
	Overwrite  bool   `cbor:"overwrite,omitempty"`
	CreateDirs bool   `cbor:"create_dirs,omitempty"`
}

// EditFileReq — client → hub → node. Replaces OldString with NewString.
// When ReplaceAll is false, OldString must occur exactly once.
// Reply.Data: {replacements int, sha256 string}.
type EditFileReq struct {
	Type       string `cbor:"type"`
	MsgID      string `cbor:"msg_id"`
	Target     string `cbor:"target,omitempty"`
	Path       string `cbor:"path"`
	OldString  []byte `cbor:"old_string"`
	NewString  []byte `cbor:"new_string"`
	ReplaceAll bool   `cbor:"replace_all,omitempty"`
}

// Marker interface for any message.
type Message interface {
	msgType() string
}

func (m *Start) msgType() string       { return "start" }
func (m *Stdin) msgType() string       { return "stdin" }
func (m *Kill) msgType() string        { return "kill" }
func (m *CaptureScreen) msgType() string  { return "capture_screen" }
func (m *List) msgType() string        { return "list" }
func (m *Attach) msgType() string      { return "attach" }
func (m *Detach) msgType() string      { return "detach" }
func (m *ListDevices) msgType() string { return "list_devices" }
func (m *Hello) msgType() string       { return "hello" }
func (m *Reply) msgType() string       { return "reply" }
func (m *Event) msgType() string       { return "event" }
func (m *ProcessOutput) msgType() string { return "process_output" }
func (m *ProcessExit) msgType() string   { return "process_exit" }
func (m *FileGetOpen) msgType() string   { return "file_get_open" }
func (m *FilePutOpen) msgType() string   { return "file_put_open" }
func (m *FileChunk) msgType() string     { return "file_chunk" }
func (m *FileAbort) msgType() string     { return "file_abort" }
func (m *FileRelay) msgType() string     { return "file_relay" }
func (m *FileLocalCopy) msgType() string { return "file_local_copy" }
func (m *ReadFileReq) msgType() string   { return "read_file" }
func (m *WriteFileReq) msgType() string  { return "write_file" }
func (m *EditFileReq) msgType() string   { return "edit_file" }
