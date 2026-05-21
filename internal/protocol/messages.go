package protocol

// Exec — client → hub → node. Runs Cmd as a plain subprocess, waits for it
// to exit (or until Timeout seconds elapse, default 30, after which the node
// kills the process group), and returns the result in a single Reply.
// Reply.Data: {stdout string, stderr string, exit_code int, timed_out bool,
// truncated bool}.
type Exec struct {
	Type    string            `cbor:"type"`
	MsgID   string            `cbor:"msg_id"`
	Target  string            `cbor:"target,omitempty"`
	Cmd     []string          `cbor:"cmd"`
	Cwd     string            `cbor:"cwd,omitempty"`
	Env     map[string]string `cbor:"env,omitempty"`
	Timeout int               `cbor:"timeout,omitempty"`
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

func (m *Exec) msgType() string        { return "exec" }
func (m *ListDevices) msgType() string { return "list_devices" }
func (m *Hello) msgType() string       { return "hello" }
func (m *Reply) msgType() string       { return "reply" }
func (m *Event) msgType() string       { return "event" }
func (m *FileGetOpen) msgType() string   { return "file_get_open" }
func (m *FilePutOpen) msgType() string   { return "file_put_open" }
func (m *FileChunk) msgType() string     { return "file_chunk" }
func (m *FileAbort) msgType() string     { return "file_abort" }
func (m *FileRelay) msgType() string     { return "file_relay" }
func (m *FileLocalCopy) msgType() string { return "file_local_copy" }
func (m *ReadFileReq) msgType() string   { return "read_file" }
func (m *WriteFileReq) msgType() string  { return "write_file" }
func (m *EditFileReq) msgType() string   { return "edit_file" }
