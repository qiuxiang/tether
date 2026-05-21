package node

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
)

func awaitReply(t *testing.T, ch <-chan protocol.Message) *protocol.Reply {
	t.Helper()
	select {
	case m := <-ch:
		r, ok := m.(*protocol.Reply)
		if !ok {
			t.Fatalf("expected *protocol.Reply, got %T", m)
		}
		return r
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for reply")
		return nil
	}
}

func TestHandleCaptureScreen_NotFound(t *testing.T) {
	h := NewProcessHandler(t.TempDir(), 16)
	s := &captureSender{msgs: make(chan protocol.Message, 4)}
	h.Handle(context.Background(), s, &protocol.CaptureScreen{MsgID: "x", ProcessID: "missing"})
	r := awaitReply(t, s.msgs)
	if r.OK {
		t.Fatalf("expected OK=false, got %+v", r)
	}
}

func TestHandleList_IncludesLogPath(t *testing.T) {
	dir := t.TempDir()
	h := NewProcessHandler(dir, 16)
	p := &Process{ID: "lp", Cmd: []string{"true"}}
	done := make(chan struct{})
	if err := p.Start(context.Background(), dir, nil, "", func(int) { close(done) }); err != nil {
		t.Fatal(err)
	}
	h.registry.Add(p)
	<-done

	s := &captureSender{msgs: make(chan protocol.Message, 4)}
	h.Handle(context.Background(), s, &protocol.List{MsgID: "m", Limit: 10})
	r := awaitReply(t, s.msgs)
	items, _ := r.Data["processes"].([]map[string]any)
	if len(items) != 1 {
		t.Fatalf("items=%d", len(items))
	}
	lp, ok := items[0]["log_path"].(string)
	if !ok || lp == "" {
		t.Fatalf("log_path missing or empty: %+v", items[0])
	}
	if lp != p.LogPath {
		t.Fatalf("log_path %q != Process.LogPath %q", lp, p.LogPath)
	}
}

func TestHandleExec(t *testing.T) {
	h := NewProcessHandler(t.TempDir(), 16)
	s := &captureSender{msgs: make(chan protocol.Message, 8)}
	h.Handle(context.Background(), s, &protocol.Exec{
		MsgID: "e1",
		Cmd:   []string{"sh", "-c", "echo hi; echo bad 1>&2; exit 2"},
	})
	r := awaitReply(t, s.msgs)
	if !r.OK {
		t.Fatalf("exec reply not OK: %+v", r)
	}
	if got, _ := r.Data["stdout"].(string); !strings.Contains(got, "hi") {
		t.Fatalf("stdout = %q, want it to contain hi", got)
	}
	if got, _ := r.Data["stderr"].(string); !strings.Contains(got, "bad") {
		t.Fatalf("stderr = %q, want it to contain bad", got)
	}
	if r.Data["exit_code"] != 2 {
		t.Fatalf("exit_code = %v, want 2", r.Data["exit_code"])
	}
}

func TestHandleCaptureScreen_HappyPath(t *testing.T) {
	dir := t.TempDir()
	h := NewProcessHandler(dir, 16)
	p := &Process{ID: "ok", Cmd: []string{"sh", "-c", "printf 'a\\nb\\n'"}}
	done := make(chan struct{})
	if err := p.Start(context.Background(), dir, nil, "", func(int) { close(done) }); err != nil {
		t.Fatal(err)
	}
	h.registry.Add(p)
	<-done

	s := &captureSender{msgs: make(chan protocol.Message, 4)}
	h.Handle(context.Background(), s, &protocol.CaptureScreen{MsgID: "m", ProcessID: "ok"})
	r := awaitReply(t, s.msgs)
	if !r.OK {
		t.Fatalf("expected OK=true, got %+v", r)
	}
	lines, _ := r.Data["lines"].([]string)
	if len(lines) != 2 || lines[0] != "a" || lines[1] != "b" {
		t.Fatalf("lines=%q", lines)
	}
	if _, ok := r.Data["cursor"]; !ok {
		t.Fatalf("missing cursor in Data: %+v", r.Data)
	}
	if cols, _ := r.Data["cols"].(int); cols != vtCols {
		t.Fatalf("cols=%v want %d", r.Data["cols"], vtCols)
	}
	if total, _ := r.Data["total_lines"].(int); total != 2 {
		t.Fatalf("total_lines=%v want 2", r.Data["total_lines"])
	}
}
