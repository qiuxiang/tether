package node

import (
	"context"
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

func TestHandleCaptureScreen_HappyPath(t *testing.T) {
	dir := t.TempDir()
	h := NewProcessHandler(dir, 16)
	p := &Process{ID: "ok", Cmd: []string{"sh", "-c", "printf 'a\\nb\\n'"}}
	done := make(chan struct{})
	if err := p.Start(context.Background(), dir, nil, "", false, func(int) { close(done) }); err != nil {
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
