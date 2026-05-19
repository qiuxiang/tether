package node

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/qiuxiang/tether/internal/protocol"
	"github.com/stretchr/testify/assert"
)

func TestStartPTY_FeedsVT(t *testing.T) {
	dir := t.TempDir()
	p := &Process{ID: "vt-pty", Cmd: []string{"sh", "-c", "printf 'foo\\rbar\\n'"}}
	done := make(chan struct{})
	if err := p.Start(context.Background(), dir, nil, "", func(code int) { close(done) }); err != nil {
		t.Fatal(err)
	}
	<-done

	lines, _, _, total := p.CaptureScreen(nil, nil)
	if total != 1 || len(lines) != 1 || lines[0] != "bar" {
		t.Fatalf("got lines=%q total=%d (CR overwrite should leave 'bar')", lines, total)
	}
}

func TestStartPTY_WinSize(t *testing.T) {
	dir := t.TempDir()
	p := &Process{ID: "vt-winsize", Cmd: []string{"sh", "-c", "stty size"}}
	done := make(chan struct{})
	if err := p.Start(context.Background(), dir, nil, "", func(code int) { close(done) }); err != nil {
		t.Fatal(err)
	}
	<-done

	lines, _, _, _ := p.CaptureScreen(nil, nil)
	want := "50 200"
	found := false
	for _, l := range lines {
		if strings.TrimSpace(l) == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected stty size %q in output, got lines=%q", want, lines)
	}
}

func TestStartPTY_BusReceivesOutputAndClosesOnExit(t *testing.T) {
	dir := t.TempDir()
	p := &Process{ID: "p1", Cmd: []string{"sh", "-c", "printf hello"}}
	exited := make(chan int, 1)
	if err := p.Start(context.Background(), dir, nil, "", func(code int) { exited <- code }); err != nil {
		t.Fatal(err)
	}

	sub := p.bus.Subscribe(0)
	var got []byte
	deadline := time.After(2 * time.Second)
	for {
		select {
		case chunk, ok := <-sub.Ch():
			if !ok {
				if !bytes.Contains(got, []byte("hello")) {
					t.Fatalf("bus output missing 'hello': %q", got)
				}
				<-exited
				return
			}
			got = append(got, chunk...)
		case <-deadline:
			t.Fatalf("timeout, got=%q", got)
		}
	}
}

func TestExecPTYTtyDetected(t *testing.T) {
	send := &captureSender{msgs: make(chan protocol.Message, 32)}
	h := NewProcessHandler(t.TempDir(), 50)

	// Start the process.
	h.Handle(context.Background(), send, &protocol.Start{
		MsgID:     "s-tty",
		ProcessID: "p-tty",
		Cmd:       []string{"sh", "-c", "if [ -t 0 ]; then echo TTY; else echo PIPE; fi"},
	})

	// Consume the Reply from Start (skip any interleaved Event frames).
	startReply := awaitReplySkipping(t, send.msgs)
	if !startReply.OK {
		t.Fatalf("Start failed: %v", startReply.Error)
	}

	// Attach to the process.
	go h.Handle(context.Background(), send, &protocol.Attach{
		MsgID:     "a-tty",
		ProcessID: "p-tty",
	})

	// Consume the Reply from Attach (skip any interleaved Event frames).
	attachReply := awaitReplySkipping(t, send.msgs)
	if !attachReply.OK {
		t.Fatalf("Attach failed: %v", attachReply.Error)
	}

	var out []byte
	deadline := time.After(5 * time.Second)
	for {
		select {
		case m := <-send.msgs:
			switch v := m.(type) {
			case *protocol.ProcessOutput:
				out = append(out, v.Data...)
			case *protocol.ProcessExit:
				assert.Contains(t, strings.ToUpper(string(out)), "TTY")
				return
			}
		case <-deadline:
			t.Fatal("timeout")
		}
	}
}
