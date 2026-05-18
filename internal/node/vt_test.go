package node

import (
	"strings"
	"testing"

	"github.com/hinshun/vt10x"
)

func newTestProcess() *Process {
	p := &Process{ID: "test"}
	p.vt = vt10x.New(vt10x.WithSize(vtCols, vtRows))
	return p
}

func TestCaptureScreen_PlainText(t *testing.T) {
	p := newTestProcess()
	p.vt.Write([]byte("hello\r\nworld\r\n"))

	lines, _, _, total := p.CaptureScreen(nil, nil)
	if total != 2 {
		t.Fatalf("total_lines: got %d, want 2", total)
	}
	if len(lines) != 2 || lines[0] != "hello" || lines[1] != "world" {
		t.Fatalf("lines: got %q", lines)
	}
}

func TestCaptureScreen_StripsANSIColor(t *testing.T) {
	p := newTestProcess()
	p.vt.Write([]byte("\x1b[31mred\x1b[0m\r\n"))

	lines, _, _, _ := p.CaptureScreen(nil, nil)
	if len(lines) != 1 || lines[0] != "red" {
		t.Fatalf("got %q", lines)
	}
	if strings.ContainsAny(lines[0], "\x1b[") {
		t.Fatalf("escape leaked: %q", lines[0])
	}
}

func TestCaptureScreen_CarriageReturnOverwrite(t *testing.T) {
	p := newTestProcess()
	p.vt.Write([]byte("foo\rbar\r\n"))

	lines, _, _, _ := p.CaptureScreen(nil, nil)
	if len(lines) != 1 || lines[0] != "bar" {
		t.Fatalf("got %q, want [bar]", lines)
	}
}

func TestCaptureScreen_LineRange(t *testing.T) {
	p := newTestProcess()
	var b strings.Builder
	for i := 0; i < 100; i++ {
		b.WriteString("line")
		b.WriteString(vtItoa(i))
		b.WriteString("\r\n")
	}
	p.vt.Write([]byte(b.String()))

	last10Start, last10End := -10, -1
	lines, _, _, total := p.CaptureScreen(&last10Start, &last10End)
	if total != 100 {
		t.Fatalf("total: %d", total)
	}
	if len(lines) != 10 || lines[0] != "line90" || lines[9] != "line99" {
		t.Fatalf("got %q", lines)
	}

	mid1, mid2 := 50, 59
	lines, _, _, _ = p.CaptureScreen(&mid1, &mid2)
	if len(lines) != 10 || lines[0] != "line50" || lines[9] != "line59" {
		t.Fatalf("got %q", lines)
	}

	oob := -9999
	lines, _, _, _ = p.CaptureScreen(&oob, nil)
	if len(lines) != 100 || lines[0] != "line0" {
		t.Fatalf("clamp failed: len=%d first=%q", len(lines), lines[0])
	}
}

func TestCaptureScreen_Empty(t *testing.T) {
	p := newTestProcess()
	lines, row, col, total := p.CaptureScreen(nil, nil)
	if len(lines) != 0 || total != 0 || row != 0 || col != 0 {
		t.Fatalf("empty: lines=%q total=%d row=%d col=%d", lines, total, row, col)
	}
}

func TestCaptureScreen_ClearScreen(t *testing.T) {
	p := newTestProcess()
	p.vt.Write([]byte("line1\r\nline2\r\n\x1b[2J\x1b[H"))

	lines, _, _, total := p.CaptureScreen(nil, nil)
	for _, l := range lines {
		if strings.TrimSpace(l) != "" {
			t.Fatalf("after clear: got nonempty %q (total=%d)", l, total)
		}
	}
}

// vtItoa avoids importing strconv just for tests.
func vtItoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
