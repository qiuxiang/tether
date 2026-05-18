package node

import (
	"io"
	"strings"
	"unicode"

	"github.com/hinshun/vt10x"
)

const (
	vtCols = 200
	vtRows = 10000
	// ptyVisibleRows is what TIOCGWINSZ reports to the child process. Smaller
	// than vtRows so curses programs (nethack, htop, top) get a normal-sized
	// canvas, while the wider VT continues to accumulate scrollback as the
	// program prints past the visible bottom.
	ptyVisibleRows = 50
)

// vtSink is an io.Writer that forwards bytes into a Process's VT under vtMu.
// Used inside io.MultiWriter so PTY/pipe copy loops stay simple.
type vtSink struct {
	p *Process
}

func (s *vtSink) Write(b []byte) (int, error) {
	s.p.vtMu.Lock()
	defer s.p.vtMu.Unlock()
	if s.p.vt == nil {
		return len(b), nil
	}
	return s.p.vt.Write(b)
}

// CaptureScreen returns rendered lines in [startLine, endLine] (inclusive),
// tmux-style: negative indices count from the end, nil means "extreme"
// (top for start, bottom for end). Out-of-range values are clamped. Colors
// and display attributes are stripped; trailing whitespace per line is trimmed.
//
// `total` is the highest line index that has received any content plus 1.
// `cursorRow` and `cursorCol` are the VT cursor position (cursorRow is the
// absolute row index inside the VT, same coordinate space as start/end).
func (p *Process) CaptureScreen(startLine, endLine *int) (lines []string, cursorRow, cursorCol, total int) {
	p.vtMu.Lock()
	defer p.vtMu.Unlock()
	if p.vt == nil {
		return nil, 0, 0, 0
	}

	cols, rows := p.vt.Size()
	total = highestNonEmptyRow(p.vt, cols, rows) + 1
	cur := p.vt.Cursor()

	start, end := resolveRange(startLine, endLine, total)
	if start > end || total == 0 {
		return []string{}, cur.Y, cur.X, total
	}

	lines = make([]string, 0, end-start+1)
	for y := start; y <= end; y++ {
		lines = append(lines, renderLine(p.vt, cols, y))
	}
	return lines, cur.Y, cur.X, total
}

// resolveRange converts tmux-style indices (nil/negative) into [0, total)
// inclusive [start, end]. Returns start > end when the range is empty.
func resolveRange(startLine, endLine *int, total int) (int, int) {
	start := 0
	if startLine != nil {
		start = *startLine
		if start < 0 {
			start = total + start
		}
	}
	end := total - 1
	if endLine != nil {
		end = *endLine
		if end < 0 {
			end = total + end
		}
	}
	if start < 0 {
		start = 0
	}
	if end > total-1 {
		end = total - 1
	}
	return start, end
}

func renderLine(vt vt10x.Terminal, cols, row int) string {
	var b strings.Builder
	b.Grow(cols)
	for x := 0; x < cols; x++ {
		g := vt.Cell(x, row)
		if g.Char == 0 {
			b.WriteRune(' ')
		} else {
			b.WriteRune(g.Char)
		}
	}
	return strings.TrimRightFunc(b.String(), unicode.IsSpace)
}

// renderAll returns every non-empty row of vt as one "\n"-joined string
// with trailing whitespace trimmed per line. Used by exec to return the
// final rendered screen after the process exits.
func renderAll(vt vt10x.Terminal) string {
	cols, rows := vt.Size()
	last := highestNonEmptyRow(vt, cols, rows)
	if last < 0 {
		return ""
	}
	var b strings.Builder
	for y := 0; y <= last; y++ {
		if y > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(renderLine(vt, cols, y))
	}
	return b.String()
}

func highestNonEmptyRow(vt vt10x.Terminal, cols, rows int) int {
	for y := rows - 1; y >= 0; y-- {
		for x := 0; x < cols; x++ {
			g := vt.Cell(x, y)
			if g.Char != 0 && g.Char != ' ' {
				return y
			}
		}
	}
	return -1
}

// Compile-time guard so a refactor doesn't accidentally break the io.Writer
// contract relied on by io.MultiWriter.
var _ io.Writer = (*vtSink)(nil)
