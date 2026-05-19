package forward

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Direction string

const (
	DirLocal  Direction = "L"
	DirRemote Direction = "R"
)

// Rule is one parsed `forwards:` entry.
type Rule struct {
	Raw        string
	Dir        Direction
	Device     string
	Bind       string
	ListenPort int
	DestHost   string
	DestPort   int
}

// ParseAll parses every rule string and rejects duplicate listen tuples.
func ParseAll(in []string) ([]Rule, error) {
	out := make([]Rule, 0, len(in))
	seen := map[string]string{}
	for _, s := range in {
		r, err := Parse(s)
		if err != nil {
			return nil, err
		}
		key := keyOf(r)
		if prev, ok := seen[key]; ok {
			return nil, fmt.Errorf("forward rule: duplicate listen tuple %q (also from %q)", s, prev)
		}
		seen[key] = s
		out = append(out, r)
	}
	return out, nil
}

func keyOf(r Rule) string {
	if r.Dir == DirLocal {
		return fmt.Sprintf("L:%s:%d", r.Bind, r.ListenPort)
	}
	return fmt.Sprintf("R:%s:%s:%d", r.Device, r.Bind, r.ListenPort)
}

func Parse(s string) (Rule, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Rule{}, fmt.Errorf("forward rule: empty")
	}
	parts := strings.SplitN(s, " ", 2)
	if len(parts) != 2 {
		return Rule{}, fmt.Errorf("forward rule: missing spec in %q", s)
	}
	dir := Direction(parts[0])
	spec := strings.TrimSpace(parts[1])
	if strings.ContainsAny(spec, " \t") {
		return Rule{}, fmt.Errorf("forward rule: unexpected whitespace in spec %q", s)
	}
	switch dir {
	case DirLocal:
		return parseLocal(s, spec)
	case DirRemote:
		return parseRemote(s, spec)
	default:
		return Rule{}, fmt.Errorf("forward rule: unknown direction %q (want L|R)", parts[0])
	}
}

// parseLocal: [bind:]port:device:[host:]port
func parseLocal(raw, spec string) (Rule, error) {
	fields, err := splitHostPort(spec)
	if err != nil {
		return Rule{}, fmt.Errorf("forward rule %q: %w", raw, err)
	}
	r := Rule{Raw: raw, Dir: DirLocal, Bind: "127.0.0.1", DestHost: "localhost"}
	switch len(fields) {
	case 3:
		if err := setPort(&r.ListenPort, fields[0], raw); err != nil {
			return Rule{}, err
		}
		r.Device = fields[1]
		if err := setPort(&r.DestPort, fields[2], raw); err != nil {
			return Rule{}, err
		}
	case 4:
		if p, err := portOf(fields[0]); err == nil {
			r.ListenPort = p
			r.Device = fields[1]
			r.DestHost = fields[2]
			if err := setPort(&r.DestPort, fields[3], raw); err != nil {
				return Rule{}, err
			}
		} else {
			r.Bind = fields[0]
			if err := setPort(&r.ListenPort, fields[1], raw); err != nil {
				return Rule{}, err
			}
			r.Device = fields[2]
			if err := setPort(&r.DestPort, fields[3], raw); err != nil {
				return Rule{}, err
			}
		}
	case 5:
		r.Bind = fields[0]
		if err := setPort(&r.ListenPort, fields[1], raw); err != nil {
			return Rule{}, err
		}
		r.Device = fields[2]
		r.DestHost = fields[3]
		if err := setPort(&r.DestPort, fields[4], raw); err != nil {
			return Rule{}, err
		}
	default:
		return Rule{}, fmt.Errorf("forward rule %q: wrong number of fields (%d)", raw, len(fields))
	}
	if r.Device == "" {
		return Rule{}, fmt.Errorf("forward rule %q: empty device", raw)
	}
	return r, nil
}

// parseRemote: device:[bind:]port:[host:]port
func parseRemote(raw, spec string) (Rule, error) {
	fields, err := splitHostPort(spec)
	if err != nil {
		return Rule{}, fmt.Errorf("forward rule %q: %w", raw, err)
	}
	r := Rule{Raw: raw, Dir: DirRemote, Bind: "127.0.0.1", DestHost: "localhost"}
	switch len(fields) {
	case 3:
		r.Device = fields[0]
		if err := setPort(&r.ListenPort, fields[1], raw); err != nil {
			return Rule{}, err
		}
		if err := setPort(&r.DestPort, fields[2], raw); err != nil {
			return Rule{}, err
		}
	case 4:
		r.Device = fields[0]
		if p, err := portOf(fields[1]); err == nil {
			r.ListenPort = p
			r.DestHost = fields[2]
			if err := setPort(&r.DestPort, fields[3], raw); err != nil {
				return Rule{}, err
			}
		} else {
			r.Bind = fields[1]
			if err := setPort(&r.ListenPort, fields[2], raw); err != nil {
				return Rule{}, err
			}
			if err := setPort(&r.DestPort, fields[3], raw); err != nil {
				return Rule{}, err
			}
		}
	case 5:
		r.Device = fields[0]
		r.Bind = fields[1]
		if err := setPort(&r.ListenPort, fields[2], raw); err != nil {
			return Rule{}, err
		}
		r.DestHost = fields[3]
		if err := setPort(&r.DestPort, fields[4], raw); err != nil {
			return Rule{}, err
		}
	default:
		return Rule{}, fmt.Errorf("forward rule %q: wrong number of fields (%d)", raw, len(fields))
	}
	if r.Device == "" {
		return Rule{}, fmt.Errorf("forward rule %q: empty device", raw)
	}
	return r, nil
}

// splitHostPort splits a colon-delimited spec, respecting [ipv6]:port brackets.
func splitHostPort(s string) ([]string, error) {
	var out []string
	i := 0
	for i < len(s) {
		if s[i] == '[' {
			end := strings.IndexByte(s[i:], ']')
			if end < 0 {
				return nil, fmt.Errorf("unterminated [")
			}
			out = append(out, s[i+1:i+end])
			i += end + 1
			if i < len(s) && s[i] != ':' {
				return nil, fmt.Errorf("expected ':' after ']'")
			}
			if i < len(s) {
				i++
			}
			continue
		}
		j := strings.IndexByte(s[i:], ':')
		if j < 0 {
			out = append(out, s[i:])
			break
		}
		out = append(out, s[i:i+j])
		i += j + 1
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty spec")
	}
	for _, f := range out {
		if f == "" {
			return nil, fmt.Errorf("empty field")
		}
	}
	return out, nil
}

func portOf(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 65535 {
		return 0, fmt.Errorf("bad port %q", s)
	}
	return n, nil
}

func setPort(dst *int, s, raw string) error {
	p, err := portOf(s)
	if err != nil {
		return fmt.Errorf("forward rule %q: %w", raw, err)
	}
	*dst = p
	return nil
}

// ListenAddr formats `Bind:ListenPort`, bracketing IPv6 literals.
func (r Rule) ListenAddr() string {
	return net.JoinHostPort(r.Bind, strconv.Itoa(r.ListenPort))
}

// DestAddr formats `DestHost:DestPort`, bracketing IPv6 literals.
func (r Rule) DestAddr() string {
	return net.JoinHostPort(r.DestHost, strconv.Itoa(r.DestPort))
}
