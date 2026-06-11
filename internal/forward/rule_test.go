package forward

import (
	"strings"
	"testing"
)

func TestParseRule(t *testing.T) {
	cases := []struct {
		in   string
		want Rule
	}{
		{"L 9000:mac:5037",
			Rule{Dir: DirLocal, Bind: "127.0.0.1", ListenPort: 9000, Device: "mac", DestHost: "localhost", DestPort: 5037}},
		{"L 0.0.0.0:9000:mac:5037",
			Rule{Dir: DirLocal, Bind: "0.0.0.0", ListenPort: 9000, Device: "mac", DestHost: "localhost", DestPort: 5037}},
		{"L 9000:mac:192.168.1.5:5037",
			Rule{Dir: DirLocal, Bind: "127.0.0.1", ListenPort: 9000, Device: "mac", DestHost: "192.168.1.5", DestPort: 5037}},
		{"L [::1]:9000:mac:db.local:5432",
			Rule{Dir: DirLocal, Bind: "::1", ListenPort: 9000, Device: "mac", DestHost: "db.local", DestPort: 5432}},
		{"R mac:8080:3000",
			Rule{Dir: DirRemote, Device: "mac", Bind: "127.0.0.1", ListenPort: 8080, DestHost: "localhost", DestPort: 3000}},
		{"R mac:0.0.0.0:8080:3000",
			Rule{Dir: DirRemote, Device: "mac", Bind: "0.0.0.0", ListenPort: 8080, DestHost: "localhost", DestPort: 3000}},
		{"R mac:8080:db.local:5432",
			Rule{Dir: DirRemote, Device: "mac", Bind: "127.0.0.1", ListenPort: 8080, DestHost: "db.local", DestPort: 5432}},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := Parse(tc.in)
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			tc.want.Raw = tc.in
			if got != tc.want {
				t.Fatalf("got %+v want %+v", got, tc.want)
			}
		})
	}
}

func TestParseRuleErrors(t *testing.T) {
	bad := []string{
		"",
		"X 1:a:2",               // unknown direction
		"L only-two:fields",     // too few colons
		"L 0:mac:5037",          // port 0
		"L 65536:mac:5037",      // port out of range
		"L abc:mac:5037",        // non-numeric port
		"R mac:8080",            // missing dest port for R
		"L 9000mac5037",         // no colons
		"L 9000:mac:5037 extra", // trailing junk
	}
	for _, s := range bad {
		t.Run(s, func(t *testing.T) {
			_, err := Parse(s)
			if err == nil {
				t.Fatalf("expected error for %q", s)
			}
			if !strings.Contains(err.Error(), "forward rule") {
				t.Fatalf("error should mention 'forward rule': %v", err)
			}
		})
	}
}

func TestParseAllDuplicateListen(t *testing.T) {
	_, err := ParseAll([]string{
		"L 9000:mac:5037",
		"L 9000:linux:22",
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("want duplicate-listen error, got %v", err)
	}
}
