package service

import "testing"

func TestResolve(t *testing.T) {
	cases := []struct {
		name      string
		requested Scope
		goos      string
		want      Scope
		wantErr   bool
	}{
		{"auto linux", ScopeAuto, "linux", ScopeUser, false},
		{"auto darwin", ScopeAuto, "darwin", ScopeUser, false},
		{"auto windows", ScopeAuto, "windows", ScopeSystem, false},
		{"explicit system linux", ScopeSystem, "linux", ScopeSystem, false},
		{"explicit user linux", ScopeUser, "linux", ScopeUser, false},
		{"explicit system windows", ScopeSystem, "windows", ScopeSystem, false},
		{"explicit user windows errors", ScopeUser, "windows", 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Resolve(c.requested, c.goos)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error, got scope %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}
