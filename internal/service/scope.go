package service

import "fmt"

// Scope selects whether a service is installed per-user or system-wide.
type Scope int

const (
	ScopeAuto Scope = iota
	ScopeSystem
	ScopeUser
)

// Resolve applies the "user unless Windows" default and rejects --user on
// Windows, which has no per-user service concept.
func Resolve(requested Scope, goos string) (Scope, error) {
	if goos == "windows" {
		if requested == ScopeUser {
			return 0, fmt.Errorf("--user is not supported on Windows")
		}
		return ScopeSystem, nil
	}
	if requested == ScopeAuto {
		return ScopeUser, nil
	}
	return requested, nil
}
