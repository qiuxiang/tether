package node

import (
	"sort"
	"sync"
	"time"
)

type ProcessRegistry struct {
	mu    sync.Mutex
	cap   int
	procs map[string]*Process
}

func NewProcessRegistry(cap int) *ProcessRegistry {
	return &ProcessRegistry{cap: cap, procs: make(map[string]*Process)}
}

func (r *ProcessRegistry) Add(p *Process) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.procs[p.ID] = p
	r.evictLocked()
}

func (r *ProcessRegistry) Get(id string) (*Process, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.procs[id]
	return p, ok
}

func (r *ProcessRegistry) List(filter string, limit int) []*Process {
	r.mu.Lock()
	defer r.mu.Unlock()

	type entry struct {
		p          *Process
		status     string
		lastActive time.Time
	}
	entries := make([]entry, 0, len(r.procs))
	for _, p := range r.procs {
		// Snapshot volatile fields under p.mu to avoid a data race with the
		// goroutine that writes Status/ExitCode/LastActiveAt on process exit.
		p.mu.Lock()
		e := entry{p: p, status: p.Status, lastActive: p.LastActiveAt}
		p.mu.Unlock()

		if filter == "running" && e.status != "running" {
			continue
		}
		if filter == "exited" && e.status != "exited" {
			continue
		}
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].lastActive.After(entries[j].lastActive) })
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	out := make([]*Process, len(entries))
	for i, e := range entries {
		out[i] = e.p
	}
	return out
}

func (r *ProcessRegistry) Touch(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.procs[id]; ok {
		p.LastActiveAt = time.Now()
	}
}

// evictLocked drops the oldest exited entries until size <= cap.
// Running entries are never evicted, even if they're the oldest.
func (r *ProcessRegistry) evictLocked() {
	if len(r.procs) <= r.cap {
		return
	}
	type exitedEntry struct {
		p          *Process
		lastActive time.Time
	}
	exited := make([]exitedEntry, 0)
	for _, p := range r.procs {
		// Snapshot under p.mu to avoid a data race with the exit goroutine.
		p.mu.Lock()
		status := p.Status
		lastActive := p.LastActiveAt
		p.mu.Unlock()
		if status == "exited" {
			exited = append(exited, exitedEntry{p: p, lastActive: lastActive})
		}
	}
	sort.Slice(exited, func(i, j int) bool { return exited[i].lastActive.Before(exited[j].lastActive) })
	for len(r.procs) > r.cap && len(exited) > 0 {
		victim := exited[0].p
		exited = exited[1:]
		delete(r.procs, victim.ID)
		// Best-effort log cleanup; ignore errors.
		if victim.LogPath != "" {
			_ = removeFile(victim.LogPath)
		}
	}
}
