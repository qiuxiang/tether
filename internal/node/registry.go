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
	out := make([]*Process, 0, len(r.procs))
	for _, p := range r.procs {
		if filter == "running" && p.Status != "running" {
			continue
		}
		if filter == "exited" && p.Status != "exited" {
			continue
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastActiveAt.After(out[j].LastActiveAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
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
// NOTE: This reads p.Status and p.LastActiveAt without holding p.mu.
// In practice, eviction only happens during Add, called single-threaded
// from handler code, so the race is theoretical but documented here.
func (r *ProcessRegistry) evictLocked() {
	if len(r.procs) <= r.cap {
		return
	}
	exited := make([]*Process, 0)
	for _, p := range r.procs {
		if p.Status == "exited" {
			exited = append(exited, p)
		}
	}
	sort.Slice(exited, func(i, j int) bool { return exited[i].LastActiveAt.Before(exited[j].LastActiveAt) })
	for len(r.procs) > r.cap && len(exited) > 0 {
		victim := exited[0]
		exited = exited[1:]
		delete(r.procs, victim.ID)
		// Best-effort log cleanup; ignore errors.
		if victim.LogPath != "" {
			_ = removeFile(victim.LogPath)
		}
	}
}
