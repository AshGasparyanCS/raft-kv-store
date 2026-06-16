package raft

import (
	"os"
	"path/filepath"
	"sync"
)

// Persister abstracts durable storage of (a) the raft state needed to recover
// after a crash and (b) the latest snapshot. Two implementations are provided:
// a file-backed one for the real binary and an in-memory one for tests.
type Persister interface {
	SaveStateAndSnapshot(state, snapshot []byte)
	ReadState() []byte
	ReadSnapshot() []byte
	StateSize() int
}

// ---- in-memory (tests) ----

type MemPersister struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

func NewMemPersister() *MemPersister { return &MemPersister{} }

func (p *MemPersister) SaveStateAndSnapshot(state, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
	p.snapshot = append([]byte(nil), snapshot...)
}

func (p *MemPersister) ReadState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.state...)
}

func (p *MemPersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.snapshot...)
}

func (p *MemPersister) StateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}

// Clone copies the persister; used by tests to "crash and restart" a node while
// keeping its on-disk state.
func (p *MemPersister) Clone() *MemPersister {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &MemPersister{
		state:    append([]byte(nil), p.state...),
		snapshot: append([]byte(nil), p.snapshot...),
	}
}

// ---- file-backed (real binary) ----

type FilePersister struct {
	mu  sync.Mutex
	dir string
}

func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FilePersister{dir: dir}, nil
}

func (p *FilePersister) statePath() string    { return filepath.Join(p.dir, "raft-state.bin") }
func (p *FilePersister) snapshotPath() string { return filepath.Join(p.dir, "snapshot.bin") }

// writeAtomic avoids torn files on crash by writing to a temp file then renaming.
func writeAtomic(path string, data []byte) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, path)
}

func (p *FilePersister) SaveStateAndSnapshot(state, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	writeAtomic(p.statePath(), state)
	if snapshot != nil {
		writeAtomic(p.snapshotPath(), snapshot)
	}
}

func (p *FilePersister) ReadState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, _ := os.ReadFile(p.statePath())
	return b
}

func (p *FilePersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, _ := os.ReadFile(p.snapshotPath())
	return b
}

func (p *FilePersister) StateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	fi, err := os.Stat(p.statePath())
	if err != nil {
		return 0
	}
	return int(fi.Size())
}
