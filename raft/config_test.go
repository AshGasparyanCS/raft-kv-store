package raft

import (
	"bytes"
	"encoding/gob"
	"sync"
	"testing"
	"time"
)

// config is an in-process cluster used to test election, replication, and
// recovery under simulated node failures and partitions.
type config struct {
	mu       sync.Mutex
	t        *testing.T
	n        int
	net      *MemNetwork
	rafts    []*Raft
	persist  []*MemPersister
	applyChs []chan ApplyMsg
	logs     []map[int]interface{} // per server: index -> applied command
	maxIndex int
	snapshot bool // exercise log compaction
}

func makeConfig(t *testing.T, n int, snapshot bool) *config {
	cfg := &config{
		t:        t,
		n:        n,
		net:      NewMemNetwork(),
		rafts:    make([]*Raft, n),
		persist:  make([]*MemPersister, n),
		applyChs: make([]chan ApplyMsg, n),
		logs:     make([]map[int]interface{}, n),
		snapshot: snapshot,
	}
	for i := 0; i < n; i++ {
		cfg.persist[i] = NewMemPersister()
		cfg.logs[i] = map[int]interface{}{}
		cfg.start(i)
	}
	return cfg
}

func peerIDs(n int) []int {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	return p
}

func (cfg *config) start(i int) {
	cfg.applyChs[i] = make(chan ApplyMsg, 1000)
	rf := Make(peerIDs(cfg.n), i, cfg.net.Transport(i), cfg.persist[i], cfg.applyChs[i])
	cfg.rafts[i] = rf
	cfg.net.Add(i, rf)
	go cfg.applyReader(i, cfg.applyChs[i])
}

// applyReader records applied commands, checks cross-server agreement, and (in
// snapshot mode) compacts the log periodically.
func (cfg *config) applyReader(i int, ch chan ApplyMsg) {
	for m := range ch {
		if m.SnapshotValid {
			cfg.mu.Lock()
			cfg.logs[i] = decodeSnap(m.Snapshot)
			cfg.mu.Unlock()
			continue
		}
		if !m.CommandValid {
			continue
		}
		cfg.mu.Lock()
		for j := 0; j < cfg.n; j++ {
			if other, ok := cfg.logs[j][m.CommandIndex]; ok && other != m.Command {
				cfg.mu.Unlock()
				cfg.t.Fatalf("apply mismatch at index %d: %v vs %v", m.CommandIndex, other, m.Command)
			}
		}
		cfg.logs[i][m.CommandIndex] = m.Command
		if m.CommandIndex > cfg.maxIndex {
			cfg.maxIndex = m.CommandIndex
		}
		needSnap := cfg.snapshot && cfg.persist[i].StateSize() > 1000
		snap := encodeSnap(cfg.logs[i])
		rf := cfg.rafts[i]
		cfg.mu.Unlock()
		if needSnap && rf != nil {
			rf.Snapshot(m.CommandIndex, snap)
		}
	}
}

func encodeSnap(m map[int]interface{}) []byte {
	var b bytes.Buffer
	_ = gob.NewEncoder(&b).Encode(m)
	return b.Bytes()
}

func decodeSnap(data []byte) map[int]interface{} {
	m := map[int]interface{}{}
	if len(data) > 0 {
		_ = gob.NewDecoder(bytes.NewBuffer(data)).Decode(&m)
	}
	return m
}

func (cfg *config) disconnect(i int) { cfg.net.SetEnabled(i, false) }
func (cfg *config) connect(i int)    { cfg.net.SetEnabled(i, true) }

// crashRestart simulates a power-cycle: kill the node, then bring it back from
// its persisted state with a fresh in-memory state machine.
func (cfg *config) crashRestart(i int) {
	cfg.rafts[i].Kill()
	cfg.net.SetEnabled(i, false)
	cfg.mu.Lock()
	cfg.logs[i] = map[int]interface{}{}
	cfg.mu.Unlock()
	cfg.start(i)
	cfg.net.SetEnabled(i, true)
}

func (cfg *config) cleanup() {
	for _, rf := range cfg.rafts {
		if rf != nil {
			rf.Kill()
		}
	}
}

// checkOneLeader returns the id of the unique leader in the highest term, or
// fails if there are zero or multiple leaders.
func (cfg *config) checkOneLeader() int {
	for tries := 0; tries < 10; tries++ {
		time.Sleep(450 * time.Millisecond)
		leaders := map[int][]int{}
		for i := 0; i < cfg.n; i++ {
			if term, isLeader := cfg.rafts[i].State(); isLeader {
				leaders[term] = append(leaders[term], i)
			}
		}
		lastTerm := -1
		for term := range leaders {
			if len(leaders[term]) > 1 {
				cfg.t.Fatalf("term %d has %d leaders", term, len(leaders[term]))
			}
			if term > lastTerm {
				lastTerm = term
			}
		}
		if lastTerm >= 0 && len(leaders[lastTerm]) == 1 {
			return leaders[lastTerm][0]
		}
	}
	cfg.t.Fatalf("no leader elected")
	return -1
}

// nCommitted returns how many servers have applied index, and the command.
func (cfg *config) nCommitted(index int) (int, interface{}) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	count := 0
	var cmd interface{}
	for i := 0; i < cfg.n; i++ {
		if c, ok := cfg.logs[i][index]; ok {
			count++
			cmd = c
		}
	}
	return count, cmd
}

// one submits cmd, waits for it to commit on at least expected servers, and
// returns the index it landed at. Retries across leadership changes.
func (cfg *config) one(cmd interface{}, expected int) int {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		// A partitioned old leader still reports isLeader but can't commit, so
		// try every server that claims leadership until one actually commits.
		for i := 0; i < cfg.n; i++ {
			idx, _, isLeader := cfg.rafts[i].Start(cmd)
			if !isLeader {
				continue
			}
			t2 := time.Now().Add(800 * time.Millisecond)
			for time.Now().Before(t2) {
				if cnt, c := cfg.nCommitted(idx); cnt >= expected && c == cmd {
					return idx
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	cfg.t.Fatalf("command %v never committed", cmd)
	return -1
}
