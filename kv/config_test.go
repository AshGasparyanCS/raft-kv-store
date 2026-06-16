package kv

import (
	"testing"
	"time"

	"kvraft/raft"
)

type kvConfig struct {
	t        *testing.T
	n        int
	net      *raft.MemNetwork
	servers  []*KVServer
	persist  []*raft.MemPersister
	maxState int
}

func makeKVConfig(t *testing.T, n, maxState int) *kvConfig {
	cfg := &kvConfig{
		t:        t,
		n:        n,
		net:      raft.NewMemNetwork(),
		servers:  make([]*KVServer, n),
		persist:  make([]*raft.MemPersister, n),
		maxState: maxState,
	}
	for i := 0; i < n; i++ {
		cfg.persist[i] = raft.NewMemPersister()
		cfg.start(i)
	}
	return cfg
}

func ids(n int) []int {
	p := make([]int, n)
	for i := range p {
		p[i] = i
	}
	return p
}

func (cfg *kvConfig) start(i int) {
	kv := StartKVServer(ids(cfg.n), i, cfg.net.Transport(i), cfg.persist[i], cfg.maxState)
	cfg.servers[i] = kv
	cfg.net.Add(i, kv.Raft())
}

func (cfg *kvConfig) disconnect(i int) { cfg.net.SetEnabled(i, false) }
func (cfg *kvConfig) connect(i int)    { cfg.net.SetEnabled(i, true) }

func (cfg *kvConfig) crashRestart(i int) {
	cfg.servers[i].Kill()
	cfg.net.SetEnabled(i, false)
	cfg.start(i)
	cfg.net.SetEnabled(i, true)
}

func (cfg *kvConfig) cleanup() {
	for _, kv := range cfg.servers {
		if kv != nil {
			kv.Kill()
		}
	}
}

// testClerk drives the cluster in-process (no TCP), with the same leader
// discovery, retry, and (ClientID, Seq) dedup semantics as the real Clerk.
type testClerk struct {
	cfg      *kvConfig
	clientID int64
	seq      int64
	leader   int
}

func (cfg *kvConfig) makeClerk() *testClerk {
	return &testClerk{cfg: cfg, clientID: nrand()}
}

func (ck *testClerk) op(op Op) string {
	op.ClientID, ck.seq = ck.clientID, ck.seq+1
	op.Seq = ck.seq
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		kv := ck.cfg.servers[ck.leader]
		val, err := kv.submit(op)
		if err == OK || err == ErrNoKey {
			return val
		}
		ck.leader = (ck.leader + 1) % ck.cfg.n
	}
	ck.cfg.t.Fatalf("op %+v never completed", op)
	return ""
}

func (ck *testClerk) Get(key string) string {
	return ck.op(Op{Type: "Get", Key: key})
}
func (ck *testClerk) Put(key, val string) {
	ck.op(Op{Type: "Put", Key: key, Value: val})
}
func (ck *testClerk) Append(key, val string) {
	ck.op(Op{Type: "Append", Key: key, Value: val})
}
