package kv

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"

	"kvraft/raft"
)

// Error / status codes returned to clients.
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

// Op is the command replicated through Raft. Every client write carries a
// (ClientID, Seq) pair so the state machine can apply each request exactly once
// even if the client retries after a leader change.
type Op struct {
	Type     string // "Get" | "Put" | "Append"
	Key      string
	Value    string
	ClientID int64
	Seq      int64
}

func init() { gob.Register(Op{}) }

type opResult struct {
	clientID int64
	seq      int64
	value    string
	err      string
}

// ---- client-facing RPC types ----

type GetArgs struct {
	Key      string
	ClientID int64
	Seq      int64
}
type GetReply struct {
	Err   string
	Value string
}
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" | "Append"
	ClientID int64
	Seq      int64
}
type PutAppendReply struct{ Err string }

// Status lets a client inspect a replica's role for diagnostics.
type StatusArgs struct{}
type StatusReply struct {
	Term     int
	IsLeader bool
	Applied  int
	Keys     int
}

// KVServer is a replicated key-value store backed by one Raft replica.
type KVServer struct {
	mu        sync.Mutex
	rf        *raft.Raft
	persister raft.Persister
	applyCh   chan raft.ApplyMsg

	maxRaftState int // snapshot when persisted raft state exceeds this (-1 = off)

	store       map[string]string
	lastApplied map[int64]int64    // clientID -> highest applied Seq (dedup)
	waiters     map[int]chan opResult // raft log index -> waiting RPC
	appliedIdx  int
	dead        bool
}

// StartKVServer builds a Raft replica and the KV state machine on top of it.
func StartKVServer(peers []int, me int, transport raft.Transport, persister raft.Persister, maxRaftState int) *KVServer {
	kv := &KVServer{
		persister:    persister,
		applyCh:      make(chan raft.ApplyMsg, 1000),
		maxRaftState: maxRaftState,
		store:        map[string]string{},
		lastApplied:  map[int64]int64{},
		waiters:      map[int]chan opResult{},
	}
	kv.restoreSnapshot(persister.ReadSnapshot())
	kv.rf = raft.Make(peers, me, transport, persister, kv.applyCh)
	go kv.applier()
	return kv
}

// Raft exposes the underlying replica so the network layer can serve its RPCs.
func (kv *KVServer) Raft() *raft.Raft { return kv.rf }

func (kv *KVServer) Kill() {
	kv.mu.Lock()
	kv.dead = true
	kv.mu.Unlock()
	kv.rf.Kill()
}

// submit replicates op through Raft and blocks until it is applied (or this
// server loses leadership / times out).
func (kv *KVServer) submit(op Op) (string, string) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrWrongLeader
	}
	ch := make(chan opResult, 1)
	kv.mu.Lock()
	kv.waiters[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waiters, index)
		kv.mu.Unlock()
	}()

	select {
	case res := <-ch:
		// If a different command committed at this index, we lost leadership
		// mid-flight; tell the client to find the new leader and retry.
		if res.clientID != op.ClientID || res.seq != op.Seq {
			return "", ErrWrongLeader
		}
		return res.value, res.err
	case <-time.After(time.Second):
		return "", ErrTimeout
	}
}

// Get RPC handler.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	value, err := kv.submit(Op{Type: "Get", Key: args.Key, ClientID: args.ClientID, Seq: args.Seq})
	reply.Value, reply.Err = value, err
	return nil
}

// PutAppend RPC handler.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	_, err := kv.submit(Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientID: args.ClientID, Seq: args.Seq})
	reply.Err = err
	return nil
}

// Status RPC handler (diagnostics, used by the CLI).
func (kv *KVServer) Status(args *StatusArgs, reply *StatusReply) error {
	term, isLeader := kv.rf.State()
	kv.mu.Lock()
	reply.Applied, reply.Keys = kv.appliedIdx, len(kv.store)
	kv.mu.Unlock()
	reply.Term, reply.IsLeader = term, isLeader
	return nil
}

// applier consumes committed entries and snapshots from Raft, updates the state
// machine, wakes waiting RPCs, and compacts the log when it grows too large.
func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if m.SnapshotValid {
			kv.mu.Lock()
			kv.restoreSnapshot(m.Snapshot)
			kv.appliedIdx = m.SnapshotIndex
			kv.mu.Unlock()
			continue
		}
		if !m.CommandValid {
			continue
		}
		kv.mu.Lock()
		op, ok := m.Command.(Op)
		if !ok {
			kv.mu.Unlock()
			continue
		}
		res := opResult{clientID: op.ClientID, seq: op.Seq, err: OK}

		switch op.Type {
		case "Get":
			if v, found := kv.store[op.Key]; found {
				res.value = v
			} else {
				res.err = ErrNoKey
			}
		default: // Put / Append / Delete — apply once per (client, seq)
			if op.Seq > kv.lastApplied[op.ClientID] {
				switch op.Type {
				case "Put":
					kv.store[op.Key] = op.Value
				case "Append":
					kv.store[op.Key] += op.Value
				case "Delete":
					delete(kv.store, op.Key)
				}
				kv.lastApplied[op.ClientID] = op.Seq
			}
		}
		kv.appliedIdx = m.CommandIndex

		ch := kv.waiters[m.CommandIndex]
		needSnap := kv.maxRaftState > 0 && kv.persister.StateSize() >= kv.maxRaftState
		var snap []byte
		if needSnap {
			snap = kv.encodeSnapshot()
		}
		kv.mu.Unlock()

		if ch != nil {
			ch <- res
		}
		if needSnap {
			kv.rf.Snapshot(m.CommandIndex, snap)
		}
	}
}

// ---- snapshot encode/decode (state machine state) ----

func (kv *KVServer) encodeSnapshot() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(kv.store)
	_ = enc.Encode(kv.lastApplied)
	return buf.Bytes()
}

func (kv *KVServer) restoreSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	store := map[string]string{}
	last := map[int64]int64{}
	_ = dec.Decode(&store)
	_ = dec.Decode(&last)
	kv.store = store
	kv.lastApplied = last
}
