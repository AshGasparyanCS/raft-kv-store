package raft

import (
	"net/rpc"
	"sync"
	"time"
)

// Transport is how a Raft node reaches its peers. Returning ok=false means the
// RPC did not complete (peer down, partitioned, or timed out) — Raft treats that
// as "retry later", which is exactly the dropped-message model the algorithm is
// designed to tolerate.
type Transport interface {
	RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, bool)
	AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, bool)
	InstallSnapshot(to int, args *InstallSnapshotArgs) (*InstallSnapshotReply, bool)
}

// ============================================================
// In-memory network (used by the test harness to simulate
// crashes and partitions deterministically and fast).
// ============================================================

type MemNetwork struct {
	mu      sync.Mutex
	nodes   map[int]*Raft
	enabled map[int]bool
}

func NewMemNetwork() *MemNetwork {
	return &MemNetwork{nodes: map[int]*Raft{}, enabled: map[int]bool{}}
}

func (n *MemNetwork) Add(id int, rf *Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = rf
	n.enabled[id] = true
}

// SetEnabled connects/disconnects a node from the network (simulates a crash or
// a partition without destroying its persisted state).
func (n *MemNetwork) SetEnabled(id int, up bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.enabled[id] = up
}

func (n *MemNetwork) reachable(from, to int) (*Raft, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.enabled[from] || !n.enabled[to] {
		return nil, false
	}
	rf, ok := n.nodes[to]
	return rf, ok
}

func (n *MemNetwork) Transport(from int) Transport { return &memTransport{net: n, from: from} }

type memTransport struct {
	net  *MemNetwork
	from int
}

func (t *memTransport) RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	peer, ok := t.net.reachable(t.from, to)
	if !ok {
		return nil, false
	}
	reply := &RequestVoteReply{}
	peer.RequestVote(args, reply)
	return reply, true
}

func (t *memTransport) AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	peer, ok := t.net.reachable(t.from, to)
	if !ok {
		return nil, false
	}
	reply := &AppendEntriesReply{}
	peer.AppendEntries(args, reply)
	return reply, true
}

func (t *memTransport) InstallSnapshot(to int, args *InstallSnapshotArgs) (*InstallSnapshotReply, bool) {
	peer, ok := t.net.reachable(t.from, to)
	if !ok {
		return nil, false
	}
	reply := &InstallSnapshotReply{}
	peer.InstallSnapshot(args, reply)
	return reply, true
}

// ============================================================
// Real network transport over Go's net/rpc (TCP).
// The same three handler methods are exposed remotely via RPCEndpoint.
// ============================================================

// RPCEndpoint adapts a *Raft into net/rpc method signatures
// (func(args, *reply) error) and is registered under the name "RaftRPC".
type RPCEndpoint struct{ rf *Raft }

func NewRPCEndpoint(rf *Raft) *RPCEndpoint { return &RPCEndpoint{rf: rf} }

func (e *RPCEndpoint) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	e.rf.RequestVote(args, reply)
	return nil
}
func (e *RPCEndpoint) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	e.rf.AppendEntries(args, reply)
	return nil
}
func (e *RPCEndpoint) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	e.rf.InstallSnapshot(args, reply)
	return nil
}

// NetTransport dials peers lazily over TCP and caches one client per peer.
type NetTransport struct {
	mu      sync.Mutex
	peers   []string // index = peer id, value = host:port ("" for self)
	clients map[int]*rpc.Client
}

func NewNetTransport(peers []string) *NetTransport {
	return &NetTransport{peers: peers, clients: map[int]*rpc.Client{}}
}

func (t *NetTransport) client(to int) *rpc.Client {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[to]; ok {
		return c
	}
	c, err := rpc.Dial("tcp", t.peers[to])
	if err != nil {
		return nil
	}
	t.clients[to] = c
	return c
}

func (t *NetTransport) drop(to int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[to]; ok {
		_ = c.Close()
		delete(t.clients, to)
	}
}

// call runs an RPC with a timeout so a hung peer can't stall the caller.
func (t *NetTransport) call(to int, method string, args, reply interface{}) bool {
	c := t.client(to)
	if c == nil {
		return false
	}
	done := make(chan error, 1)
	go func() { done <- c.Call(method, args, reply) }()
	select {
	case err := <-done:
		if err != nil {
			t.drop(to)
			return false
		}
		return true
	case <-time.After(300 * time.Millisecond):
		t.drop(to)
		return false
	}
}

func (t *NetTransport) RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	reply := &RequestVoteReply{}
	return reply, t.call(to, "RaftRPC.RequestVote", args, reply)
}
func (t *NetTransport) AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	reply := &AppendEntriesReply{}
	return reply, t.call(to, "RaftRPC.AppendEntries", args, reply)
}
func (t *NetTransport) InstallSnapshot(to int, args *InstallSnapshotArgs) (*InstallSnapshotReply, bool) {
	reply := &InstallSnapshotReply{}
	return reply, t.call(to, "RaftRPC.InstallSnapshot", args, reply)
}
