package kv

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
	"sync"
	"time"
)

// Clerk is a client for the replicated store. It hides leader discovery and
// retries: callers just see Get/Put/Append that always eventually succeed as
// long as a majority of servers is alive.
type Clerk struct {
	mu       sync.Mutex
	servers  []string // host:port of every replica
	clients  map[int]*rpc.Client
	leader   int   // last server known to be leader (a hint)
	clientID int64 // unique per Clerk
	seq      int64 // monotonically increasing request id
}

func nrand() int64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	return n.Int64()
}

func MakeClerk(servers []string) *Clerk {
	return &Clerk{
		servers:  servers,
		clients:  map[int]*rpc.Client{},
		clientID: nrand(),
	}
}

func (ck *Clerk) client(i int) *rpc.Client {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if c, ok := ck.clients[i]; ok {
		return c
	}
	c, err := rpc.Dial("tcp", ck.servers[i])
	if err != nil {
		return nil
	}
	ck.clients[i] = c
	return c
}

func (ck *Clerk) drop(i int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if c, ok := ck.clients[i]; ok {
		_ = c.Close()
		delete(ck.clients, i)
	}
}

func (ck *Clerk) nextSeq() int64 {
	ck.seq++
	return ck.seq
}

// Get returns the value for key, or "" if it does not exist.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{Key: key, ClientID: ck.clientID, Seq: ck.nextSeq()}
	for {
		i := ck.leader
		reply := &GetReply{}
		if c := ck.client(i); c != nil {
			if err := c.Call("KV.Get", args, reply); err == nil {
				switch reply.Err {
				case OK:
					return reply.Value
				case ErrNoKey:
					return ""
				}
			} else {
				ck.drop(i)
			}
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) putAppend(key, value, op string) {
	args := &PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, Seq: ck.nextSeq()}
	for {
		i := ck.leader
		reply := &PutAppendReply{}
		if c := ck.client(i); c != nil {
			if err := c.Call("KV.PutAppend", args, reply); err == nil && reply.Err == OK {
				return
			} else if err != nil {
				ck.drop(i)
			}
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key, value string)    { ck.putAppend(key, value, "Put") }
func (ck *Clerk) Append(key, value string) { ck.putAppend(key, value, "Append") }
func (ck *Clerk) Delete(key string)        { ck.putAppend(key, "", "Delete") }
