# kvraft: a distributed key-value store with Raft consensus

![tests](https://github.com/AshGasparyanCS/raft-kv-store/actions/workflows/tests.yml/badge.svg)

A replicated, fault-tolerant key-value store I wrote from scratch in Go. The replication layer is a full Raft implementation: leader election, log replication, durable persistence, log compaction via snapshots, and the `InstallSnapshot` path for catching up followers that have fallen way behind. On top of Raft there's a linearizable key-value state machine with exactly-once client semantics, a CLI client, and a test suite that kills and partitions nodes to make sure none of it breaks.

No third-party dependencies, just the Go standard library.

> **Transport note.** The wire layer uses Go's `net/rpc`. The original brief asked for gRPC, but I built this in a sandbox where the gRPC module host (`google.golang.org`) is firewalled, so the gRPC stubs can't be fetched or compiled there. The Raft and KV layers don't care what the transport is. `proto/kvstore.proto` defines the equivalent gRPC service, and swapping it in is a thin adapter change (see [Swapping in gRPC](#swapping-in-grpc)).

---

## Architecture

```
                         clients (kvctl / Clerk)
                                   |
                  get / put / append / delete / status
                                   |
          +------------------------+------------------------+
          |                        |                        |
   +-------------+          +-------------+          +-------------+
   |  kvnode 0   |          |  kvnode 1   |          |  kvnode 2   |
   |             |          |  (LEADER)   |          |             |
   |  KV state   |          |  KV state   |          |  KV state   |
   |  machine    |          |  machine    |          |  machine    |
   |   store{}   |          |   store{}   |          |   store{}   |
   |   dedup{}   |          |   dedup{}   |          |   dedup{}   |
   +------+------+          +------+------+          +------+------+
          |   applyCh              |   applyCh              |   applyCh
   +------+------+          +------+------+          +------+------+
   |    Raft     |<-------->|    Raft     |<-------->|    Raft     |
   |  replica    | AppendEntries / RequestVote /     |  replica    |
   |             |          InstallSnapshot          |             |
   +------+------+          +------+------+          +------+------+
          |                        |                        |
   +------+------+          +------+------+          +------+------+
   |  Persister  |          |  Persister  |          |  Persister  |
   | state + snap|          | state + snap|          | state + snap|
   |   (disk)    |          |   (disk)    |          |   (disk)    |
   +-------------+          +-------------+          +-------------+
```

**What happens on a write:**

```
client --Put(k,v)--> leader.KV.PutAppend
   leader wraps it as an Op{Put,k,v,clientID,seq}
   leader.Raft.Start(op) -> appended to leader log at index N
   leader replicates entry N to followers via AppendEntries
   once a MAJORITY has entry N, leader advances commitIndex to N
   each replica's apply loop delivers entry N on applyCh
   KV state machine applies the Op to store{}, records seq for dedup
   leader's waiting RPC handler is signalled -> returns OK to client
```

Reads (`Get`) go through the log too, so they see a linearizable point in the committed history instead of possibly-stale local state.

### Layers

| Layer | Package | What it does |
|-------|---------|----------------|
| Consensus | `raft/` | Election, replication, commit, persistence, snapshots. Doesn't know or care about the transport. |
| State machine | `kv/` | Applies committed ops, dedups via `(clientID, seq)`, snapshot encode/restore. |
| Transport | `raft/network.go` | A `Transport` interface. `MemNetwork` for tests, `NetTransport` (TCP/`net/rpc`) for real runs. |
| Persistence | `raft/persister.go` | `FilePersister` (atomic disk writes) and `MemPersister` (tests). |
| Server | `cmd/kvnode` | Wires it all together and serves Raft + KV RPCs on one port. |
| Client | `cmd/kvctl`, `kv/client.go` | Finds the leader, retries automatically. |

---

## How the consensus actually works

Raft keeps an append-only **log** that's identical across servers, and applying that log in order makes every state machine end up in the same place. One elected **leader** owns all writes at any given moment, which makes the whole thing much easier to reason about than leaderless designs.

### 1. Terms and roles

Time is split into **terms**, each kicked off by an election. Every server is a **Follower**, **Candidate**, or **Leader**. Every RPC carries the sender's term, and seeing a higher term immediately knocks a server back to Follower and bumps its term. That one rule is what makes stale leaders step down.

### 2. Leader election

A follower that hasn't heard from a leader within a randomized timeout (300 to 600 ms here) becomes a candidate: it bumps its term, votes for itself, and sends `RequestVote` to everyone. A server grants its vote only if it hasn't already voted this term and the candidate's log is at least as up to date as its own (compared by last-entry term, then index, per Raft section 5.4.1). Win a **majority** and you're the leader. Randomized timeouts keep split votes rare and self-correcting. (`raft.go`: `startElection`, `RequestVote`, `becomeLeader`.)

### 3. Log replication

Clients send commands to the leader, which appends them and sends `AppendEntries` to each follower. Each entry includes the `prevLogIndex/prevLogTerm` of the entry before it, and a follower rejects the RPC unless that previous entry matches. This is the **log-matching property**: a successful append proves the follower's log is identical to the leader's up to that point. On a rejection the follower returns a conflict hint, so the leader can back up a whole term in one round instead of crawling back one entry at a time (the section 5.3 optimization). (`replicateTo`, `AppendEntries`.)

### 4. Commitment

An entry is **committed** once it lives on a majority of servers. The leader tracks `matchIndex[]` per follower and advances `commitIndex` to the highest index replicated on a majority, but only for entries from its **own current term** (section 5.4.2). Committing an older-term entry only indirectly, by committing a current-term entry above it, is what avoids a nasty already-committed-then-overwritten bug. Committed entries flow to the state machine over `applyCh`, in order, from a single applier goroutine. (`advanceCommit`, `applier`.)

### 5. Persistence

Before responding to any RPC that changes them, a server flushes `currentTerm`, `votedFor`, and its `log` to disk (atomic temp file plus rename). After a crash it reloads these and rejoins like nothing happened. That's the property `TestPersistence` and `TestKVRestart` hammer on by power-cycling every node. (`persist`, `readPersist`, `FilePersister`.)

### 6. Snapshots and log compaction

An unbounded log grows forever, which isn't great. When the state machine's persisted size crosses a threshold, it serializes its state and calls `Raft.Snapshot(index)`, which throws away all log entries at or below `index` and keeps a sentinel recording the snapshot's last-included index and term. All log access goes through offset helpers (`snapIndex`, `entry`, `termAt`), so compaction is invisible to the rest of the code. A follower so far behind that the entries it needs are already gone gets caught up with `InstallSnapshot` instead of log replay: it installs the snapshot wholesale and carries on. (`Snapshot`, `InstallSnapshot`, the index helpers.)

### Exactly-once client semantics

Since clients retry on leader changes, the same write can reach the log twice. Each request carries `(clientID, seq)`, the state machine remembers the highest `seq` it has applied per client, and it ignores anything that isn't strictly newer. So an `Append` retried after a failover never gets applied twice. The dedup table rides along in snapshots, so it survives compaction and restarts. (`kv/server.go`: `applier`.)

---

## Build and run

Needs Go 1.21+ (it uses the `min` builtin). No external modules.

```bash
make build          # -> bin/kvnode, bin/kvctl
```

Start a 3-node cluster (each node in its own terminal, or just `make cluster`):

```bash
./bin/kvnode --id 0 --peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --data ./data0
./bin/kvnode --id 1 --peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --data ./data1
./bin/kvnode --id 2 --peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --data ./data2
```

Drive it with the CLI:

```bash
P=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002
./bin/kvctl --peers $P status
./bin/kvctl --peers $P put name ada
./bin/kvctl --peers $P append name " lovelace"
./bin/kvctl --peers $P get name          # -> ada lovelace
./bin/kvctl --peers $P delete name
```

Now kill whichever node `status` shows as `leader: YES`. Within a second a new leader is elected, your data is still there, and writes keep working. That's the exact scenario the tests check end to end.

`kvnode` flags: `--id`, `--peers`, `--listen` (defaults to this node's peers entry), `--data` (state/snapshot dir), `--maxstate` (snapshot threshold in bytes, `-1` turns snapshotting off).

## Tests

```bash
make test        # full suite
make test-race   # same thing under the race detector (clean)
```

| Test | What it simulates |
|------|-------------------|
| `raft/TestElection` | Leader elected, then re-elected after the leader is disconnected. |
| `raft/TestReplication` | Commands replicate to every follower at the right index. |
| `raft/TestLeaderFailure` | Cluster keeps committing through a leader crash, old leader catches up. |
| `raft/TestPartition` | Minority partition can't commit, majority does, logs converge once it heals. |
| `raft/TestPersistence` | State survives a crash-restart of every node. |
| `raft/TestSnapshot` | A far-behind follower gets caught up via `InstallSnapshot`. |
| `kv/TestKVConcurrentAppends` | 5 clients times 20 appends each land exactly once (linearizable, deduped). |
| `kv/TestKVFailover` / `TestKVRestart` / `TestKVSnapshot` | End-to-end correctness under the same failures. |

## Project layout

```
raft/      consensus core (types, raft, network, persister) + tests
kv/        key-value state machine + client + tests
cmd/kvnode server binary
cmd/kvctl  CLI client
proto/     gRPC service definitions (for the migration path)
```

## Swapping in gRPC

On a machine with normal network access:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
protoc --go_out=. --go-grpc_out=. proto/kvstore.proto
```

Then implement the generated `KVServer`/`RaftServer` interfaces as adapters that call the existing `kv.KVServer` and `raft.RPCEndpoint` methods, and point `NetTransport`/`Clerk` at gRPC clients. Nothing in `raft/` or the KV state machine has to change, which is the whole reason the `Transport` interface exists.

## Things I deliberately left out

Cluster membership is static (no dynamic add/remove), and there's no read-lease optimization (reads go through the log for linearizability instead of being served from a leader lease). Both are scope cuts, not correctness gaps. The safety-critical parts (election restriction, current-term commit rule, snapshot index handling) are all implemented in full.
