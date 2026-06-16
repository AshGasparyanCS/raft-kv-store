# kvraft — a distributed key-value store with Raft consensus

A replicated, fault-tolerant key-value store written from scratch in Go. The
replication layer is a complete Raft implementation: leader election, log
replication, durable persistence, log compaction via snapshots, and the
`InstallSnapshot` path for catching up far-behind followers. On top of Raft sits
a linearizable key-value state machine with exactly-once client semantics, a CLI
client, and a fault-injection test suite that kills and partitions nodes.

The whole thing has no third-party dependencies — only the Go standard library.

> **Transport note.** The wire layer uses Go's `net/rpc`. The original brief
> asked for gRPC; this was built in a sandbox where the gRPC module host
> (`google.golang.org`) is firewalled, so gRPC stubs can't be fetched or
> compiled there. The Raft and KV layers are transport-agnostic — `proto/kvstore.proto`
> defines the equivalent gRPC service, and swapping it in is a thin-adapter
> change (see [Swapping in gRPC](#swapping-in-grpc)).

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

**Request lifecycle (a write):**

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

Reads (`Get`) go through the log too, so they observe a linearizable point in
the committed history rather than possibly-stale local state.

### Layers

| Layer | Package | Responsibility |
|-------|---------|----------------|
| Consensus | `raft/` | Election, replication, commit, persistence, snapshots. Transport-agnostic. |
| State machine | `kv/` | Applies committed ops; dedup via `(clientID, seq)`; snapshot encode/restore. |
| Transport | `raft/network.go` | `Transport` interface. `MemNetwork` (tests) + `NetTransport` (TCP/`net/rpc`). |
| Persistence | `raft/persister.go` | `FilePersister` (atomic disk writes) + `MemPersister` (tests). |
| Server | `cmd/kvnode` | Wires it together; serves Raft + KV RPCs on one port. |
| Client | `cmd/kvctl`, `kv/client.go` | Leader discovery + automatic retry. |

---

## The consensus protocol

Raft keeps a replicated, append-only **log** identical across servers; applying
that log in order makes every state machine converge. A single elected **leader**
owns all writes at any moment, which makes the protocol far easier to reason
about than leaderless schemes.

### 1. Terms and roles

Time is divided into **terms**, each starting with an election. Every server is a
**Follower**, **Candidate**, or **Leader**. Every RPC carries the sender's term;
seeing a higher term immediately forces a server back to Follower and updates its
term. This single rule is what guarantees stale leaders step down.

### 2. Leader election

A follower that hears nothing from a leader within a randomized timeout
(300–600 ms here) becomes a candidate: it increments its term, votes for itself,
and sends `RequestVote` to everyone. A server grants its vote only if it hasn't
already voted this term **and** the candidate's log is at least as up-to-date as
its own (compared by last-entry term, then index — Raft §5.4.1). Winning a
**majority** makes the candidate leader; randomized timeouts make split votes
rare and self-correcting. *(`raft.go`: `startElection`, `RequestVote`, `becomeLeader`.)*

### 3. Log replication

Clients send commands to the leader, which appends them and sends `AppendEntries`
to each follower. Each entry includes the `prevLogIndex/prevLogTerm` of the entry
before it; a follower rejects the RPC unless that previous entry matches. This
**log-matching property** means a successful append proves the follower's log is
identical to the leader's up to that point. On rejection, the follower returns a
conflict hint so the leader can back up by a whole term in one round instead of
one entry at a time (§5.3 optimization). *(`replicateTo`, `AppendEntries`.)*

### 4. Commitment

An entry is **committed** once it lives on a majority of servers. The leader
tracks `matchIndex[]` per follower and advances `commitIndex` to the highest
index replicated on a majority — but, crucially, only for entries from its
**own current term** (§5.4.2). Committing an older-term entry only indirectly,
by committing a current-term entry above it, is what prevents a subtle
already-committed-then-overwritten bug. Committed entries flow to the state
machine over `applyCh`, in order, by a single applier goroutine. *(`advanceCommit`, `applier`.)*

### 5. Persistence

Before responding to any RPC that changes them, a server flushes `currentTerm`,
`votedFor`, and its `log` to disk (atomic temp-file + rename). After a crash it
reloads these and rejoins seamlessly — the property the `TestPersistence` and
`TestKVRestart` tests exercise by power-cycling every node. *(`persist`, `readPersist`, `FilePersister`.)*

### 6. Snapshots / log compaction

An unbounded log would grow forever. When the state machine's persisted size
crosses a threshold, it serializes its state and calls `Raft.Snapshot(index)`,
which discards all log entries at or below `index`, keeping a sentinel that
records the snapshot's last-included index and term. All log access goes through
offset helpers (`snapIndex`, `entry`, `termAt`) so compaction is invisible to the
rest of the code. A follower so far behind that the needed entries are already
compacted is caught up with `InstallSnapshot` instead of log replay; it installs
the snapshot wholesale and resumes. *(`Snapshot`, `InstallSnapshot`, index helpers.)*

### Exactly-once client semantics

Because clients retry on leader changes, the same write could reach the log
twice. Each request carries `(clientID, seq)`; the state machine remembers the
highest `seq` applied per client and ignores anything not strictly newer, so an
`Append` retried after a failover is never applied twice. The dedup table is
included in snapshots so it survives compaction and restarts. *(`kv/server.go`: `applier`.)*

---

## Build and run

Requires Go 1.21+ (uses the `min` builtin). No external modules.

```bash
make build          # -> bin/kvnode, bin/kvctl
```

Start a 3-node cluster (each node in its own terminal, or use `make cluster`):

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

Now kill whichever node `status` shows as `leader: YES`. Within a second a new
leader is elected, your data is intact, and writes resume — the exact scenario
verified end-to-end in testing.

`kvnode` flags: `--id`, `--peers`, `--listen` (defaults to this node's peers
entry), `--data` (state/snapshot dir), `--maxstate` (snapshot threshold in bytes,
`-1` disables snapshotting).

## Tests

```bash
make test        # full suite
make test-race   # same, under the race detector (clean)
```

| Test | What it simulates |
|------|-------------------|
| `raft/TestElection` | Leader elected; re-elected after the leader is disconnected. |
| `raft/TestReplication` | Commands replicate to every follower at the right index. |
| `raft/TestLeaderFailure` | Cluster keeps committing through a leader crash; old leader catches up. |
| `raft/TestPartition` | Minority partition can't commit; majority does; logs converge on heal. |
| `raft/TestPersistence` | State survives crash-restart of every node. |
| `raft/TestSnapshot` | Far-behind follower caught up via `InstallSnapshot`. |
| `kv/TestKVConcurrentAppends` | 5 clients × 20 appends land exactly once (linearizable, deduped). |
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

Then implement the generated `KVServer`/`RaftServer` interfaces as adapters that
call the existing `kv.KVServer` and `raft.RPCEndpoint` methods, and point
`NetTransport`/`Clerk` at gRPC clients. Nothing in `raft/` or the KV state
machine changes — that separation is the whole point of the `Transport`
interface.

## Known simplifications

Cluster membership is static (no dynamic add/remove), and there's no read-lease
optimization (reads go through the log for linearizability rather than being
served from a leader lease). Both are deliberate scope cuts, not correctness
gaps — the safety-critical parts (election restriction, current-term commit rule,
snapshot index handling) are implemented in full.
