package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	default:
		return "follower"
	}
}

const (
	heartbeatInterval = 100 * time.Millisecond
	electionMin       = 300 * time.Millisecond
	electionMax       = 600 * time.Millisecond
)

// Raft is one replica in the consensus group.
type Raft struct {
	mu        sync.Mutex
	me        int
	peers     []int // ids of all servers, including self
	transport Transport
	persister Persister
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// Persistent state (survives crashes).
	currentTerm int
	votedFor    int // -1 = none
	// log[0] is a sentinel holding lastIncludedIndex/Term of the snapshot;
	// real entries are log[1:]. Absolute index i -> slice pos i-log[0].Index.
	log []LogEntry

	// Volatile state.
	role        Role
	commitIndex int
	lastApplied int
	electionAt  time.Time // when the current election timeout fires

	// Leader-only volatile state, reset on each election.
	nextIndex  []int // per peer: next log index to send
	matchIndex []int // per peer: highest index known replicated

	pendingSnapshot *ApplyMsg // snapshot waiting to be handed to the state machine
	dead            bool
}

// ---- log index helpers (account for the snapshot offset) ----

func (rf *Raft) snapIndex() int { return rf.log[0].Index }
func (rf *Raft) snapTerm() int  { return rf.log[0].Term }
func (rf *Raft) lastIndex() int { return rf.log[0].Index + len(rf.log) - 1 }

func (rf *Raft) entry(i int) LogEntry { return rf.log[i-rf.snapIndex()] }

func (rf *Raft) termAt(i int) int {
	if i < rf.snapIndex() {
		return -1 // compacted away
	}
	return rf.log[i-rf.snapIndex()].Term
}

func (rf *Raft) lastLogTerm() int { return rf.log[len(rf.log)-1].Term }

// Make creates and starts a Raft replica. peers lists every server id (0..n-1);
// me is this server's id. transport is how it reaches peers; persister holds its
// durable state; committed entries are delivered on applyCh.
func Make(peers []int, me int, transport Transport, persister Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		me:          me,
		peers:       peers,
		transport:   transport,
		persister:   persister,
		applyCh:     applyCh,
		votedFor:    -1,
		role:        Follower,
		log:         []LogEntry{{Term: 0, Index: 0}}, // sentinel
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		commitIndex: 0,
		lastApplied: 0,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.ReadState())
	rf.resetElectionTimer()

	go rf.electionLoop()
	go rf.applier()
	return rf
}

// ---- persistence (gob-encoded) ----

func (rf *Raft) encodeState() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(rf.currentTerm)
	_ = enc.Encode(rf.votedFor)
	_ = enc.Encode(rf.log)
	return buf.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), rf.persister.ReadSnapshot())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	_ = dec.Decode(&rf.currentTerm)
	_ = dec.Decode(&rf.votedFor)
	_ = dec.Decode(&rf.log)
	// After recovery, everything up to the snapshot is already applied.
	rf.commitIndex = rf.snapIndex()
	rf.lastApplied = rf.snapIndex()
}

// ---- role transitions (caller holds rf.mu) ----

func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
}

func (rf *Raft) resetElectionTimer() {
	d := electionMin + time.Duration(rand.Int63n(int64(electionMax-electionMin)))
	rf.electionAt = time.Now().Add(d)
}

func (rf *Raft) majority() int { return len(rf.peers)/2 + 1 }

func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.dead = true
	rf.mu.Unlock()
	rf.applyCond.Broadcast()
}

func (rf *Raft) killed() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.dead
}

// State reports the current term and whether this server believes it is leader.
func (rf *Raft) State() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// ============================================================
// Leader election
// ============================================================

func (rf *Raft) electionLoop() {
	for !rf.killed() {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		if rf.role != Leader && time.Now().After(rf.electionAt) {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// startElection runs with rf.mu held.
func (rf *Raft) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	rf.persist()

	term := rf.currentTerm
	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	votes := 1 // self
	for _, peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply, ok := rf.transport.RequestVote(peer, args)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != term || rf.role != Candidate {
				return // stale reply
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				rf.persist()
				return
			}
			if reply.VoteGranted {
				votes++
				if votes >= rf.majority() {
					rf.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeLeader runs with rf.mu held.
func (rf *Raft) becomeLeader() {
	if rf.role != Candidate {
		return
	}
	rf.role = Leader
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastIndex() + 1
		rf.matchIndex[i] = 0
	}
	go rf.replicationLoop(rf.currentTerm)
}

// RequestVote handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	reply.Term = rf.currentTerm

	upToDate := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastIndex())
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}
}

// ============================================================
// Log replication
// ============================================================

// Start appends a command to the leader's log and kicks off replication.
// Returns the index it will occupy, the current term, and whether this server
// is the leader (if not, the caller must redirect to the leader).
func (rf *Raft) Start(command interface{}) (index, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}
	index = rf.lastIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Index: index, Command: command})
	rf.persist()
	go rf.broadcastAppend(term)
	return index, term, true
}

func (rf *Raft) replicationLoop(term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.broadcastAppend(term)
		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) broadcastAppend(term int) {
	for _, peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicateTo(peer, term)
	}
}

// replicateTo sends one AppendEntries (or InstallSnapshot) to a single peer.
func (rf *Raft) replicateTo(peer, term int) {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	next := rf.nextIndex[peer]

	// Follower is behind the snapshot boundary — send the snapshot instead.
	if next <= rf.snapIndex() {
		args := &InstallSnapshotArgs{
			Term:              term,
			LeaderID:          rf.me,
			LastIncludedIndex: rf.snapIndex(),
			LastIncludedTerm:  rf.snapTerm(),
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()
		reply, ok := rf.transport.InstallSnapshot(peer, args)
		if !ok {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm != term || rf.role != Leader {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.persist()
			return
		}
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
		return
	}

	prevIndex := next - 1
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  rf.termAt(prevIndex),
		LeaderCommit: rf.commitIndex,
		Entries:      append([]LogEntry(nil), rf.log[next-rf.snapIndex():]...),
	}
	rf.mu.Unlock()

	reply, ok := rf.transport.AppendEntries(peer, args)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != term || rf.role != Leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.persist()
		return
	}
	if reply.Success {
		rf.matchIndex[peer] = prevIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.advanceCommit(term)
		return
	}
	// Failed consistency check — back nextIndex up using the conflict hint.
	if reply.ConflictTerm == -1 {
		rf.nextIndex[peer] = reply.ConflictIndex
	} else {
		// Find the leader's last entry with ConflictTerm; jump past it.
		idx := -1
		for i := rf.lastIndex(); i > rf.snapIndex(); i-- {
			if rf.termAt(i) == reply.ConflictTerm {
				idx = i
				break
			}
		}
		if idx >= 0 {
			rf.nextIndex[peer] = idx + 1
		} else {
			rf.nextIndex[peer] = reply.ConflictIndex
		}
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
}

// advanceCommit moves commitIndex forward to the highest N replicated on a
// majority, but only for entries from the current term (Raft §5.4.2).
func (rf *Raft) advanceCommit(term int) {
	for n := rf.lastIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != term {
			continue
		}
		count := 1 // self
		for _, peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= n {
				count++
			}
		}
		if count >= rf.majority() {
			rf.commitIndex = n
			rf.applyCond.Broadcast()
			break
		}
	}
}

// AppendEntries handler (heartbeat + replication + commit propagation).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.ConflictIndex, reply.ConflictTerm = -1, -1

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	rf.role = Follower
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// PrevLog points into compacted history — let the leader send a snapshot.
	if args.PrevLogIndex < rf.snapIndex() {
		reply.Success = false
		reply.ConflictIndex = rf.snapIndex() + 1
		return
	}
	if args.PrevLogIndex > rf.lastIndex() {
		reply.Success = false
		reply.ConflictIndex = rf.lastIndex() + 1
		return
	}
	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.termAt(args.PrevLogIndex)
		// First index of that conflicting term.
		i := args.PrevLogIndex
		for i > rf.snapIndex() && rf.termAt(i-1) == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// Merge entries: keep the common prefix, overwrite on first divergence.
	for j, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + j
		if idx <= rf.lastIndex() && rf.termAt(idx) == e.Term {
			continue
		}
		rf.log = append(rf.log[:idx-rf.snapIndex()], args.Entries[j:]...)
		break
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastIndex())
		rf.applyCond.Broadcast()
	}
	reply.Success = true
}

// ============================================================
// Apply loop: hand committed entries / snapshots to the state machine.
// ============================================================

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !rf.dead && rf.pendingSnapshot == nil && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.dead {
			rf.mu.Unlock()
			return
		}
		if rf.pendingSnapshot != nil {
			msg := *rf.pendingSnapshot
			rf.pendingSnapshot = nil
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}
		// Apply the next batch of committed commands.
		var msgs []ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			if rf.lastApplied <= rf.snapIndex() {
				continue
			}
			e := rf.entry(rf.lastApplied)
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: e.Index,
			})
		}
		rf.mu.Unlock()
		for _, m := range msgs {
			rf.applyCh <- m
		}
	}
}

// ============================================================
// Snapshotting / log compaction
// ============================================================

// Snapshot is called by the state machine once it has durably captured all state
// through index. Raft discards log entries up to and including index.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapIndex() || index > rf.commitIndex {
		return
	}
	keepTerm := rf.termAt(index)
	rf.log = append([]LogEntry{{Term: keepTerm, Index: index}}, rf.log[index-rf.snapIndex()+1:]...)
	rf.persistWithSnapshot(snapshot)
}

// InstallSnapshot handler: a far-behind follower adopts the leader's snapshot.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.persist()
	}
	rf.role = Follower
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// Stale or already-covered snapshot.
	if args.LastIncludedIndex <= rf.snapIndex() {
		return
	}

	// Trim the log to start at the snapshot point; keep any entries beyond it
	// if they match, otherwise reset to just the sentinel.
	if args.LastIncludedIndex < rf.lastIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		rf.log = append([]LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}},
			rf.log[args.LastIncludedIndex-rf.snapIndex()+1:]...)
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}

	rf.persistWithSnapshot(args.Data)
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCond.Broadcast()
}
