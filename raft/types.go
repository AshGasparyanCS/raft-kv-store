package raft

// LogEntry is a single replicated command at a given term/index.
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// ApplyMsg is delivered to the state machine over applyCh. It carries either a
// committed command (CommandValid) or an installed snapshot (SnapshotValid),
// never both.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// ---- RequestVote ----

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// ---- AppendEntries (heartbeat + replication) ----

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// Fast-backup hints (Raft extended paper §5.3 optimization) so the leader
	// can skip a whole conflicting term in one round instead of one entry/RPC.
	ConflictIndex int
	ConflictTerm  int
}

// ---- InstallSnapshot (log catch-up for far-behind followers) ----

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}
