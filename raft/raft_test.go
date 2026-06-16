package raft

import (
	"testing"
	"time"
)

// A leader is elected from a healthy cluster, and re-elected after the current
// leader is disconnected.
func TestElection(t *testing.T) {
	cfg := makeConfig(t, 5, false)
	defer cfg.cleanup()

	leader1 := cfg.checkOneLeader()

	cfg.disconnect(leader1)
	leader2 := cfg.checkOneLeader()
	if leader2 == leader1 {
		t.Fatalf("disconnected leader %d still leads", leader1)
	}

	// Reconnect the old leader; the cluster must still have exactly one leader.
	cfg.connect(leader1)
	cfg.checkOneLeader()
}

// Commands committed by the leader are replicated to all followers.
func TestReplication(t *testing.T) {
	cfg := makeConfig(t, 3, false)
	defer cfg.cleanup()
	cfg.checkOneLeader()

	for i := 1; i <= 10; i++ {
		idx := cfg.one(i*100, cfg.n)
		if idx != i {
			t.Fatalf("expected command at index %d, got %d", i, idx)
		}
	}
}

// The cluster keeps committing through a leader failure, and the failed node
// catches up when it returns.
func TestLeaderFailure(t *testing.T) {
	cfg := makeConfig(t, 5, false)
	defer cfg.cleanup()

	cfg.one(1, cfg.n)
	leader := cfg.checkOneLeader()

	// Kill the leader; the remaining 4 must elect a new one and keep going.
	cfg.disconnect(leader)
	cfg.one(2, cfg.n-1)
	cfg.one(3, cfg.n-1)

	// Old leader returns and must catch up to the full history.
	cfg.connect(leader)
	idx := cfg.one(4, cfg.n)

	time.Sleep(time.Second)
	if cnt, _ := cfg.nCommitted(idx); cnt != cfg.n {
		t.Fatalf("only %d/%d servers caught up", cnt, cfg.n)
	}
}

// A minority partition cannot commit; once healed, its writes are discarded in
// favor of the majority's and everyone converges.
func TestPartition(t *testing.T) {
	cfg := makeConfig(t, 5, false)
	defer cfg.cleanup()
	cfg.one(1, cfg.n)

	leader := cfg.checkOneLeader()
	// Isolate the leader with one follower (a minority of 2).
	others := []int{}
	for i := 0; i < cfg.n; i++ {
		if i != leader {
			others = append(others, i)
		}
	}
	minorityFollower := others[0]
	cfg.disconnect(leader)
	cfg.disconnect(minorityFollower)

	// Majority side (3 nodes) keeps committing.
	cfg.one(2, 3)
	cfg.one(3, 3)

	// Heal the partition; the whole cluster converges on the majority's log.
	cfg.connect(leader)
	cfg.connect(minorityFollower)
	idx := cfg.one(4, cfg.n)
	time.Sleep(time.Second)
	if cnt, _ := cfg.nCommitted(idx); cnt != cfg.n {
		t.Fatalf("cluster failed to converge: %d/%d", cnt, cfg.n)
	}
}

// Persisted state survives a crash/restart so a node rejoins with its log intact.
func TestPersistence(t *testing.T) {
	cfg := makeConfig(t, 3, false)
	defer cfg.cleanup()

	cfg.one(10, cfg.n)
	cfg.one(20, cfg.n)

	// Crash-restart every node in turn; data must survive each cycle.
	for i := 0; i < cfg.n; i++ {
		cfg.crashRestart(i)
		time.Sleep(500 * time.Millisecond)
	}
	idx := cfg.one(30, cfg.n)
	time.Sleep(500 * time.Millisecond)
	if cnt, _ := cfg.nCommitted(idx); cnt != cfg.n {
		t.Fatalf("data lost across restarts: %d/%d", cnt, cfg.n)
	}
}

// A node that falls far behind is caught up via InstallSnapshot rather than a
// full log replay.
func TestSnapshot(t *testing.T) {
	cfg := makeConfig(t, 3, true)
	defer cfg.cleanup()
	cfg.checkOneLeader()

	// Take one follower offline, then commit far more than the snapshot
	// threshold so the leader compacts past where the follower left off.
	leader := cfg.checkOneLeader()
	behind := (leader + 1) % cfg.n
	cfg.disconnect(behind)

	for i := 0; i < 200; i++ {
		cfg.one(i, cfg.n-1)
	}

	// Reconnect; the follower can only catch up via a snapshot, not log replay.
	cfg.connect(behind)
	idx := cfg.one(99999, cfg.n)

	time.Sleep(2 * time.Second)
	if cnt, _ := cfg.nCommitted(idx); cnt != cfg.n {
		t.Fatalf("snapshot catch-up failed: %d/%d", cnt, cfg.n)
	}
}
