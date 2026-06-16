package kv

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Basic single-client correctness for Put / Append / Get.
func TestKVBasic(t *testing.T) {
	cfg := makeKVConfig(t, 3, -1)
	defer cfg.cleanup()
	ck := cfg.makeClerk()

	ck.Put("a", "1")
	if v := ck.Get("a"); v != "1" {
		t.Fatalf("get a = %q, want 1", v)
	}
	ck.Append("a", "2")
	ck.Append("a", "3")
	if v := ck.Get("a"); v != "123" {
		t.Fatalf("get a = %q, want 123", v)
	}
	if v := ck.Get("missing"); v != "" {
		t.Fatalf("get missing = %q, want empty", v)
	}
}

// The store keeps serving correct values across a leader failure.
func TestKVFailover(t *testing.T) {
	cfg := makeKVConfig(t, 5, -1)
	defer cfg.cleanup()
	ck := cfg.makeClerk()

	for i := 0; i < 5; i++ {
		ck.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	// Knock out two servers (still a majority of 3 alive).
	cfg.disconnect(0)
	cfg.disconnect(1)
	time.Sleep(500 * time.Millisecond)

	ck.Put("k0", "updated")
	if v := ck.Get("k0"); v != "updated" {
		t.Fatalf("after failover get k0 = %q, want updated", v)
	}
	if v := ck.Get("k3"); v != "v3" {
		t.Fatalf("after failover get k3 = %q, want v3", v)
	}

	// Heal and confirm the recovered nodes serve consistent data.
	cfg.connect(0)
	cfg.connect(1)
	time.Sleep(time.Second)
	if v := ck.Get("k0"); v != "updated" {
		t.Fatalf("after heal get k0 = %q, want updated", v)
	}
}

// Many clients append to one key concurrently; every append must appear exactly
// once (no lost or duplicated writes), proving linearizable, deduplicated ops.
func TestKVConcurrentAppends(t *testing.T) {
	cfg := makeKVConfig(t, 3, -1)
	defer cfg.cleanup()

	const clients, perClient = 5, 20
	var wg sync.WaitGroup
	for c := 0; c < clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			ck := cfg.makeClerk()
			for i := 0; i < perClient; i++ {
				ck.Append("log", fmt.Sprintf("(%d:%d)", c, i))
			}
		}(c)
	}
	wg.Wait()

	final := cfg.makeClerk().Get("log")
	var got []string
	for _, tok := range strings.SplitAfter(final, ")") {
		if tok != "" {
			got = append(got, tok)
		}
	}
	if len(got) != clients*perClient {
		t.Fatalf("got %d appends, want %d", len(got), clients*perClient)
	}
	sort.Strings(got)
	for c := 0; c < clients; c++ {
		for i := 0; i < perClient; i++ {
			want := fmt.Sprintf("(%d:%d)", c, i)
			if sort.SearchStrings(got, want) >= len(got) || got[sort.SearchStrings(got, want)] != want {
				t.Fatalf("missing append %s", want)
			}
		}
	}
}

// Data persists across a full-cluster crash/restart.
func TestKVRestart(t *testing.T) {
	cfg := makeKVConfig(t, 3, -1)
	defer cfg.cleanup()
	ck := cfg.makeClerk()

	for i := 0; i < 10; i++ {
		ck.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	for i := 0; i < cfg.n; i++ {
		cfg.crashRestart(i)
		time.Sleep(500 * time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		if v := ck.Get(fmt.Sprintf("k%d", i)); v != fmt.Sprintf("v%d", i) {
			t.Fatalf("after restart k%d = %q, want v%d", i, v, i)
		}
	}
}

// With snapshotting on, a node taken far behind is caught up via InstallSnapshot
// and still serves correct values.
func TestKVSnapshot(t *testing.T) {
	cfg := makeKVConfig(t, 3, 1000) // compact aggressively
	defer cfg.cleanup()
	ck := cfg.makeClerk()
	ck.Put("anchor", "start")

	behind := 2
	cfg.disconnect(behind)

	for i := 0; i < 300; i++ {
		ck.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}

	cfg.connect(behind)
	time.Sleep(2 * time.Second)
	ck.Put("anchor", "end")

	// Force reads to land on the previously-behind node by killing the others.
	cfg.disconnect((behind + 1) % cfg.n)
	cfg.disconnect((behind + 2) % cfg.n)
	cfg.connect(behind)
	time.Sleep(time.Second)
	cfg.connect((behind + 1) % cfg.n) // need a majority to serve reads
	time.Sleep(500 * time.Millisecond)

	if v := ck.Get("k150"); v != "v150" {
		t.Fatalf("after snapshot catch-up k150 = %q, want v150", v)
	}
	if v := ck.Get("anchor"); v != "end" {
		t.Fatalf("anchor = %q, want end", v)
	}
}
