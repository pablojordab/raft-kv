package node

import (
	"testing"
	"time"
)

// Tests for the client-facing pieces of node: tracking who the leader is
// (used to redirect clients) and waiting for a submitted command to
// commit.

func TestLeaderID_LearnedFromAppendEntriesAndClearedOnNewTerm(t *testing.T) {
	n := NewNode(1, map[int]string{2: "n2", 3: "n3"}, nil)
	if got := n.LeaderID(); got != -1 {
		t.Fatalf("new node should know no leader, got %d", got)
	}

	n.HandleAppendEntries(1, 3, 0, 0, nil, 0)
	if got := n.LeaderID(); got != 3 {
		t.Fatalf("leader = %d after node 3's heartbeat, want 3", got)
	}

	// A stale leader's heartbeat is rejected and must not change the hint.
	n.HandleAppendEntries(0, 2, 0, 0, nil, 0)
	if got := n.LeaderID(); got != 3 {
		t.Fatalf("stale heartbeat changed the leader hint to %d", got)
	}

	// Learning of a newer term means the old leader is no longer leader.
	n.HandleRequestVote(5, 2, 0, 0)
	if got := n.LeaderID(); got != -1 {
		t.Fatalf("leader hint should be cleared on a newer term, got %d", got)
	}
}

func TestLeaderID_SelfAfterWinningElection(t *testing.T) {
	n := NewNode(1, map[int]string{}, nil)
	n.mu.Lock()
	n.leaderID = 7 // stale hint from before; winning must replace it
	n.persistent.CurrentTerm = 1
	n.state = Candidate
	n.becomeLeaderLocked(t.Context())
	got := n.leaderID
	n.mu.Unlock()
	if got != 1 {
		t.Fatalf("a leader should name itself as leader, got %d", got)
	}
}

// leaderWithSilentPeer builds a Leader with one peer that never replies,
// so nothing commits unless the test commits it by hand.
func leaderWithSilentPeer() *Node {
	n := NewNode(1, map[int]string{2: "n2"}, nil)
	n.state = Leader
	n.persistent.CurrentTerm = 2
	n.volatile.NextIndex = map[int]int{2: 1}
	n.volatile.MatchIndex = map[int]int{2: 0}
	return n
}

func TestWaitForCommit_ReturnsTrueOnceCommitted(t *testing.T) {
	n := leaderWithSilentPeer()
	index, term, _ := n.Submit([]byte("SET x 1"))

	go func() {
		time.Sleep(30 * time.Millisecond)
		n.mu.Lock()
		n.volatile.CommitIndex = index
		n.mu.Unlock()
	}()

	if !n.WaitForCommit(index, term, time.Second) {
		t.Fatal("expected the entry to be reported committed")
	}
}

func TestWaitForCommit_TimesOutWhenNotCommitted(t *testing.T) {
	n := leaderWithSilentPeer()
	index, term, _ := n.Submit([]byte("SET x 1"))

	start := time.Now()
	if n.WaitForCommit(index, term, 50*time.Millisecond) {
		t.Fatal("nothing committed it, yet WaitForCommit returned true")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("timeout not respected: waited %v", elapsed)
	}
}

// The slot our command was in gets committed, but holding a DIFFERENT
// entry written by a newer leader: our command was lost and must not be
// reported as committed.
func TestWaitForCommit_FalseIfEntryWasOverwritten(t *testing.T) {
	n := leaderWithSilentPeer()
	index, term, _ := n.Submit([]byte("SET x 1"))

	n.mu.Lock()
	n.persistent.Log[index-1] = LogEntry{Term: term + 1, Index: index, Command: []byte("SET y 2")}
	n.volatile.CommitIndex = index
	n.mu.Unlock()

	if n.WaitForCommit(index, term, time.Second) {
		t.Fatal("reported a lost (overwritten) command as committed")
	}
}

func TestWaitForCommit_FalseIfEntryWasTruncated(t *testing.T) {
	n := leaderWithSilentPeer()
	index, term, _ := n.Submit([]byte("SET x 1"))

	n.mu.Lock()
	n.persistent.Log = n.persistent.Log[:0]
	n.mu.Unlock()

	start := time.Now()
	if n.WaitForCommit(index, term, time.Second) {
		t.Fatal("reported a truncated command as committed")
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("should return as soon as the entry is gone, not wait for the timeout")
	}
}
