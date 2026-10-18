package node

import (
	"context"
	"testing"
	"time"
)

// Ad-hoc smoke check for task 5: confirms the election timer actually
// fires and transitions the node to Candidate with an incrementing term.
// This is NOT the real task-10/11 test suite (those build real clusters
// and check leader election across multiple nodes over RPC) — this is
// just verifying the timer mechanism in isolation before building on it.
func TestElectionTimerFiresAndBecomesCandidate(t *testing.T) {
	n := NewNode(1, map[int]string{2: "127.0.0.1:9002", 3: "127.0.0.1:9003"})

	if got := n.State(); got != Follower {
		t.Fatalf("new node should start as Follower, got %v", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go n.RunElectionTimer(ctx)

	// electionTimeoutMax is 300ms; give it comfortably longer than that
	// to guarantee at least one timeout fires, without the test being
	// flaky on a slow CI machine.
	deadline := time.After(600 * time.Millisecond)
	for {
		if n.State() == Candidate {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("node never became Candidate within 600ms (timeout range is %v-%v)",
				electionTimeoutMin, electionTimeoutMax)
		case <-time.After(5 * time.Millisecond):
		}
	}

	if term := n.CurrentTerm(); term != 1 {
		t.Fatalf("expected CurrentTerm to be 1 after first election timeout, got %d", term)
	}

	n.mu.Lock()
	votedFor := n.persistent.VotedFor
	n.mu.Unlock()
	if votedFor != n.ID() {
		t.Fatalf("candidate should vote for itself, got VotedFor=%d, node id=%d", votedFor, n.ID())
	}
}