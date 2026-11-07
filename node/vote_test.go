package node

import (
	"testing"
	"time"
)

// Tests for the receiver side of RequestVote (vote.go): the voting rules of
// the Raft paper's Figure 2 and the up-to-date log restriction of §5.4.1.

// voter builds a Follower in `term` with a log whose entries have the
// given terms, and no vote cast yet.
func voter(term int, logTerms ...int) *Node {
	n := NewNode(1, map[int]string{2: "n2", 3: "n3"}, nil)
	n.persistent.CurrentTerm = term
	n.persistent.Log = entriesWithTerms(logTerms...)
	return n
}

func TestVote_GrantedWhenEligible(t *testing.T) {
	n := voter(1)
	term, granted := n.HandleRequestVote(1, 2, 0, 0)
	if !granted || term != 1 || n.persistent.VotedFor != 2 {
		t.Fatalf("got granted=%v term=%d votedFor=%d", granted, term, n.persistent.VotedFor)
	}
}

func TestVote_RejectsStaleTerm(t *testing.T) {
	n := voter(5)
	term, granted := n.HandleRequestVote(3, 2, 0, 0)
	if granted || term != 5 {
		t.Fatalf("stale candidate: granted=%v, reported term %d (want our term 5)", granted, term)
	}
}

// Only one vote per term: that's what makes a majority meaningful.
func TestVote_RejectsSecondCandidateInSameTerm(t *testing.T) {
	n := voter(1)
	n.HandleRequestVote(1, 2, 0, 0)
	if _, granted := n.HandleRequestVote(1, 3, 0, 0); granted {
		t.Fatal("voted for two candidates in the same term")
	}
}

// If our reply was lost and the same candidate retries, voting again for
// it is harmless and correct.
func TestVote_RegrantsSameCandidateOnRetry(t *testing.T) {
	n := voter(1)
	n.HandleRequestVote(1, 2, 0, 0)
	if _, granted := n.HandleRequestVote(1, 2, 0, 0); !granted {
		t.Fatal("refused a retried request from the candidate we already voted for")
	}
}

// A newer term makes any node, even a Leader, step down and start the new
// term with no vote cast, so it can vote in it.
func TestVote_NewerTermMakesLeaderStepDownAndFreesTheVote(t *testing.T) {
	n := voter(2)
	n.state = Leader
	n.persistent.VotedFor = 1

	term, granted := n.HandleRequestVote(4, 3, 0, 0)
	if !granted || term != 4 || n.state != Follower || n.persistent.VotedFor != 3 {
		t.Fatalf("got granted=%v term=%d state=%v votedFor=%d", granted, term, n.state, n.persistent.VotedFor)
	}
}

// A Leader must NOT step down for a candidate of its own term: that's just
// a rival it refuses (it already voted for itself).
func TestVote_SameTermRequestDoesNotDemoteLeader(t *testing.T) {
	n := voter(3)
	n.state = Leader
	n.persistent.VotedFor = 1
	if _, granted := n.HandleRequestVote(3, 2, 0, 0); granted || n.state != Leader {
		t.Fatalf("granted=%v state=%v; want refusal and still Leader", granted, n.state)
	}
}

// §5.4.1: a candidate whose log is less up-to-date than ours must not get
// our vote, so a node missing committed entries can never become leader.
// "Up-to-date" compares the LAST entry's term first, then log length.
func TestVote_UpToDateLogRestriction(t *testing.T) {
	cases := []struct {
		name          string
		ourLog        []int
		candLastIndex int
		candLastTerm  int
		wantGranted   bool
	}{
		{"candidate's last term is older", []int{1, 2}, 5, 1, false},
		{"same last term, candidate's log is shorter", []int{1, 2, 2}, 2, 2, false},
		{"same last term, same length", []int{1, 2}, 2, 2, true},
		{"same last term, candidate's log is longer", []int{1, 2}, 4, 2, true},
		{"candidate's last term is newer, even if its log is shorter", []int{1, 1, 1, 1}, 1, 3, true},
		{"both logs empty", nil, 0, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := voter(3, c.ourLog...)
			_, granted := n.HandleRequestVote(3, 2, c.candLastIndex, c.candLastTerm)
			if granted != c.wantGranted {
				t.Fatalf("granted = %v, want %v", granted, c.wantGranted)
			}
		})
	}
}

// Granting a vote postpones our own election; refusing one doesn't (a
// partitioned node retrying hopeless requests must not keep resetting a
// healthy cluster's timers).
func TestVote_OnlyAGrantedVoteResetsTheElectionTimer(t *testing.T) {
	n := voter(1)
	// Start from a reset time far in the past. Comparing against the time
	// the node was created instead would be flaky on Windows, whose clock
	// can return the same instant for calls made microseconds apart.
	longAgo := time.Now().Add(-time.Hour)
	n.electionResetEvent = longAgo

	n.HandleRequestVote(0, 2, 0, 0) // stale: refused
	if !n.electionResetEvent.Equal(longAgo) {
		t.Fatal("a refused vote reset the election timer")
	}
	n.HandleRequestVote(1, 2, 0, 0) // granted
	if !n.electionResetEvent.After(longAgo) {
		t.Fatal("a granted vote did not reset the election timer")
	}
}
