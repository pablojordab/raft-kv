package node

import (
	"fmt"
	"testing"
	"time"
)

// Tests for the receiver side of AppendEntries (append.go): the five
// receiver rules of the Raft paper's Figure 2, including the §5.3 log
// consistency check and the conflict hints returned on a mismatch.

// followerWith builds a Follower in `term` whose log entries have the given
// terms.
func followerWith(term int, logTerms ...int) *Node {
	n := NewNode(1, map[int]string{2: "n2", 3: "n3"}, nil)
	n.persistent.CurrentTerm = term
	n.persistent.Log = entriesWithTerms(logTerms...)
	return n
}

// logTerms renders a log as its terms, e.g. "[1 1 3]".
func logTerms(n *Node) string {
	terms := make([]int, len(n.persistent.Log))
	for i, e := range n.persistent.Log {
		terms[i] = e.Term
	}
	return fmt.Sprint(terms)
}

func requireIndexesConsistent(t *testing.T, n *Node) {
	t.Helper()
	for k, e := range n.persistent.Log {
		if e.Index != k+1 {
			t.Fatalf("Log[%d].Index = %d, want %d", k, e.Index, k+1)
		}
	}
}

// Rule 1.
func TestAppend_RejectsStaleLeader(t *testing.T) {
	n := followerWith(5, 1, 1)
	res := n.HandleAppendEntries(4, 2, 2, 1, entriesWithTerms(4), 0)
	if res.Success || res.Term != 5 || logTerms(n) != "[1 1]" {
		t.Fatalf("stale leader: %+v, log %s", res, logTerms(n))
	}
}

// Rule 2: no entry at prevLogIndex at all.
func TestAppend_RejectsWhenPrevEntryIsMissing(t *testing.T) {
	n := followerWith(2, 1)
	res := n.HandleAppendEntries(2, 2, 3, 1, nil, 0)
	if res.Success {
		t.Fatal("accepted with prevLogIndex beyond our log")
	}
	if res.ConflictTerm != 0 || res.ConflictIndex != 2 {
		t.Fatalf("hint = (term %d, index %d), want (0, 2): our log ends at 1", res.ConflictTerm, res.ConflictIndex)
	}
}

// Rule 2: an entry exists at prevLogIndex but from a different term.
func TestAppend_RejectsWhenPrevTermDiffers(t *testing.T) {
	n := followerWith(3, 1, 2, 2)
	res := n.HandleAppendEntries(3, 2, 3, 3, nil, 0)
	if res.Success || logTerms(n) != "[1 2 2]" {
		t.Fatalf("accepted a mismatching prevLogTerm or changed the log: %+v %s", res, logTerms(n))
	}
	if res.ConflictTerm != 2 || res.ConflictIndex != 2 {
		t.Fatalf("hint = (term %d, index %d), want (2, 2): our term-2 run starts at 2", res.ConflictTerm, res.ConflictIndex)
	}
}

// Even when rule 2 fails, the sender is the legitimate leader: a Candidate
// must give up and the election timer must be reset.
func TestAppend_RejectionStillRecognizesTheLeader(t *testing.T) {
	n := followerWith(3, 1)
	n.state = Candidate
	// Start from a reset time far in the past, so "the timer was reset"
	// doesn't depend on the clock's resolution (coarse on Windows).
	longAgo := time.Now().Add(-time.Hour)
	n.electionResetEvent = longAgo
	if res := n.HandleAppendEntries(3, 2, 5, 3, nil, 0); res.Success {
		t.Fatal("expected a rejection")
	}
	if n.state != Follower || !n.electionResetEvent.After(longAgo) || n.leaderID != 2 {
		t.Fatalf("state=%v leaderID=%d timerReset=%v", n.state, n.leaderID, n.electionResetEvent.After(longAgo))
	}
}

func TestAppend_AppendsToAnEmptyLog(t *testing.T) {
	n := followerWith(1)
	res := n.HandleAppendEntries(1, 2, 0, 0, entriesWithTerms(1, 1), 0)
	if !res.Success || logTerms(n) != "[1 1]" || string(n.persistent.Log[1].Command) != "cmd2" {
		t.Fatalf("%+v %+v", res, n.persistent.Log)
	}
	requireIndexesConsistent(t, n)
}

func TestAppend_HeartbeatLeavesTheLogUntouched(t *testing.T) {
	n := followerWith(2, 1, 2, 2)
	if res := n.HandleAppendEntries(2, 2, 3, 2, nil, 0); !res.Success || logTerms(n) != "[1 2 2]" {
		t.Fatalf("%+v %s", res, logTerms(n))
	}
}

// Rule 3: an uncommitted tail from a dead leader's term is replaced.
func TestAppend_ConflictTruncatesTheRestOfTheLog(t *testing.T) {
	n := followerWith(3, 1, 1, 2, 2)
	res := n.HandleAppendEntries(3, 2, 2, 1, []LogEntry{{Term: 3, Command: []byte("x")}}, 0)
	if !res.Success || logTerms(n) != "[1 1 3]" {
		t.Fatalf("%+v, log %s, want [1 1 3]", res, logTerms(n))
	}
	requireIndexesConsistent(t, n)
}

// A delayed RPC carrying entries we already have must not truncate the
// newer entries after them (they may already be committed).
func TestAppend_StaleReorderedRPCDoesNotTruncate(t *testing.T) {
	n := followerWith(1, 1, 1, 1, 1)
	if res := n.HandleAppendEntries(1, 2, 1, 1, entriesWithTerms(1), 0); !res.Success || len(n.persistent.Log) != 4 {
		t.Fatalf("an old RPC truncated the log to %d entries", len(n.persistent.Log))
	}
}

// Rule 4: of a batch that overlaps our log, only the new part is appended.
func TestAppend_PartialOverlapAppendsOnlyTheNewEntries(t *testing.T) {
	n := followerWith(2, 1, 2)
	n.HandleAppendEntries(2, 2, 1, 1, []LogEntry{{Term: 2}, {Term: 2}, {Term: 2}}, 0)
	if logTerms(n) != "[1 2 2 2]" {
		t.Fatalf("log %s, want [1 2 2 2]", logTerms(n))
	}
	requireIndexesConsistent(t, n)
}

// Rule 5: commitIndex = min(leaderCommit, index of last new entry).
func TestAppend_CommitIndexIsMinOfLeaderCommitAndLastNewEntry(t *testing.T) {
	n := followerWith(1)
	n.HandleAppendEntries(1, 2, 0, 0, entriesWithTerms(1, 1), 10)
	if n.volatile.CommitIndex != 2 {
		t.Fatalf("commit = %d, want min(10, 2) = 2", n.volatile.CommitIndex)
	}
}

// A heartbeat only verifies our log up to prevLogIndex; what follows could
// be a stale tail, so it must not be marked committed.
func TestAppend_HeartbeatDoesNotCommitAnUnverifiedTail(t *testing.T) {
	n := followerWith(2, 1, 1, 1)
	n.HandleAppendEntries(2, 2, 1, 1, nil, 3)
	if n.volatile.CommitIndex != 1 {
		t.Fatalf("commit = %d, want 1", n.volatile.CommitIndex)
	}
}

func TestAppend_CommitIndexNeverDecreases(t *testing.T) {
	n := followerWith(1, 1, 1, 1)
	n.volatile.CommitIndex = 3
	n.HandleAppendEntries(1, 2, 1, 1, nil, 5) // verifies only up to 1
	if n.volatile.CommitIndex != 3 {
		t.Fatalf("commit moved back to %d", n.volatile.CommitIndex)
	}
}

func TestAppend_NewerTermIsAdoptedAndCandidacyDropped(t *testing.T) {
	n := followerWith(3)
	n.state = Candidate
	n.persistent.VotedFor = 1
	res := n.HandleAppendEntries(7, 2, 0, 0, nil, 0)
	if !res.Success || res.Term != 7 || n.state != Follower || n.persistent.VotedFor != -1 {
		t.Fatalf("%+v state=%v votedFor=%d", res, n.state, n.persistent.VotedFor)
	}
}
