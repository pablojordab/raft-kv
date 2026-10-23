package node

import "testing"

// A follower that restarts without persistence comes back with an empty
// log, even though the leader's MatchIndex says it holds several entries.
// The leader must believe the follower's report, correct its MatchIndex
// down, and jump straight to sending from index 1, instead of walking back
// one entry per round because of the MatchIndex+1 floor.
func TestBackUp_FollowerThatLostItsLogResetsMatchIndex(t *testing.T) {
	l := NewNode(1, map[int]string{2: "n2"}, nil)
	l.state = Leader
	l.persistent.CurrentTerm = 1
	l.persistent.Log = entriesWithTerms(1, 1, 1, 1, 1, 1, 1, 1)
	l.volatile.NextIndex = map[int]int{2: 9}
	l.volatile.MatchIndex = map[int]int{2: 5} // recorded before the follower crashed

	// The restarted follower rejects prevLogIndex 8: its log is empty.
	l.backUpNextIndexLocked(
		appendEntriesRequest{peerID: 2, prevLogIndex: 8},
		AppendEntriesResult{Term: 1, ConflictTerm: 0, ConflictIndex: 1},
	)

	if got := l.volatile.MatchIndex[2]; got != 0 {
		t.Fatalf("MatchIndex = %d, want 0 (the follower has no entries)", got)
	}
	if got := l.volatile.NextIndex[2]; got != 1 {
		t.Fatalf("NextIndex = %d, want 1 (resend the whole log in one step)", got)
	}
}

// The ordinary case must keep the floor: a follower whose log is merely
// missing the newest entries (still at least MatchIndex long) never makes
// the leader resend entries it already confirmed.
func TestBackUp_KeepsMatchIndexWhenFollowerStillHasConfirmedEntries(t *testing.T) {
	l := NewNode(1, map[int]string{2: "n2"}, nil)
	l.state = Leader
	l.persistent.CurrentTerm = 1
	l.persistent.Log = entriesWithTerms(1, 1, 1, 1, 1, 1, 1, 1)
	l.volatile.NextIndex = map[int]int{2: 9}
	l.volatile.MatchIndex = map[int]int{2: 5}

	// Follower has 6 entries (>= MatchIndex): nothing was lost.
	l.backUpNextIndexLocked(
		appendEntriesRequest{peerID: 2, prevLogIndex: 8},
		AppendEntriesResult{Term: 1, ConflictTerm: 0, ConflictIndex: 7},
	)

	if got := l.volatile.MatchIndex[2]; got != 5 {
		t.Fatalf("MatchIndex = %d, want it unchanged at 5", got)
	}
	if got := l.volatile.NextIndex[2]; got != 7 {
		t.Fatalf("NextIndex = %d, want 7", got)
	}
}
