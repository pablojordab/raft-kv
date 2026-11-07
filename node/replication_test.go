package node

import (
	"fmt"
	"testing"
)

// Tests for the leader side of log replication (heartbeat.go, submit.go):
// accepting commands, sending each follower what it's missing, folding the
// replies back in, and backing up quickly when a follower's log differs,
// including the six divergent followers from the Raft paper's Figure 7.

func TestSubmit_RefusedOnANonLeader(t *testing.T) {
	n := NewNode(1, map[int]string{}, nil)
	if _, _, ok := n.Submit([]byte("SET x 1")); ok || n.LogLength() != 0 {
		t.Fatal("a follower accepted a command")
	}
}

func TestSubmit_AppendsAtTheNextIndexAndCopiesTheCommand(t *testing.T) {
	leader := newMemCluster(3, 1)[0]
	buf := []byte("SET a 1")
	i1, term, ok := leader.Submit(buf)
	buf[4] = 'Z' // the caller reuses its buffer
	i2, _, _ := leader.Submit([]byte("SET b 2"))
	if !ok || i1 != 1 || i2 != 2 || term != 1 {
		t.Fatalf("got i1=%d i2=%d term=%d ok=%v", i1, i2, term, ok)
	}
	if got := string(leader.Status().Log[0].Command); got != "SET a 1" {
		t.Fatalf("stored command changed with the caller's buffer: %q", got)
	}
}

func TestReplication_FollowersEndUpWithTheLeadersLog(t *testing.T) {
	nodes := newMemCluster(3, 1)
	leader := nodes[0]
	for _, c := range []string{"SET x 1", "SET y 2", "SET z 3"} {
		leader.Submit([]byte(c))
	}
	replicationRound(leader)

	for _, f := range nodes[1:] {
		if got := fmt.Sprint(commandsIn(f)); got != "[SET x 1 SET y 2 SET z 3]" {
			t.Fatalf("follower %d log = %s", f.id, got)
		}
	}
	leader.mu.Lock()
	defer leader.mu.Unlock()
	if leader.volatile.MatchIndex[2] != 3 || leader.volatile.NextIndex[2] != 4 {
		t.Fatalf("matchIndex=%d nextIndex=%d, want 3 and 4", leader.volatile.MatchIndex[2], leader.volatile.NextIndex[2])
	}
}

// Each round sends only what a follower is missing; an up-to-date follower
// gets an empty AppendEntries, i.e. a heartbeat.
func TestReplication_SendsOnlyNewEntries(t *testing.T) {
	leader := newMemCluster(2, 1)[0]
	leader.Submit([]byte("a"))
	leader.Submit([]byte("b"))
	replicationRound(leader)

	leader.Submit([]byte("c"))
	leader.mu.Lock()
	reqs := leader.buildAppendEntriesLocked()
	leader.mu.Unlock()
	if len(reqs) != 1 || reqs[0].prevLogIndex != 2 || len(reqs[0].entries) != 1 || string(reqs[0].entries[0].Command) != "c" {
		t.Fatalf("expected only entry 3 after prevLogIndex 2, got %+v", reqs)
	}

	replicationRound(leader)
	leader.mu.Lock()
	reqs = leader.buildAppendEntriesLocked()
	leader.mu.Unlock()
	if len(reqs[0].entries) != 0 || reqs[0].prevLogIndex != 3 {
		t.Fatalf("an up-to-date follower should get an empty heartbeat at prevLogIndex 3, got %+v", reqs[0])
	}
}

// Rounds overlap, so replies can arrive out of order: a late reply to an
// older, shorter request must never move a follower's progress backwards.
func TestReplication_LateReplyDoesNotMoveProgressBackwards(t *testing.T) {
	leader := newMemCluster(2, 1)[0]
	leader.mu.Lock()
	defer leader.mu.Unlock()
	leader.persistent.Log = entriesWithTerms(1, 1, 1, 1, 1)
	leader.handleAppendEntriesReplyLocked(1, appendEntriesRequest{peerID: 2, entries: entriesWithTerms(1, 1, 1, 1, 1)}, AppendEntriesResult{Term: 1, Success: true})
	leader.handleAppendEntriesReplyLocked(1, appendEntriesRequest{peerID: 2, entries: entriesWithTerms(1)}, AppendEntriesResult{Term: 1, Success: true})
	if leader.volatile.MatchIndex[2] != 5 || leader.volatile.NextIndex[2] != 6 {
		t.Fatalf("progress rolled back: matchIndex=%d nextIndex=%d", leader.volatile.MatchIndex[2], leader.volatile.NextIndex[2])
	}
}

// A reply that arrives after this node stopped being leader belongs to a
// leadership that no longer exists and must be ignored.
func TestReplication_ReplyAfterSteppingDownIsIgnored(t *testing.T) {
	leader := newMemCluster(2, 1)[0]
	leader.mu.Lock()
	defer leader.mu.Unlock()
	leader.stepDownLocked(2) // leader-only maps are now nil; writing to them would panic
	if leader.handleAppendEntriesReplyLocked(1, appendEntriesRequest{peerID: 2, entries: entriesWithTerms(1)}, AppendEntriesResult{Term: 2, Success: true}) {
		t.Fatal("a reply from an old leadership was processed")
	}
}

// --- Backing up nextIndex after a rejection (the §5.3 optimization) ---

// pairTransport connects a leader directly to one follower in memory.
type pairTransport struct{ follower *Node }

func (p *pairTransport) SendRequestVote(string, int, int, int, int) (int, bool, error) {
	return 0, false, fmt.Errorf("not used")
}
func (p *pairTransport) SendAppendEntries(_ string, term, leaderID, prevIdx, prevTerm int, entries []LogEntry, commit int) (AppendEntriesResult, error) {
	return p.follower.HandleAppendEntries(term, leaderID, prevIdx, prevTerm, entries, commit), nil
}

// converge replicates from a leader (term 8, log with `leaderTerms`, plus
// one new term-8 command, as in Figure 7) to a follower whose log has
// `followerTerms`, until the follower's log equals the leader's. Returns
// how many rounds that took.
func converge(t *testing.T, leaderTerms, followerTerms []int) int {
	t.Helper()
	f := NewNode(2, map[int]string{1: "l"}, nil)
	f.persistent.CurrentTerm = 7
	f.persistent.Log = entriesWithTerms(followerTerms...)

	l := NewNode(1, map[int]string{2: "f"}, &pairTransport{follower: f})
	l.persistent.CurrentTerm = 8
	l.persistent.Log = entriesWithTerms(leaderTerms...)
	l.state = Leader
	l.volatile.NextIndex = map[int]int{2: len(leaderTerms) + 1}
	l.volatile.MatchIndex = map[int]int{2: 0}
	l.Submit([]byte("new"))

	for round := 1; round <= 50; round++ {
		replicationRound(l)
		if fmt.Sprint(l.Status().Log) == fmt.Sprint(f.Status().Log) {
			return round
		}
	}
	t.Fatal("did not converge within 50 rounds")
	return 0
}

// The six divergent followers of the Raft paper's Figure 7 (plus an empty
// one), each replicating from the figure's leader. All must end up with
// the leader's exact log, and thanks to the conflict hints each takes at
// most 3 rounds; plain one-entry-per-round backtracking needs up to 11 for
// these same cases.
func TestBackUp_Figure7FollowersConvergeInAFewRounds(t *testing.T) {
	leader := []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6}
	cases := []struct {
		name     string
		follower []int
	}{
		{"(a) missing one entry", []int{1, 1, 1, 4, 4, 5, 5, 6, 6}},
		{"(b) missing six entries", []int{1, 1, 1, 4}},
		{"(c) one extra entry", []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6}},
		{"(d) extra term-7 tail", []int{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7}},
		{"(e) stale term-4 run", []int{1, 1, 1, 4, 4, 4, 4}},
		{"(f) terms the leader never had", []int{1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3}},
		{"empty log", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rounds := converge(t, leader, c.follower)
			t.Logf("converged in %d round(s)", rounds)
			if rounds > 3 {
				t.Fatalf("took %d rounds; term-skipping backtracking should need at most 3", rounds)
			}
		})
	}
}

func TestBackUp_ConflictHint(t *testing.T) {
	n := followerWith(3, 1, 1, 2, 2, 2, 3)
	cases := []struct {
		prevLogIndex int
		haveEntry    bool
		ourTerm      int
		wantTerm     int
		wantIndex    int
	}{
		{9, false, 0, 0, 7}, // too short: our log ends at 6
		{5, true, 2, 2, 3},  // mismatch inside the term-2 run that starts at 3
		{2, true, 1, 1, 1},  // a run starting at index 1
	}
	for _, c := range cases {
		term, index := n.conflictHintLocked(c.prevLogIndex, c.haveEntry, c.ourTerm)
		if term != c.wantTerm || index != c.wantIndex {
			t.Errorf("hint(prevLogIndex=%d) = (%d, %d), want (%d, %d)", c.prevLogIndex, term, index, c.wantTerm, c.wantIndex)
		}
	}
}

func TestBackUp_IgnoresARejectionOfAnOlderRequest(t *testing.T) {
	l := leaderAt(5, 0)
	// A rejection of a request sent back when NextIndex was 9.
	l.backUpNextIndexLocked(appendEntriesRequest{peerID: 2, prevLogIndex: 8}, AppendEntriesResult{Term: 8, ConflictIndex: 1})
	if l.volatile.NextIndex[2] != 5 {
		t.Fatalf("an old rejection moved NextIndex to %d", l.volatile.NextIndex[2])
	}
}

// A malformed hint must not stall the leader: every rejection moves
// NextIndex strictly backwards, but never below MatchIndex+1 while the
// follower still holds the matched entries.
func TestBackUp_AlwaysMakesProgressAndRespectsTheMatchFloor(t *testing.T) {
	l := leaderAt(9, 0)
	l.backUpNextIndexLocked(appendEntriesRequest{peerID: 2, prevLogIndex: 8}, AppendEntriesResult{Term: 8, ConflictTerm: 1, ConflictIndex: 100})
	if got := l.volatile.NextIndex[2]; got >= 9 {
		t.Fatalf("NextIndex did not move back: %d", got)
	}

	// A conflict in a term the leader never had, starting at index 1, would
	// send NextIndex to 1; but entries up to MatchIndex 6 are known to
	// match, so the leader resumes at 7. (A ConflictTerm of 0 with an index
	// below MatchIndex means something else: the follower lost its log;
	// see backup_test.go.)
	l2 := leaderAt(9, 6)
	l2.backUpNextIndexLocked(appendEntriesRequest{peerID: 2, prevLogIndex: 8}, AppendEntriesResult{Term: 8, ConflictTerm: 2, ConflictIndex: 1})
	if got := l2.volatile.NextIndex[2]; got != 7 {
		t.Fatalf("NextIndex = %d, want 7 (MatchIndex+1)", got)
	}
}

func TestBackUp_LastIndexOfTerm(t *testing.T) {
	n := followerWith(6, 1, 1, 4, 4, 5, 6)
	for term, want := range map[int]int{1: 2, 4: 4, 5: 5, 6: 6, 2: 0, 3: 0, 9: 0} {
		if got := n.lastIndexOfTermLocked(term); got != want {
			t.Errorf("lastIndexOfTerm(%d) = %d, want %d", term, got, want)
		}
	}
}

// leaderAt builds a term-8 leader with an 8-entry log and the given
// NextIndex/MatchIndex for its one peer, node 2.
func leaderAt(nextIndex, matchIndex int) *Node {
	l := NewNode(1, map[int]string{2: "n2"}, nil)
	l.persistent.CurrentTerm = 8
	l.persistent.Log = entriesWithTerms(1, 1, 1, 1, 1, 1, 1, 1)
	l.state = Leader
	l.volatile.NextIndex = map[int]int{2: nextIndex}
	l.volatile.MatchIndex = map[int]int{2: matchIndex}
	return l
}

// commandsIn lists the commands in a node's log, skipping no-ops.
func commandsIn(n *Node) []string {
	var out []string
	for _, e := range n.Status().Log {
		if !e.IsNoOp() {
			out = append(out, string(e.Command))
		}
	}
	return out
}
