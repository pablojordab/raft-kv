package node

import (
	"context"
	"fmt"
	"testing"
)

// These tests cover how the leader decides an entry is committed
// (commit.go) and how followers learn about it, including the Figure 8
// rule from the Raft paper. They run whole in-memory clusters, where the
// real leader-side code (heartbeat.go) talks directly to the real
// follower-side handler (append.go), with no network involved.

// memTransport delivers RPCs by calling straight into other in-memory
// Nodes, keyed by the address each node is registered under.
type memTransport struct {
	nodes map[string]*Node
}

func (m *memTransport) SendRequestVote(addr string, term, candidateID, lastLogIndex, lastLogTerm int) (int, bool, error) {
	t, ok := m.nodes[addr].HandleRequestVote(term, candidateID, lastLogIndex, lastLogTerm)
	return t, ok, nil
}

func (m *memTransport) SendAppendEntries(addr string, term, leaderID, prevLogIndex, prevLogTerm int, entries []LogEntry, leaderCommit int) (AppendEntriesResult, error) {
	return m.nodes[addr].HandleAppendEntries(term, leaderID, prevLogIndex, prevLogTerm, entries, leaderCommit), nil
}

// newMemCluster builds `size` nodes wired through one memTransport, with
// node 1 set up as Leader of `term` (by hand, without starting the real
// background heartbeat loop, so each test drives rounds explicitly).
func newMemCluster(size, term int) []*Node {
	tr := &memTransport{nodes: map[string]*Node{}}
	nodes := make([]*Node, size)
	for id := 1; id <= size; id++ {
		peers := map[int]string{}
		for other := 1; other <= size; other++ {
			if other != id {
				peers[other] = fmt.Sprintf("n%d", other)
			}
		}
		n := NewNode(id, peers, tr)
		n.persistent.CurrentTerm = term
		nodes[id-1] = n
		tr.nodes[fmt.Sprintf("n%d", id)] = n
	}
	leader := nodes[0]
	leader.state = Leader
	leader.volatile.NextIndex = map[int]int{}
	leader.volatile.MatchIndex = map[int]int{}
	for peerID := range leader.peers {
		leader.volatile.NextIndex[peerID] = 1
		leader.volatile.MatchIndex[peerID] = 0
	}
	return nodes
}

// replicationRound runs one leader round synchronously.
func replicationRound(leader *Node) {
	leader.mu.Lock()
	term := leader.persistent.CurrentTerm
	reqs := leader.buildAppendEntriesLocked()
	leader.mu.Unlock()
	leader.replicateRound(context.Background(), term, reqs)
}

func entriesWithTerms(terms ...int) []LogEntry {
	out := make([]LogEntry, len(terms))
	for i, t := range terms {
		out[i] = LogEntry{Term: t, Index: i + 1, Command: []byte(fmt.Sprintf("cmd%d", i+1))}
	}
	return out
}

func TestCommit_EndToEndInThreeNodeCluster(t *testing.T) {
	nodes := newMemCluster(3, 1)
	leader := nodes[0]
	for i := 1; i <= 3; i++ {
		leader.Submit([]byte(fmt.Sprintf("set k%d", i)))
	}
	if got := leader.CommitIndex(); got != 0 {
		t.Fatalf("nothing should be committed before replication, got %d", got)
	}

	replicationRound(leader)
	if got := leader.CommitIndex(); got != 3 {
		t.Fatalf("leader commit = %d after replication to a majority, want 3", got)
	}

	// Followers learn the new commit index from the next round's
	// leaderCommit.
	for _, f := range nodes[1:] {
		if got := f.CommitIndex(); got != 0 {
			t.Fatalf("follower %d committed %d before hearing leaderCommit", f.id, got)
		}
	}
	replicationRound(leader)
	for _, f := range nodes[1:] {
		if got := f.CommitIndex(); got != 3 {
			t.Fatalf("follower %d commit = %d, want 3", f.id, got)
		}
	}
}

func TestCommit_NeedsAMajorityNotJustTheLeader(t *testing.T) {
	leader := newMemCluster(3, 1)[0]
	leader.mu.Lock()
	defer leader.mu.Unlock()
	leader.persistent.Log = entriesWithTerms(1, 1)

	leader.advanceCommitIndexLocked() // only the leader has the entries
	if leader.volatile.CommitIndex != 0 {
		t.Fatalf("committed %d with only the leader holding it", leader.volatile.CommitIndex)
	}

	leader.volatile.MatchIndex[2] = 1 // leader + node 2 = 2 of 3
	leader.advanceCommitIndexLocked()
	if leader.volatile.CommitIndex != 1 {
		t.Fatalf("commit = %d, want 1", leader.volatile.CommitIndex)
	}
}

func TestCommit_FiveNodesNeedThree(t *testing.T) {
	leader := newMemCluster(5, 1)[0]
	leader.mu.Lock()
	defer leader.mu.Unlock()
	leader.persistent.Log = entriesWithTerms(1, 1, 1)

	leader.volatile.MatchIndex[2] = 3 // 2 of 5: not enough
	leader.advanceCommitIndexLocked()
	if leader.volatile.CommitIndex != 0 {
		t.Fatalf("committed with 2 of 5 nodes")
	}

	leader.volatile.MatchIndex[3] = 2 // index 2 now on 3 of 5, index 3 on 2 of 5
	leader.advanceCommitIndexLocked()
	if leader.volatile.CommitIndex != 2 {
		t.Fatalf("commit = %d, want 2 (the highest index on a majority)", leader.volatile.CommitIndex)
	}
}

// With an even cluster size, exactly half is NOT a majority: 2 of 4 nodes
// could be one side of a 2/2 network partition, and the other side could
// commit something conflicting. (With odd sizes, "more than half" and "at
// least half" happen to coincide, so only an even size tests this.)
func TestCommit_HalfOfEvenClusterIsNotAMajority(t *testing.T) {
	leader := newMemCluster(4, 1)[0]
	leader.mu.Lock()
	defer leader.mu.Unlock()
	leader.persistent.Log = entriesWithTerms(1)

	leader.volatile.MatchIndex[2] = 1 // leader + node 2 = 2 of 4
	leader.advanceCommitIndexLocked()
	if leader.volatile.CommitIndex != 0 {
		t.Fatalf("committed with exactly half (2 of 4) of the cluster")
	}

	leader.volatile.MatchIndex[3] = 1 // 3 of 4
	leader.advanceCommitIndexLocked()
	if leader.volatile.CommitIndex != 1 {
		t.Fatalf("commit = %d, want 1 with 3 of 4 nodes", leader.volatile.CommitIndex)
	}
}

// TestCommit_Figure8 is the scenario from the Raft paper's Figure 8. The
// leader is in term 4 and its log holds an entry from term 2 at index 2.
// That entry being on a majority is NOT enough to commit it: only an entry
// from the leader's own term may be committed by counting replicas.
func TestCommit_Figure8OldTermEntryNotCommittedByCounting(t *testing.T) {
	leader := newMemCluster(3, 4)[0]
	leader.mu.Lock()
	defer leader.mu.Unlock()
	leader.persistent.Log = entriesWithTerms(1, 2)

	leader.volatile.MatchIndex[2] = 2 // the term-2 entry is now on 2 of 3 nodes
	leader.advanceCommitIndexLocked()
	if leader.volatile.CommitIndex != 0 {
		t.Fatalf("committed index %d from an earlier term by counting replicas (Figure 8 violation)",
			leader.volatile.CommitIndex)
	}

	// Once an entry from the CURRENT term is on a majority, it commits,
	// and the earlier entries commit with it.
	leader.persistent.Log = append(leader.persistent.Log, LogEntry{Term: 4, Index: 3})
	leader.volatile.MatchIndex[2] = 3
	leader.advanceCommitIndexLocked()
	if leader.volatile.CommitIndex != 3 {
		t.Fatalf("commit = %d, want 3 (current-term entry plus everything before it)",
			leader.volatile.CommitIndex)
	}
}

func TestCommit_SingleNodeClusterCommitsOnSubmit(t *testing.T) {
	n := NewNode(1, map[int]string{}, nil)
	n.state = Leader
	n.persistent.CurrentTerm = 1
	n.volatile.NextIndex = map[int]int{}
	n.volatile.MatchIndex = map[int]int{}

	if index, _, ok := n.Submit([]byte("x")); !ok || index != 1 {
		t.Fatalf("Submit failed: index=%d ok=%v", index, ok)
	}
	if got := n.CommitIndex(); got != 1 {
		t.Fatalf("single-node cluster commit = %d, want 1", got)
	}
}

func TestCommit_NeverMovesBackwards(t *testing.T) {
	leader := newMemCluster(3, 1)[0]
	leader.mu.Lock()
	defer leader.mu.Unlock()
	leader.persistent.Log = entriesWithTerms(1, 1, 1)
	leader.volatile.CommitIndex = 3
	leader.volatile.MatchIndex[2] = 1 // stale view: only index 1 known on a majority

	leader.advanceCommitIndexLocked()
	if leader.volatile.CommitIndex != 3 {
		t.Fatalf("commit moved backwards to %d", leader.volatile.CommitIndex)
	}
}
