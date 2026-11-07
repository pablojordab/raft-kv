package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Tests for the candidate side of elections (campaign.go), the election
// timer (election.go) and the leader's heartbeat loop (heartbeat.go).

// voteScript is a Transport whose RequestVote replies are scripted per
// peer address; AppendEntries always succeeds with a configurable term and
// is counted, so heartbeat tests can see what the leader sends.
type voteScript struct {
	mu     sync.Mutex
	votes  map[string]voteReply
	aeTerm int
	aeSent atomic.Int32
}

type voteReply struct {
	term    int
	granted bool
	err     error
}

func (s *voteScript) SendRequestVote(addr string, term, candidateID, lastLogIndex, lastLogTerm int) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.votes[addr]
	if !ok {
		return 0, false, errors.New("no scripted reply for " + addr)
	}
	return r.term, r.granted, r.err
}

func (s *voteScript) SendAppendEntries(addr string, term, leaderID, prevLogIndex, prevLogTerm int, entries []LogEntry, commit int) (AppendEntriesResult, error) {
	s.aeSent.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	return AppendEntriesResult{Term: s.aeTerm, Success: true}, nil
}

// candidateIn builds a node already in the state becomeCandidateLocked
// leaves it in (Candidate, term set, self-voted), so a test can run the
// campaign directly without waiting for a real timeout.
func candidateIn(term int, peers map[int]string, tr Transport) *Node {
	n := NewNode(1, peers, tr)
	n.state = Candidate
	n.persistent.CurrentTerm = term
	n.persistent.VotedFor = 1
	return n
}

func TestCampaign_WinsWithAMajorityAndAppendsANoOp(t *testing.T) {
	tr := &voteScript{votes: map[string]voteReply{"n2": {term: 1, granted: true}, "n3": {term: 1, granted: false}}, aeTerm: 1}
	n := candidateIn(1, map[int]string{2: "n2", 3: "n3"}, tr)

	n.runElectionCampaign(t.Context(), 1)

	st := n.Status()
	if st.State != Leader {
		t.Fatalf("2 of 3 votes (with its own) should win, got %v", st.State)
	}
	if len(st.Log) != 1 || !st.Log[0].IsNoOp() || st.Log[0].Term != 1 {
		t.Fatalf("a new leader must append a no-op from its term, log = %+v", st.Log)
	}
}

func TestCampaign_SplitVoteStaysCandidate(t *testing.T) {
	// 5 nodes: one yes, two no, one unreachable: 2 votes of the 3 needed.
	tr := &voteScript{votes: map[string]voteReply{
		"n2": {term: 1, granted: true},
		"n3": {term: 1, granted: false},
		"n4": {term: 1, granted: false},
		"n5": {err: errors.New("connection refused")},
	}}
	n := candidateIn(1, map[int]string{2: "n2", 3: "n3", 4: "n4", 5: "n5"}, tr)

	n.runElectionCampaign(t.Context(), 1)

	if st := n.Status(); st.State != Candidate || st.Term != 1 {
		t.Fatalf("after a split vote: state=%v term=%d, want Candidate in term 1", st.State, st.Term)
	}
}

func TestCampaign_StepsDownOnSeeingAHigherTerm(t *testing.T) {
	tr := &voteScript{votes: map[string]voteReply{"n2": {term: 1}, "n3": {term: 5}}}
	n := candidateIn(1, map[int]string{2: "n2", 3: "n3"}, tr)

	n.runElectionCampaign(t.Context(), 1)

	if st := n.Status(); st.State != Follower || st.Term != 5 {
		t.Fatalf("state=%v term=%d, want Follower in term 5", st.State, st.Term)
	}
}

func TestCampaign_SingleNodeClusterWinsImmediately(t *testing.T) {
	n := candidateIn(1, map[int]string{}, &voteScript{})
	n.runElectionCampaign(t.Context(), 1)
	if st := n.Status(); st.State != Leader || st.CommitIndex != 1 {
		t.Fatalf("state=%v commit=%d, want Leader with its no-op committed", st.State, st.CommitIndex)
	}
}

// With nobody to hear from, a follower's timer must fire and turn it into a
// candidate in a new term (here it then wins alone: a single-node cluster).
func TestElectionTimer_FiresWithoutALeader(t *testing.T) {
	n := NewNode(1, map[int]string{}, &voteScript{})
	n.SetElectionTimeout(20 * time.Millisecond)
	go n.RunElectionTimer(t.Context())

	deadline := time.Now().Add(2 * time.Second)
	for n.State() != Leader {
		if time.Now().After(deadline) {
			t.Fatalf("timer never fired: state=%v term=%d", n.State(), n.CurrentTerm())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n.CurrentTerm() < 1 {
		t.Fatalf("term = %d, want >= 1", n.CurrentTerm())
	}
}

// A follower that keeps hearing from its leader must never start an
// election, however long it runs.
func TestElectionTimer_HeartbeatsKeepAFollowerFromCampaigning(t *testing.T) {
	n := NewNode(1, map[int]string{2: "n2"}, &voteScript{})
	n.SetElectionTimeout(40 * time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go n.RunElectionTimer(ctx)

	for end := time.Now().Add(400 * time.Millisecond); time.Now().Before(end); {
		n.HandleAppendEntries(1, 2, 0, 0, nil, 0) // heartbeat from leader 2
		time.Sleep(10 * time.Millisecond)
		if s := n.State(); s != Follower {
			t.Fatalf("became %v despite steady heartbeats", s)
		}
	}
}

func TestHeartbeat_NewLeaderStartsSendingAppendEntries(t *testing.T) {
	tr := &voteScript{votes: map[string]voteReply{"n2": {term: 1, granted: true}}, aeTerm: 1}
	n := candidateIn(1, map[int]string{2: "n2"}, tr)
	n.runElectionCampaign(t.Context(), 1)

	deadline := time.Now().Add(time.Second)
	for tr.aeSent.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the new leader sent no AppendEntries")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHeartbeat_LeaderStepsDownWhenAReplyHasAHigherTerm(t *testing.T) {
	tr := &voteScript{aeTerm: 7}
	n := NewNode(1, map[int]string{2: "n2"}, tr)
	n.mu.Lock()
	n.persistent.CurrentTerm = 1
	n.becomeLeaderLocked(t.Context())
	n.mu.Unlock()

	deadline := time.Now().Add(time.Second)
	for n.State() == Leader {
		if time.Now().After(deadline) {
			t.Fatal("leader ignored a reply carrying a higher term")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n.CurrentTerm() != 7 {
		t.Fatalf("term = %d, want 7", n.CurrentTerm())
	}
}

func TestHeartbeat_LoopStopsOnceNoLongerLeader(t *testing.T) {
	tr := &voteScript{aeTerm: 1}
	n := NewNode(1, map[int]string{2: "n2"}, tr)
	n.mu.Lock()
	n.persistent.CurrentTerm = 1
	n.becomeLeaderLocked(t.Context())
	n.mu.Unlock()
	time.Sleep(3 * heartbeatInterval)

	n.mu.Lock()
	n.stepDownLocked(2)
	n.mu.Unlock()
	time.Sleep(2 * heartbeatInterval) // let any in-flight round finish
	sent := tr.aeSent.Load()
	time.Sleep(5 * heartbeatInterval)
	if after := tr.aeSent.Load(); after != sent {
		t.Fatalf("an ex-leader kept sending AppendEntries: %d -> %d", sent, after)
	}
}
