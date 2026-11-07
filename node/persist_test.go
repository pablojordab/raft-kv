package node

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// memStorage is an in-memory Storage that records every Save, so tests can
// check what was persisted and when.
type memStorage struct {
	saves []PersistentState
	fail  error // if set, Save returns it instead of saving
}

func (m *memStorage) Save(s PersistentState) error {
	if m.fail != nil {
		return m.fail
	}
	// Deep-copy: the node keeps mutating its own log after Save returns.
	cp := PersistentState{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor}
	for _, e := range s.Log {
		e.Command = append([]byte(nil), e.Command...)
		cp.Log = append(cp.Log, e)
	}
	m.saves = append(m.saves, cp)
	return nil
}

func (m *memStorage) last() (PersistentState, bool) {
	if len(m.saves) == 0 {
		return PersistentState{}, false
	}
	return m.saves[len(m.saves)-1], true
}

func samePersistent(a, b PersistentState) bool {
	if a.CurrentTerm != b.CurrentTerm || a.VotedFor != b.VotedFor || len(a.Log) != len(b.Log) {
		return false
	}
	for i := range a.Log {
		x, y := a.Log[i], b.Log[i]
		if x.Term != y.Term || x.Index != y.Index || string(x.Command) != string(y.Command) {
			return false
		}
	}
	return true
}

// failingTransport makes every RPC fail, so a candidate's campaign runs
// without reaching anyone.
type failingTransport struct{}

func (failingTransport) SendRequestVote(string, int, int, int, int) (int, bool, error) {
	return 0, false, errors.New("unreachable")
}
func (failingTransport) SendAppendEntries(string, int, int, int, int, []LogEntry, int) (AppendEntriesResult, error) {
	return AppendEntriesResult{}, errors.New("unreachable")
}

// TestPersist_EveryStateChangeIsSavedBeforeReturning drives one node
// through every code path that changes currentTerm, votedFor or the log,
// and after each step checks two things:
//
//   - nothing is left unsaved: the last saved state equals the node's
//     in-memory persistent state (the Figure 2 rule: durable before any
//     reply goes out), and
//   - operations that change nothing don't write to disk at all.
//
// If a future change adds a new way to modify that state and forgets to
// persist it, the first check fails.
func TestPersist_EveryStateChangeIsSavedBeforeReturning(t *testing.T) {
	store := &memStorage{}
	n := NewNode(1, map[int]string{2: "n2", 3: "n3"}, failingTransport{})
	n.SetStorage(store)

	step := func(name string, wantSave bool, op func()) {
		t.Helper()
		before := len(store.saves)
		op()
		n.mu.Lock()
		current := n.persistent
		n.mu.Unlock()

		saved, ok := store.last()
		if !ok || !samePersistent(saved, current) {
			// Before the first save, an unchanged fresh node is fine.
			if ok || current.CurrentTerm != 0 || current.VotedFor != -1 || len(current.Log) != 0 {
				t.Fatalf("%s: in-memory state %+v was not saved (last save %+v)", name, current, saved)
			}
		}
		if wrote := len(store.saves) > before; wrote != wantSave {
			t.Fatalf("%s: wrote to storage = %v, want %v", name, wrote, wantSave)
		}
	}

	step("grant a vote in a newer term", true, func() {
		if _, granted := n.HandleRequestVote(1, 2, 0, 0); !granted {
			t.Fatal("expected the vote")
		}
	})
	step("refuse a second candidate in the same term", false, func() {
		if _, granted := n.HandleRequestVote(1, 3, 0, 0); granted {
			t.Fatal("voted twice in one term")
		}
	})
	step("refuse a stale candidate", false, func() { n.HandleRequestVote(0, 3, 0, 0) })
	step("heartbeat from the leader, nothing new", false, func() { n.HandleAppendEntries(1, 2, 0, 0, nil, 0) })
	step("receive new entries", true, func() {
		n.HandleAppendEntries(1, 2, 0, 0, []LogEntry{{Term: 1, Command: []byte("a")}, {Term: 1, Command: []byte("b")}}, 0)
	})
	step("receive entries we already have", false, func() {
		n.HandleAppendEntries(1, 2, 0, 0, []LogEntry{{Term: 1, Command: []byte("a")}}, 0)
	})
	step("rejected AppendEntries (log mismatch)", false, func() { n.HandleAppendEntries(1, 2, 9, 1, nil, 0) })
	step("conflicting entry from a newer leader truncates the log", true, func() {
		n.HandleAppendEntries(2, 3, 1, 1, []LogEntry{{Term: 2, Command: []byte("c")}}, 0)
	})
	step("become candidate (new term, vote for self)", true, func() {
		n.mu.Lock()
		n.becomeCandidateLocked(context.Background())
		n.mu.Unlock()
	})
	step("become leader (no persistent change)", false, func() {
		n.mu.Lock()
		n.becomeLeaderLocked(t.Context())
		n.mu.Unlock()
	})
	step("leader appends a client command", true, func() {
		if _, _, ok := n.Submit([]byte("SET x 1")); !ok {
			t.Fatal("Submit refused on the leader")
		}
	})
	step("a reply with a higher term makes the leader step down", true, func() {
		n.mu.Lock()
		n.handleAppendEntriesReplyLocked(n.persistent.CurrentTerm, appendEntriesRequest{peerID: 2}, AppendEntriesResult{Term: 9})
		n.mu.Unlock()
	})
	if n.State() != Follower || n.CurrentTerm() != 9 {
		t.Fatalf("expected Follower in term 9, got %v term %d", n.State(), n.CurrentTerm())
	}
}

// The specific guarantee the paper cares most about: by the time a "yes"
// vote is returned, it is already on disk.
func TestPersist_VoteIsDurableBeforeTheReply(t *testing.T) {
	store := &memStorage{}
	n := NewNode(1, map[int]string{2: "n2"}, nil)
	n.SetStorage(store)

	if _, granted := n.HandleRequestVote(5, 2, 0, 0); !granted {
		t.Fatal("expected the vote")
	}
	saved, ok := store.last()
	if !ok || saved.CurrentTerm != 5 || saved.VotedFor != 2 {
		t.Fatalf("vote not durable when the reply was returned: last save %+v", saved)
	}
}

func TestPersist_SaveFailureStopsTheNode(t *testing.T) {
	n := NewNode(1, map[int]string{2: "n2"}, nil)
	n.SetStorage(&memStorage{fail: errors.New("disk full")})

	defer func() {
		r := recover()
		if r == nil || !strings.Contains(r.(string), "disk full") {
			t.Fatalf("expected the default handler to stop the node, got %v", r)
		}
	}()
	n.HandleRequestVote(1, 2, 0, 0) // needs to persist the vote, which fails
	t.Fatal("a vote was granted even though it could not be saved")
}

func TestPersist_NoStorageMeansInMemoryOnly(t *testing.T) {
	n := NewNode(1, map[int]string{2: "n2"}, nil)
	if _, granted := n.HandleRequestVote(1, 2, 0, 0); !granted {
		t.Fatal("a node without storage should still work in memory")
	}
}
