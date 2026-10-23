package node

// This file groups the helpers that read or transition a Node's Raft state
// (Figure 2 of the paper), so the rules for that state live in one place
// instead of being re-implemented inline by every RPC handler.
//
// Log indexing convention, used by every helper below: the log is
// 1-indexed, and n.persistent.Log[i] always holds the entry with
// Index == i+1. Index 0 is the "no entry" sentinel with term 0 — it's what
// prevLogIndex/prevLogTerm refer to when a leader sends its very first
// entry, so it has to be a valid, answerable index even though no entry is
// stored there.

// stepDownLocked makes this node a Follower, adopting newTerm first if
// it's newer than ours. It's the single implementation of the paper's
// "Rules for Servers" (Figure 2): "If RPC request or response contains term
// T > currentTerm: set currentTerm = T, convert to follower". Every place
// that discovers a newer term, or a legitimate leader for the current term,
// calls this instead of hand-writing the transition.
//
// When the term advances, VotedFor is cleared, since a vote only has
// meaning within the term it was cast in. Leader-only bookkeeping
// (NextIndex/MatchIndex) is always dropped: a Follower has no business
// holding another leader's replication progress, and keeping stale maps
// around would let a later bug quietly read numbers from a previous
// leadership. They're rebuilt from scratch by becomeLeaderLocked if this
// node ever wins again.
//
// Must be called with n.mu held.
func (n *Node) stepDownLocked(newTerm int) {
	if newTerm > n.persistent.CurrentTerm {
		n.persistent.CurrentTerm = newTerm
		n.persistent.VotedFor = -1
		n.leaderID = -1 // whoever led the old term doesn't lead this one
	}
	n.state = Follower
	n.volatile.NextIndex = nil
	n.volatile.MatchIndex = nil
}

// lastLogIndexAndTermLocked returns the index and term of this node's last
// log entry, or (0, 0) if the log is empty — the index-0 sentinel described
// at the top of this file. Must be called with n.mu held.
func (n *Node) lastLogIndexAndTermLocked() (index int, term int) {
	if len(n.persistent.Log) == 0 {
		return 0, 0
	}
	last := n.persistent.Log[len(n.persistent.Log)-1]
	return last.Index, last.Term
}

// termAtLocked returns the term of the entry at the given log index.
// Index 0 is always valid and has term 0 (the sentinel). ok is false if the
// index is negative or beyond the end of this node's log — which, for a
// follower checking an AppendEntries' prevLogIndex (task 13), means "I
// don't have that entry, so my log doesn't match yours yet".
//
// Must be called with n.mu held.
func (n *Node) termAtLocked(index int) (term int, ok bool) {
	if index == 0 {
		return 0, true
	}
	if index < 0 || index > len(n.persistent.Log) {
		return 0, false
	}
	return n.persistent.Log[index-1].Term, true
}

// entriesFromLocked returns a COPY of every log entry from the given index
// to the end of the log (empty if index is past the end). A leader will use
// this to build the Entries it sends a follower, starting at that
// follower's NextIndex (task 14).
//
// It returns a copy rather than a sub-slice on purpose: the caller sends
// these entries over the network after releasing n.mu, and a sub-slice
// would still share its backing array with n.persistent.Log. If the log
// were truncated or appended to concurrently (exactly what AppendEntries
// does on conflict), the in-flight RPC could end up reading entries that
// were being overwritten — a data race the copy rules out entirely.
//
// Must be called with n.mu held.
func (n *Node) entriesFromLocked(index int) []LogEntry {
	if index < 1 {
		index = 1
	}
	if index > len(n.persistent.Log) {
		return nil
	}
	src := n.persistent.Log[index-1:]
	out := make([]LogEntry, len(src))
	copy(out, src)
	return out
}

// CommitIndex returns the index of the highest log entry this node knows
// to be committed. Takes the lock; intended for tests and status logging.
func (n *Node) CommitIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.volatile.CommitIndex
}

// LastApplied returns the index of the highest log entry this node has
// applied to its state machine. Takes the lock; intended for tests and
// status logging.
func (n *Node) LastApplied() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.volatile.LastApplied
}

// Status is a point-in-time snapshot of a node's Raft state, for tests and
// debugging tools (see the Status RPC in rpc/server.go). Everything in it
// is a copy: changing it never affects the node.
type Status struct {
	ID          int
	State       NodeState
	Term        int
	LeaderID    int
	CommitIndex int
	LastApplied int
	Log         []LogEntry
}

// Status returns a consistent snapshot of this node's state: every field
// is read under one acquisition of the lock, so the log, term and commit
// index all describe the same instant (reading them through separate
// accessors could mix values from before and after a concurrent update).
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.id,
		State:       n.state,
		Term:        n.persistent.CurrentTerm,
		LeaderID:    n.leaderID,
		CommitIndex: n.volatile.CommitIndex,
		LastApplied: n.volatile.LastApplied,
		Log:         n.entriesFromLocked(1), // a copy of the whole log
	}
}

// LeaderID returns the id of the node this one believes leads its current
// term, or -1 if it doesn't know. It's a hint for redirecting clients, not
// a guarantee: the leader may have just crashed or been replaced.
func (n *Node) LeaderID() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// LogLength returns how many entries this node's log currently holds.
// Takes the lock; intended for tests and status logging.
func (n *Node) LogLength() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.persistent.Log)
}
