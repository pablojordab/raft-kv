package node

// HandleAppendEntries implements the receiver side of the AppendEntries
// RPC — both as the leader's heartbeat (Entries empty) and, eventually, as
// real log replication. Right now (ahead of Block 2's log-replication
// tasks) it only handles the heartbeat case correctly: recognizing a valid
// leader and postponing this node's own election timeout. It deliberately
// does NOT yet perform the §5.3 log-consistency check (comparing
// prevLogIndex/prevLogTerm against this node's own log and rejecting on a
// mismatch) or append anything to the log — there's nothing to check yet
// when Entries is empty, which is the only case callers send today (see
// node/heartbeat.go). Once Block 2 starts sending real entries, this
// method's unconditional "success" will need to become conditional on
// that consistency check actually passing.
//
// Parameters mirror the AppendEntries RPC's arguments (Figure 2): the
// leader's term and id, the index/term of the log entry immediately
// preceding the new ones (for the future consistency check), the entries
// themselves (always nil today), and the leader's CommitIndex.
//
// Returns this node's current term (so a stale leader can discover it's
// behind and step down) and whether the request was accepted.
func (n *Node) HandleAppendEntries(leaderTerm, leaderID, prevLogIndex, prevLogTerm int, entries []LogEntry, leaderCommit int) (currentTerm int, success bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Case 1: the leader's term is stale (older than ours). Reject
	// outright and report our term, so it discovers it's no longer the
	// leader and steps down — same idea as RequestVote's stale-term
	// check in vote.go.
	if leaderTerm < n.persistent.CurrentTerm {
		return n.persistent.CurrentTerm, false
	}

	// leaderTerm >= our term: this is a legitimate leader (or one from a
	// newer term than we knew about). Per the paper's "Rules for
	// Servers": if we learn of a newer term, adopt it and clear any vote
	// we'd cast under the old one. Either way, a valid AppendEntries
	// means there IS a leader for this term, so we must be a Follower,
	// not a competing Candidate (or a stale Leader of our own, which
	// shouldn't happen under correct majority voting, but stepping down
	// defensively here costs nothing).
	if leaderTerm > n.persistent.CurrentTerm {
		n.persistent.CurrentTerm = leaderTerm
		n.persistent.VotedFor = -1
	}
	n.state = Follower

	// A valid AppendEntries from the current leader is exactly the kind
	// of event that should postpone our own election timeout (same
	// reasoning as granting a vote, in vote.go).
	n.resetElectionTimerLocked()

	return n.persistent.CurrentTerm, true
}
