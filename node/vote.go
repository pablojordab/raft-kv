package node

// HandleRequestVote implements the receiver-side logic of the RequestVote
// RPC, per the Raft paper's Figure 2 and the safety rules in §5.2 and
// §5.4.1. It takes plain values rather than an rpc.RequestVoteArgs on
// purpose: this package (node) holds the consensus logic and must stay
// free of any dependency on the rpc package, both to avoid an import
// cycle (rpc already imports node, for LogEntry) and so this logic can be
// unit-tested directly, without spinning up any RPC server. The rpc
// package's RaftServer (see rpc/server.go) is the thin adapter that calls
// this method and translates to/from the wire format.
//
// Parameters mirror the RequestVote RPC's arguments: the candidate's term,
// id, and the index/term of the last entry in the candidate's log (used
// for the up-to-date log check below).
//
// Returns this node's current term (so a stale candidate can discover it's
// behind and step down) and whether the vote was granted.
func (n *Node) HandleRequestVote(candidateTerm, candidateID, candidateLastLogIndex, candidateLastLogTerm int) (currentTerm int, voteGranted bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// "Rules for Servers" (Figure 2, applies to every RPC handler): if
	// this request carries a newer term than ours, we're stale — step
	// down to Follower and adopt the new term before doing anything
	// else. This must happen BEFORE the voting checks below: a
	// Candidate or Leader from an old term has no business denying votes
	// once a newer term is known to exist, and starting the new term
	// also clears our old vote, since VotedFor only has meaning within a
	// single term.
	if candidateTerm > n.persistent.CurrentTerm {
		n.persistent.CurrentTerm = candidateTerm
		n.state = Follower
		n.persistent.VotedFor = -1
	}

	// Case 1: the candidate's term is stale (older than ours, even after
	// the step-down check above). Reject outright, and tell it our term
	// so it can catch up and stop campaigning.
	if candidateTerm < n.persistent.CurrentTerm {
		return n.persistent.CurrentTerm, false
	}

	// Case 2: we've already cast our vote this term for a DIFFERENT
	// candidate. A node may only vote once per term (that's what makes
	// "majority" meaningful) — but if we already voted for this exact
	// candidate (e.g. our vote reply to it was lost and it's retrying),
	// re-granting is harmless and correct.
	alreadyVotedForSomeoneElse := n.persistent.VotedFor != -1 && n.persistent.VotedFor != candidateID
	if alreadyVotedForSomeoneElse {
		return n.persistent.CurrentTerm, false
	}

	// Case 3: the "up-to-date log" restriction (§5.4.1), Raft's core
	// safety mechanism for leader election. A candidate only gets our
	// vote if its log is at least as up-to-date as ours: compare the
	// term of each log's last entry first (higher term wins outright);
	// if those terms are equal, the longer log wins. This is what
	// guarantees a node missing committed entries can never become
	// leader — the majority it would need to win always includes at
	// least one node that has those entries, and that node won't vote
	// for it.
	ourLastIndex, ourLastTerm := n.lastLogIndexAndTermLocked()
	candidateLogIsUpToDate := candidateLastLogTerm > ourLastTerm ||
		(candidateLastLogTerm == ourLastTerm && candidateLastLogIndex >= ourLastIndex)

	if !candidateLogIsUpToDate {
		return n.persistent.CurrentTerm, false
	}

	// All checks passed: grant the vote.
	n.persistent.VotedFor = candidateID

	// Per the Raft paper's companion "Students' Guide to Raft": reset
	// the election timer only when we actually GRANT a vote, not on
	// every RequestVote received. Resetting unconditionally would let a
	// node that's been partitioned off (and is stuck retrying stale,
	// never-to-be-granted RequestVotes) keep interrupting a healthy
	// cluster's elections just by sending RPCs nobody should vote for.
	n.resetElectionTimerLocked()

	return n.persistent.CurrentTerm, true
}

// lastLogIndexAndTermLocked returns the index and term of this node's last
// log entry, or (0, 0) if the log is empty — consistent with LogEntry's
// convention that index 0 is a reserved "no entry" sentinel, never a real
// entry's index. Must be called with n.mu held.
func (n *Node) lastLogIndexAndTermLocked() (index int, term int) {
	if len(n.persistent.Log) == 0 {
		return 0, 0
	}
	last := n.persistent.Log[len(n.persistent.Log)-1]
	return last.Index, last.Term
}
