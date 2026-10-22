package node

// HandleAppendEntries implements the receiver side of the AppendEntries RPC,
// following the five receiver rules in the Raft paper's Figure 2. The same
// RPC serves two purposes: a heartbeat (entries empty) that keeps
// followers from starting elections, and log replication (entries
// non-empty). Both go through every rule below — a heartbeat is just the
// special case with nothing to append.
//
// Parameters mirror the AppendEntries RPC's arguments: the leader's term
// and id, the index/term of the log entry immediately preceding the new
// ones, the new entries themselves, and the leader's CommitIndex.
//
// Returns this node's current term (so a stale leader can discover it's
// behind and step down) and whether the request was accepted. Success is
// true only if this node's log contained an entry matching
// prevLogIndex/prevLogTerm — which is what tells the leader "our logs now
// agree up to the end of what you sent", and lets it advance its
// MatchIndex for this follower. On a log mismatch, the result also carries
// ConflictTerm/ConflictIndex so the leader can back up quickly.
func (n *Node) HandleAppendEntries(leaderTerm, leaderID, prevLogIndex, prevLogTerm int, entries []LogEntry, leaderCommit int) AppendEntriesResult {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Rule 1: reply false if the leader's term is stale. Report our term
	// so it discovers it's no longer the leader and steps down — same
	// idea as RequestVote's stale-term check in vote.go.
	if leaderTerm < n.persistent.CurrentTerm {
		return AppendEntriesResult{Term: n.persistent.CurrentTerm}
	}

	// leaderTerm >= our term: this sender IS the leader for that term.
	// Adopt its term if it's newer, and in every case become (or stay) a
	// Follower — a Candidate competing in this term has already lost.
	// stepDownLocked (state.go) implements exactly that rule.
	n.stepDownLocked(leaderTerm)

	// Remember who the leader is, so clients that contact us can be
	// redirected to it (Submit in rpc/server.go).
	n.leaderID = leaderID

	// Hearing from the current leader postpones our own election
	// timeout. This happens BEFORE the log-consistency check below on
	// purpose: even if our log doesn't match yet (rule 2 fails), the
	// sender is still the legitimate leader, and it will keep retrying
	// with earlier entries until our logs agree. Starting an election
	// just because our log is behind would needlessly depose a healthy
	// leader.
	n.resetElectionTimerLocked()

	// Rule 2: reply false if our log doesn't contain an entry at
	// prevLogIndex whose term matches prevLogTerm. This is the core of
	// the Log Matching Property (§5.3): if two logs agree on the index
	// and term of one entry, they agree on every entry before it too —
	// so by checking only the single entry right before the new ones,
	// the follower effectively checks its whole log prefix. Index 0 is
	// the empty-log sentinel and always matches (termAtLocked returns
	// term 0 for it), which is how a leader sends a follower its very
	// first entries.
	ourTerm, haveEntry := n.termAtLocked(prevLogIndex)
	if !haveEntry || ourTerm != prevLogTerm {
		// The leader will back up this follower's NextIndex and retry
		// with an earlier prevLogIndex until our logs agree. Rather
		// than making it guess one entry at a time, tell it where the
		// disagreement starts (see conflictHintLocked).
		conflictTerm, conflictIndex := n.conflictHintLocked(prevLogIndex, haveEntry, ourTerm)
		return AppendEntriesResult{
			Term:          n.persistent.CurrentTerm,
			ConflictTerm:  conflictTerm,
			ConflictIndex: conflictIndex,
		}
	}

	// Rules 3 and 4: walk the new entries alongside our own log.
	//   - Rule 3: if an existing entry conflicts with a new one (same
	//     index, different term), delete it and everything after it.
	//     Those entries came from a leader whose term ended before they
	//     were committed, so they can be safely discarded — the current
	//     leader's log is the source of truth.
	//   - Rule 4: append any new entries not already in the log.
	//
	// Crucially, we do NOT blindly truncate our log at prevLogIndex and
	// paste the entries on top. RPCs can be delayed or reordered: an old
	// AppendEntries carrying entries 2-3 might arrive after a newer one
	// already gave us entries 2-5. Truncating on that old RPC would throw
	// away entries 4-5, which may already be committed. Only an actual
	// term conflict justifies deleting anything.
	for i, entry := range entries {
		index := prevLogIndex + 1 + i // position this entry belongs at (1-indexed)

		if index <= len(n.persistent.Log) {
			if n.persistent.Log[index-1].Term == entry.Term {
				continue // already have this exact entry; nothing to do
			}
			// Conflict: drop this entry and everything after it.
			n.persistent.Log = n.persistent.Log[:index-1]
		}

		// From here on we're at the end of our log: append the rest.
		for j := i; j < len(entries); j++ {
			e := entries[j]
			// Set Index from our own position math rather than trusting
			// the wire value, so the "Log[k].Index == k+1" invariant
			// (state.go) holds no matter what the sender put there.
			e.Index = prevLogIndex + 1 + j
			n.persistent.Log = append(n.persistent.Log, e)
		}
		break
	}

	// Rule 5: if leaderCommit > commitIndex, set
	// commitIndex = min(leaderCommit, index of last new entry).
	//
	// The min matters: the leader may have committed entries beyond what
	// it sent in THIS RPC (e.g. a heartbeat carrying no entries). We may
	// only mark as committed what we've verified matches the leader's
	// log, which is exactly everything up to prevLogIndex + len(entries)
	// — anything past that in our log could still be a stale, conflicting
	// tail that a later RPC will overwrite.
	if leaderCommit > n.volatile.CommitIndex {
		lastNewIndex := prevLogIndex + len(entries)
		newCommit := leaderCommit
		if lastNewIndex < newCommit {
			newCommit = lastNewIndex
		}
		// CommitIndex must never decrease; the min above can only be
		// smaller than our current value on a delayed, stale RPC.
		if newCommit > n.volatile.CommitIndex {
			n.volatile.CommitIndex = newCommit
		}
	}

	return AppendEntriesResult{Term: n.persistent.CurrentTerm, Success: true}
}

// conflictHintLocked computes the ConflictTerm/ConflictIndex hint returned
// when the consistency check at prevLogIndex fails. There are two ways it
// can fail, and each gets a different hint:
//
//   - Our log is too short to have an entry at prevLogIndex at all
//     (haveEntry is false). ConflictTerm is 0 ("no entry there") and
//     ConflictIndex is one past our last entry: the leader can jump
//     straight to sending from there, skipping every index we're missing.
//
//   - We do have an entry there, but from a different term (ourTerm). Our
//     whole run of entries from that term is suspect: they came from a
//     leader whose term ended before they were committed. ConflictTerm is
//     that term and ConflictIndex is the first index where it appears in
//     our log, so the leader can skip back past the entire run in one
//     step instead of one entry per round.
//
// Must be called with n.mu held.
func (n *Node) conflictHintLocked(prevLogIndex int, haveEntry bool, ourTerm int) (conflictTerm, conflictIndex int) {
	if !haveEntry {
		return 0, len(n.persistent.Log) + 1
	}
	first := prevLogIndex
	for first > 1 && n.persistent.Log[first-2].Term == ourTerm {
		first--
	}
	return ourTerm, first
}
