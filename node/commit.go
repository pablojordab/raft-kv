package node

import "log"

// advanceCommitIndexLocked moves the leader's CommitIndex forward to the
// highest log index that is safe to consider committed, per the last rule
// of the Raft paper's Figure 2 ("Rules for Servers", Leaders):
//
//	If there exists an N such that N > commitIndex, a majority of
//	matchIndex[i] >= N, and log[N].term == currentTerm:
//	set commitIndex = N.
//
// The leader counts itself as one of the replicas holding entry N (its
// own log always has every entry it has sent), so in a 3-node cluster one
// follower with MatchIndex >= N is enough.
//
// The "log[N].term == currentTerm" condition is what the paper's Figure 8
// is about, and it's essential, not an optimization. A leader must NOT
// mark an entry from an EARLIER term as committed just because a majority
// now stores it: Figure 8 shows a sequence of crashes where such an entry,
// although on a majority, is later overwritten by another leader — which
// would mean a "committed" entry got lost. Restricting commitment-by-
// counting to entries from the leader's own term avoids that. Entries from
// earlier terms still get committed, just indirectly: once a current-term
// entry at N is committed, every entry before N is committed with it (by
// the Log Matching Property, every follower that has N also has them).
//
// Called after every successful AppendEntries reply (heartbeat.go), since
// that's when a MatchIndex can grow, and after Submit (submit.go), which
// is what lets a single-node cluster commit on its own.
//
// Must be called with n.mu held, by a Leader.
func (n *Node) advanceCommitIndexLocked() {
	clusterSize := len(n.peers) + 1

	// Scan from the end of the log down to just above the current
	// CommitIndex, and stop at the first N that qualifies: if a majority
	// has N, a majority also has everything below N, so the first hit
	// from the top is the highest committable index.
	for index := len(n.persistent.Log); index > n.volatile.CommitIndex; index-- {
		entryTerm := n.persistent.Log[index-1].Term

		// Figure 8 rule. Terms in a log never decrease, so once we see
		// an entry from an earlier term, every entry below it is from an
		// earlier term too: nothing further down can be committed by
		// counting, and we can stop.
		if entryTerm < n.persistent.CurrentTerm {
			return
		}
		if entryTerm != n.persistent.CurrentTerm {
			continue // can't happen for a correct leader; skip defensively
		}

		replicas := 1 // the leader itself
		for peerID := range n.peers {
			if n.volatile.MatchIndex[peerID] >= index {
				replicas++
			}
		}

		// Strict majority: more than half of the whole cluster.
		if replicas*2 > clusterSize {
			log.Printf("[node %d] committed up to index %d (term %d, on %d/%d nodes)",
				n.id, index, entryTerm, replicas, clusterSize)
			n.volatile.CommitIndex = index
			return
		}
	}
}
