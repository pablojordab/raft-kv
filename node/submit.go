package node

import "time"

// commitPollInterval is how often WaitForCommit re-checks CommitIndex.
// Commits happen on replication rounds every heartbeatInterval (50ms), so
// 10ms keeps the added latency small without busy-looping.
const commitPollInterval = 10 * time.Millisecond

// Submit asks this node to append a new command to the replicated log.
// Only the Leader accepts commands (§5.1: "the leader handles all client
// requests"); on any other node it returns isLeader=false and changes
// nothing, so the caller can go find the real leader instead.
//
// On the Leader, the command is appended to its own log immediately, and
// returned index/term identify the new entry. It's then shipped to
// followers by the next replication round (heartbeat.go, within
// heartbeatInterval).
//
// Note what a successful return does NOT mean: the entry is not yet
// committed. It exists only on the leader, and could still be discarded
// if this leader crashes before a majority stores it. A command is only
// safe once CommitIndex reaches its index (see commit.go).
func (n *Node) Submit(command []byte) (index int, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return 0, n.persistent.CurrentTerm, false
	}

	lastIndex, _ := n.lastLogIndexAndTermLocked()
	index = lastIndex + 1
	term = n.persistent.CurrentTerm

	// Copy the command: the caller still owns its slice and might reuse
	// it, which would otherwise silently rewrite an entry in our log.
	cmd := make([]byte, len(command))
	copy(cmd, command)

	n.persistent.Log = append(n.persistent.Log, LogEntry{Term: term, Index: index, Command: cmd})

	// In a multi-node cluster this never commits anything yet (no
	// follower has the new entry). It matters for a single-node cluster,
	// where the leader alone is already a majority and there are no
	// replies to trigger the check from heartbeat.go.
	n.advanceCommitIndexLocked()
	return index, term, true
}

// WaitForCommit blocks until the entry that Submit returned as (index,
// term) is committed, or until timeout. It returns true only if that exact
// entry was committed.
//
// "That exact entry" matters: after a leader change, the slot at `index`
// may be committed holding a DIFFERENT entry, written by a new leader that
// never received ours (the paper's §5.3: a leader's uncommitted entries can
// be overwritten). So once CommitIndex reaches index, we also check the
// term of what's stored there: same term means it's our entry (by the Log
// Matching Property, index+term identify an entry uniquely); a different
// term, or no entry at all because the log was truncated, means ours was
// lost and the caller must not report success.
//
// A false return is therefore NOT "the command failed": on a timeout, the
// entry may still commit later through a new leader. The caller only knows
// the outcome is uncertain.
func (n *Node) WaitForCommit(index, term int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		n.mu.Lock()
		storedTerm, present := n.termAtLocked(index)
		committed := n.volatile.CommitIndex >= index
		n.mu.Unlock()

		if !present || storedTerm != term {
			return false // our entry was overwritten or truncated away
		}
		if committed {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(commitPollInterval)
	}
}
