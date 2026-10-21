package node

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
// safe once CommitIndex reaches its index (task 16).
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
	return index, term, true
}
