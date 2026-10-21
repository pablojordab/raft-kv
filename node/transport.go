package node

// Transport is how a Node reaches its peers over the network to campaign
// for votes. It's defined here, in the node package, rather than depending
// on the rpc package's concrete client — for the same import-cycle reason
// RequestVote's receiving side is split across packages (see vote.go and
// rpc/server.go): the rpc package already imports node (for LogEntry), so
// node cannot import rpc back without creating a cycle. Taking and
// returning plain values (not rpc.RequestVoteArgs/Reply) is what lets this
// interface live here safely.
//
// A second benefit, beyond breaking the cycle: node's election logic can be
// unit-tested against a fake Transport that returns canned votes instantly,
// with no real network, no goroutine timing flakiness, and no dependency on
// rpc at all.
type Transport interface {
	// SendRequestVote dials peerAddr and issues a RequestVote RPC with the
	// given arguments (mirroring the Raft paper's Figure 2 RequestVote RPC
	// parameters), returning the responder's current term and whether it
	// granted the vote.
	//
	// A non-nil error means the RPC itself failed (peer unreachable,
	// connection refused, timeout, etc.) — callers treat that as "no
	// reply received," not as a reason to abort the election: a candidate
	// can still win a majority even if some peers never answer.
	SendRequestVote(peerAddr string, candidateTerm, candidateID, lastLogIndex, lastLogTerm int) (term int, voteGranted bool, err error)

	// SendAppendEntries dials peerAddr and issues an AppendEntries RPC,
	// mirroring the Raft paper's Figure 2 AppendEntries parameters. The
	// same RPC carries both heartbeats (entries empty) and log
	// replication (see heartbeat.go).
	//
	// Returns the follower's reply (see AppendEntriesResult). A non-nil
	// error means the RPC itself failed, which a Leader treats the same
	// way a Candidate treats a failed RequestVote: that one peer just
	// didn't get this round's message, and the next round retries.
	SendAppendEntries(peerAddr string, leaderTerm, leaderID, prevLogIndex, prevLogTerm int, entries []LogEntry, leaderCommit int) (AppendEntriesResult, error)
}

// AppendEntriesResult is a follower's reply to AppendEntries, as plain
// values so it can live in this package (see the import-cycle note on
// Transport above).
type AppendEntriesResult struct {
	// Term is the follower's current term, so a stale leader can
	// discover it's been superseded and step down.
	Term int
	// Success is true if the follower's log matched prevLogIndex/
	// prevLogTerm and the entries were accepted.
	Success bool

	// ConflictTerm and ConflictIndex are only meaningful when Success
	// is false because of a log mismatch. They let the leader back up
	// NextIndex by a whole term at a time instead of one entry per
	// round (the optimization described at the end of the Raft paper's
	// §5.3; see append.go for how they're filled in and heartbeat.go
	// for how they're used).
	//
	// ConflictTerm is the term of the follower's entry at prevLogIndex,
	// or 0 if the follower's log is too short to have one.
	// ConflictIndex is the first index the follower holds for
	// ConflictTerm, or, when ConflictTerm is 0, one past the end of the
	// follower's log.
	ConflictTerm  int
	ConflictIndex int
}
