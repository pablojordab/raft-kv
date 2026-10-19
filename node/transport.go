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
	// mirroring the Raft paper's Figure 2 AppendEntries parameters.
	//
	// As of task 8, a Leader only ever calls this with entries left nil
	// (a heartbeat: "I'm still the Leader, don't start an election") and
	// prevLogIndex/prevLogTerm left at 0 — real log replication (filling
	// in entries, and caring whether success is true) is task 13.
	//
	// Returns the responder's current term (so a stale Leader can
	// discover it's been superseded and step down) and whether it
	// reported success. A non-nil error means the RPC itself failed,
	// which a Leader treats the same way a Candidate treats a failed
	// RequestVote: that one peer just didn't get this round's message.
	SendAppendEntries(peerAddr string, leaderTerm, leaderID, prevLogIndex, prevLogTerm int, entries []LogEntry, leaderCommit int) (term int, success bool, err error)
}