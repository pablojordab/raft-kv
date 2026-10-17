// Package node contains the core Raft state machine: node states, elections,
// log replication, and the data structures that back them.
package node

// NodeState represents the three roles a Raft node can be in at any given
// time, per the Raft paper (Ongaro & Ousterhout, 2014).
type NodeState int

const (
	// Follower is the default state. A follower takes no initiative; it
	// only responds to RPCs from leaders and candidates.
	Follower NodeState = iota
	// Candidate is the state a node enters while campaigning for
	// leadership, after its election timeout fires without hearing from
	// a leader.
	Candidate
	// Leader is the state a node enters once it has received votes from
	// a majority of the cluster. The leader handles all client requests
	// and replicates log entries to the other nodes.
	Leader
)

// String implements fmt.Stringer so NodeState prints as a readable label
// (e.g. in logs) instead of a bare integer.
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry is a single entry in a node's replicated log. Each entry carries
// the term in which it was created by the leader, so followers can detect
// and discard entries from a stale or deposed leader.
type LogEntry struct {
	// Term is the leader's term when this entry was appended to the log.
	Term int
	// Index is this entry's position in the log (1-indexed, per the
	// Raft paper's convention — index 0 is reserved as the "no entry"
	// sentinel used before the first real entry).
	Index int
	// Command is the opaque payload the client wants applied to the
	// state machine once this entry is committed. Left as []byte for
	// now; Bloque 6 (the KV layer) will define how it's interpreted.
	Command []byte
}

// PersistentState holds the fields that MUST be written to stable storage
// before a node responds to any RPC, as required by the Raft paper (Figure
// 2, "Persistent state on all servers"). Losing this state on a crash would
// let a node vote twice in the same term or forget committed entries —
// both of which break Raft's safety guarantees.
//
// Bloque 4 (persistencia) is where this actually gets written to disk; for
// now it's just the in-memory shape.
type PersistentState struct {
	// CurrentTerm is the latest term this node has seen. Initialized to
	// 0 on first boot and increases monotonically.
	CurrentTerm int
	// VotedFor is the candidate ID this node voted for in CurrentTerm,
	// or -1 if it hasn't voted yet this term. Needed so a node never
	// votes for two different candidates in the same term, even across
	// a crash/restart.
	VotedFor int
	// Log is this node's replicated log. Entries here are not
	// necessarily committed — see VolatileState.CommitIndex for that.
	Log []LogEntry
}

// VolatileState holds the fields that are reset on every node and do NOT
// need to be persisted, since they can always be reconstructed from other
// information after a restart (per Raft paper, Figure 2, "Volatile state
// on all servers" and "Volatile state on leaders").
type VolatileState struct {
	// CommitIndex is the index of the highest log entry known to be
	// committed (i.e. replicated to a majority). Initialized to 0,
	// increases monotonically.
	CommitIndex int
	// LastApplied is the index of the highest log entry applied to this
	// node's local state machine. Initialized to 0, increases
	// monotonically, and is always <= CommitIndex.
	LastApplied int

	// --- Leader-only volatile state. Reinitialized after every
	// election; nil/empty on nodes that are not currently the leader. ---

	// NextIndex tracks, for each other node, the index of the next log
	// entry the leader believes it needs to send to that node.
	// Initialized to leader's last log index + 1 on election.
	NextIndex map[int]int
	// MatchIndex tracks, for each other node, the index of the highest
	// log entry known to be replicated on that node. Initialized to 0
	// on election; used to calculate CommitIndex via majority match.
	MatchIndex map[int]int
}
