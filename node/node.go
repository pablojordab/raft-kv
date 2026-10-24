package node

import (
	"log/slog"
	"sync"
	"time"
)

// Node is a single Raft server: it holds this server's view of the
// cluster's persistent and volatile state, its current role
// (Follower/Candidate/Leader), and the identifiers it needs to talk to its
// peers over RPC.
//
// All fields that can be read or written concurrently — by the node's own
// election timer, by incoming RPC handlers, and (once Block 1's leader
// logic lands) by the goroutines driving elections and heartbeats — are
// guarded by mu. Callers must hold mu before touching any field below it.
type Node struct {
	mu sync.Mutex

	// id is this node's own identifier within the cluster. Matches the
	// key this node is registered under in peers (see below) from every
	// other node's point of view.
	id int

	// peers maps every OTHER node's id to the network address its RPC
	// server listens on (e.g. "127.0.0.1:9001"). It does not include
	// this node's own id — a node never calls itself over RPC.
	peers map[int]string

	// transport is how this node calls peers over the network (currently
	// just RequestVote, used by the election campaign in campaign.go).
	// It's an interface — see transport.go — specifically so this
	// package never has to import the rpc package's concrete client,
	// and so tests can supply a fake that returns canned votes with no
	// real network involved.
	transport Transport

	state NodeState

	persistent PersistentState
	volatile   VolatileState

	// electionResetEvent is the timestamp of the last event that should
	// postpone this node starting an election: hearing a valid heartbeat
	// or AppendEntries from the current leader, or granting a vote to a
	// candidate. The election timer (election.go) compares time.Since
	// against this to decide whether it's genuinely timed out, rather
	// than using time.Timer.Reset — see election.go's doc comment for
	// why that avoids a well-known class of timer-reset races in Go.
	electionResetEvent time.Time

	// leaderID is the id of the node this one believes is the leader of
	// its current term, or -1 if it doesn't know yet (e.g. mid-election).
	// It's only a hint, used to redirect clients that contacted a
	// follower (see Submit in rpc/server.go): it's learned from accepted
	// AppendEntries (append.go), set to our own id when we win an
	// election, and cleared whenever our term advances.
	leaderID int

	// logger writes this node's structured log events (log/slog). Every
	// event carries node=<id>; call sites add the fields that matter for
	// that event (term, state, peer, index...), so logs from several
	// processes can be merged and filtered by field instead of parsed
	// with regexes. It's derived from slog.Default() when the node is
	// created, so cmd/main.go chooses the format and level for the whole
	// process just by setting the default logger first.
	logger *slog.Logger
}

// NewNode constructs a Node in the initial state every Raft server starts
// in: a Follower, in term 0, with an empty log and no vote cast yet. Per
// the Raft paper, every node starts as a Follower — nodes become
// Candidates only after their election timeout fires without hearing from
// a leader, which Block 1's election timer (not yet implemented) will
// drive.
//
// id is this node's own identifier. peers maps every other node's id to
// its RPC address, and must not contain an entry for id itself. transport
// is how this node will call those peers (e.g. an *rpc.NetRPCTransport in
// production, or a fake in tests) — see transport.go for why it's taken as
// an interface here instead of a concrete rpc-package type.
func NewNode(id int, peers map[int]string, transport Transport) *Node {
	return &Node{
		id:        id,
		peers:     peers,
		transport: transport,
		state:     Follower,
		persistent: PersistentState{
			CurrentTerm: 0,
			VotedFor:    -1, // -1 means "no vote cast yet this term"
			Log:         nil,
		},
		volatile: VolatileState{
			CommitIndex: 0,
			LastApplied: 0,
			// NextIndex/MatchIndex are leader-only and left nil here;
			// they're allocated when a node wins an election (Block 1).
			NextIndex:  nil,
			MatchIndex: nil,
		},
		// Start the election clock as of construction, so a freshly
		// created node gets a full timeout window before its first
		// election, same as if it had just heard from a leader.
		electionResetEvent: time.Now(),
		leaderID:           -1, // no leader known yet
		logger:             slog.Default().With("node", id),
	}
}

// ID returns this node's own identifier. Safe to call without holding a
// lock: id is set once in NewNode and never changes afterward.
func (n *Node) ID() int {
	return n.id
}

// State returns this node's current role. Takes the lock, since state can
// change concurrently once the election timer and RPC handlers are wired
// up in later tasks.
func (n *Node) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// CurrentTerm returns this node's latest known term.
func (n *Node) CurrentTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.persistent.CurrentTerm
}
