// Package rpc defines the wire messages Raft nodes exchange, and will host
// the net/rpc-based transport that sends them.
//
// We use Go's standard library net/rpc instead of gRPC: it needs no code
// generation step and no external dependencies, which keeps the project
// trivial to build on any machine with just the Go toolchain installed.
// It's also the mechanism used by MIT's 6.824 Distributed Systems course
// labs, so it's a recognized choice for this specific kind of project, not
// a shortcut. net/rpc requires every RPC method to have the shape
// func (t *T) Method(args *ArgsType, reply *ReplyType) error — the structs
// below are the Args/Reply pairs for Raft's two core RPCs.
package rpc

import "github.com/pablojordab/raft-kv/node"

// RequestVoteArgs is sent by a candidate to gather votes, per the Raft
// paper (Figure 2, "RequestVote RPC").
type RequestVoteArgs struct {
	// Term is the candidate's current term.
	Term int
	// CandidateID identifies the candidate requesting the vote.
	CandidateID int
	// LastLogIndex is the index of the candidate's last log entry, used
	// by the receiver to enforce the "up-to-date log" election
	// restriction (a candidate can't win votes if its log is behind).
	LastLogIndex int
	// LastLogTerm is the term of the candidate's last log entry, used
	// alongside LastLogIndex for the same up-to-date check.
	LastLogTerm int
}

// RequestVoteReply is the receiver's response to a RequestVoteArgs.
type RequestVoteReply struct {
	// Term is the receiver's current term, so the candidate can update
	// itself if it discovers it's behind.
	Term int
	// VoteGranted is true if the receiver cast its vote for this
	// candidate in this term.
	VoteGranted bool
}

// SubmitArgs is sent by a client (cmd/client) to ask the cluster to apply
// a command. It isn't part of the Raft paper's Figure 2: it's this
// project's client-facing RPC, served by the same RaftServer.
type SubmitArgs struct {
	// Command is the opaque command to append to the replicated log,
	// e.g. "SET x 1". Raft itself never interprets it.
	Command []byte
}

// SubmitReply is the node's answer to a SubmitArgs.
type SubmitReply struct {
	// IsLeader is false if the contacted node isn't the leader; the
	// command was then NOT accepted, and the client should retry
	// elsewhere.
	IsLeader bool
	// LeaderID is the id of the node the contacted node believes is
	// the leader, or -1 if unknown (e.g. an election is in progress).
	// Clients use it to go straight to the leader.
	LeaderID int
	// Index and Term identify the log entry the command was stored in
	// (only meaningful when IsLeader is true).
	Index int
	Term  int
	// Committed is true if that entry was committed (stored on a
	// majority) before the server's wait timed out. If IsLeader is true
	// but Committed is false, the outcome is UNKNOWN: the command may
	// still commit later, or may have been lost in a leader change.
	Committed bool
}

// AppendEntriesArgs is sent by the leader both to replicate log entries
// and as a heartbeat (when Entries is empty), per the Raft paper (Figure
// 2, "AppendEntries RPC").
type AppendEntriesArgs struct {
	// Term is the leader's current term.
	Term int
	// LeaderID lets a follower redirect clients to the leader, and lets
	// it recognize who the current leader is.
	LeaderID int
	// PrevLogIndex is the index of the log entry immediately preceding
	// the new ones, used by the receiver to check log consistency
	// before accepting the new entries.
	PrevLogIndex int
	// PrevLogTerm is the term of the entry at PrevLogIndex, for the
	// same consistency check.
	PrevLogTerm int
	// Entries holds the log entries to append, in order. Empty for a
	// heartbeat.
	Entries []node.LogEntry
	// LeaderCommit is the leader's CommitIndex, letting followers know
	// which entries are safe to apply to their own state machine.
	LeaderCommit int
}

// AppendEntriesReply is the receiver's response to an AppendEntriesArgs.
type AppendEntriesReply struct {
	// Term is the receiver's current term, so the leader can step down
	// if it discovers it's stale.
	Term int
	// Success is true if the receiver's log matched PrevLogIndex/
	// PrevLogTerm, meaning the new entries (if any) were accepted.
	Success bool
	// ConflictTerm and ConflictIndex are the receiver's hint for the
	// leader when Success is false because of a log mismatch, so the
	// leader can skip back a whole term at once. See
	// node.AppendEntriesResult for their exact meaning.
	ConflictTerm  int
	ConflictIndex int
}
