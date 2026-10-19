package rpc

import "github.com/pablojordab/raft-kv/node"

// RaftServer adapts a *node.Node's pure consensus logic to the method
// shape Go's net/rpc requires (func(*ArgsType, *ReplyType) error, both
// exported, registered via rpc.Register).
//
// It's deliberately a thin translation layer and nothing more: every
// actual rule from the Raft paper lives in node.Node's methods (see
// node/vote.go for RequestVote), which this just calls and converts
// to/from the wire types defined in messages.go. Keeping the logic out of
// this type is what lets node.Node be unit-tested directly, without
// standing up a real RPC server or network connection — and it's also
// what makes the node package safely importable here without a cycle,
// since the dependency only runs one way (rpc depends on node, never the
// reverse).
type RaftServer struct {
	node *node.Node
}

// NewRaftServer wraps n so its RPC-callable methods can be registered with
// net/rpc (task 9 will do the actual rpc.Register / network listening).
func NewRaftServer(n *node.Node) *RaftServer {
	return &RaftServer{node: n}
}

// RequestVote is the net/rpc entry point for the RequestVote RPC. All of
// Figure 2's actual voting rules live in node.Node.HandleRequestVote; this
// method only unpacks args, calls it, and packs the result into reply.
func (s *RaftServer) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	term, granted := s.node.HandleRequestVote(args.Term, args.CandidateID, args.LastLogIndex, args.LastLogTerm)
	reply.Term = term
	reply.VoteGranted = granted
	return nil
}

// AppendEntries is not implemented yet — that's task 13 (log replication),
// not this one. Left out entirely rather than stubbed, so a missing
// implementation shows up as a compile error wherever it's expected,
// instead of a silent no-op at runtime.