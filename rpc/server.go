package rpc

import (
	"time"

	"github.com/pablojordab/raft-kv/node"
)

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

// AppendEntries is the net/rpc entry point for the AppendEntries RPC, used
// both for heartbeats and log replication. All of Figure 2's receiver rules
// live in node.Node.HandleAppendEntries (node/append.go); this method only
// unpacks args, calls it, and packs the result into reply, exactly like
// RequestVote above.
func (s *RaftServer) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	res := s.node.HandleAppendEntries(args.Term, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	reply.Term = res.Term
	reply.Success = res.Success
	reply.ConflictTerm = res.ConflictTerm
	reply.ConflictIndex = res.ConflictIndex
	return nil
}

// submitCommitTimeout bounds how long a Submit RPC waits for the client's
// command to commit before answering with Committed=false (outcome
// unknown). A healthy cluster commits within one or two replication rounds
// (~50-100ms); 2s leaves room for slow peers without hanging a client
// forever when the leader can't reach a majority.
const submitCommitTimeout = 2 * time.Second

// Submit is the client-facing RPC (see cmd/client): it appends a command to
// the replicated log through this node and waits until it's committed.
//
// If this node isn't the leader, nothing is appended and the reply carries
// IsLeader=false plus this node's best guess of who the leader is, so the
// client can redirect. On the leader, the reply is sent only once the entry
// is committed (Committed=true) or the wait times out (Committed=false,
// outcome unknown — see node.WaitForCommit).
func (s *RaftServer) Submit(args *SubmitArgs, reply *SubmitReply) error {
	index, term, isLeader := s.node.Submit(args.Command)
	reply.IsLeader = isLeader
	reply.LeaderID = s.node.LeaderID()
	if !isLeader {
		return nil
	}
	reply.Index = index
	reply.Term = term
	reply.Committed = s.node.WaitForCommit(index, term, submitCommitTimeout)
	return nil
}

// Status returns a snapshot of this node's state, including a copy of its
// whole log. Read-only: it never changes the node. It's how process-level
// tests (test/) inspect what each node stores.
func (s *RaftServer) Status(args *StatusArgs, reply *StatusReply) error {
	st := s.node.Status()
	reply.ID = st.ID
	reply.State = st.State.String()
	reply.Term = st.Term
	reply.LeaderID = st.LeaderID
	reply.CommitIndex = st.CommitIndex
	reply.LastApplied = st.LastApplied
	reply.Log = st.Log
	return nil
}
