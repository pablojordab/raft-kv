package rpc

import (
	"net/rpc"

	"github.com/pablojordab/raft-kv/node"
)

// NetRPCTransport implements node.Transport (see node/transport.go) using
// Go's standard net/rpc package. It's the real, over-the-network
// implementation that cmd/main.go (task 9) will wire into every node it
// launches. Tests of the node package's election logic use a fake
// Transport instead, so they never need an actual listening socket.
type NetRPCTransport struct{}

// NewNetRPCTransport constructs a NetRPCTransport. It holds no state of its
// own — every call dials fresh — so a single instance can safely be shared
// across every Node in a process (there will only ever be one per process
// in this project anyway, started in cmd/main.go).
func NewNetRPCTransport() *NetRPCTransport {
	return &NetRPCTransport{}
}

// SendRequestVote dials peerAddr and issues a RequestVote RPC, satisfying
// node.Transport.
//
// It dials a fresh TCP connection for every single call and closes it
// immediately afterward, rather than keeping a persistent connection per
// peer open. That's less efficient, but it keeps this first version simple
// and removes an entire class of bugs (stale/broken connections, reconnect
// logic) — connection reuse is a reasonable later optimization, not a
// correctness requirement for a project at this scale.
func (t *NetRPCTransport) SendRequestVote(peerAddr string, candidateTerm, candidateID, lastLogIndex, lastLogTerm int) (term int, voteGranted bool, err error) {
	client, dialErr := rpc.Dial("tcp", peerAddr)
	if dialErr != nil {
		return 0, false, dialErr
	}
	defer client.Close()

	args := &RequestVoteArgs{
		Term:         candidateTerm,
		CandidateID:  candidateID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	var reply RequestVoteReply

	// "RaftServer.RequestVote" names the method net/rpc dispatches to:
	// the registered service name (the type name of whatever's passed to
	// rpc.Register — RaftServer, from server.go) plus the method name.
	if callErr := client.Call("RaftServer.RequestVote", args, &reply); callErr != nil {
		return 0, false, callErr
	}
	return reply.Term, reply.VoteGranted, nil
}

// SendAppendEntries dials peerAddr and issues an AppendEntries RPC,
// satisfying node.Transport.
//
// As of task 8, RaftServer has no AppendEntries method yet (it's task
// 13's responsibility — see server.go) — so every call made here will
// fail with an "rpc: can't find method" error until then. That's fine for
// now: a Leader already treats a failed heartbeat call as "this peer
// didn't get the message this round" rather than something fatal (see
// node/heartbeat.go), exactly like a failed RequestVote during an
// election. Once task 13 adds the receiver, these same calls start
// actually landing.
func (t *NetRPCTransport) SendAppendEntries(peerAddr string, leaderTerm, leaderID, prevLogIndex, prevLogTerm int, entries []node.LogEntry, leaderCommit int) (term int, success bool, err error) {
	client, dialErr := rpc.Dial("tcp", peerAddr)
	if dialErr != nil {
		return 0, false, dialErr
	}
	defer client.Close()

	args := &AppendEntriesArgs{
		Term:         leaderTerm,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	var reply AppendEntriesReply

	if callErr := client.Call("RaftServer.AppendEntries", args, &reply); callErr != nil {
		return 0, false, callErr
	}
	return reply.Term, reply.Success, nil
}