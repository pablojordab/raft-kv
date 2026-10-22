package rpc

import (
	"fmt"
	"net"
	"net/rpc"
	"time"

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
// satisfying node.Transport. Used for both heartbeats and log replication;
// the receiving side is RaftServer.AppendEntries (server.go).
func (t *NetRPCTransport) SendAppendEntries(peerAddr string, leaderTerm, leaderID, prevLogIndex, prevLogTerm int, entries []node.LogEntry, leaderCommit int) (node.AppendEntriesResult, error) {
	client, dialErr := rpc.Dial("tcp", peerAddr)
	if dialErr != nil {
		return node.AppendEntriesResult{}, dialErr
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
		return node.AppendEntriesResult{}, callErr
	}
	return node.AppendEntriesResult{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictTerm:  reply.ConflictTerm,
		ConflictIndex: reply.ConflictIndex,
	}, nil
}

// clientDialTimeout bounds how long SubmitCommand waits to connect to a
// node. Connecting to a dead node on localhost fails immediately anyway;
// this mostly protects against unreachable addresses on a real network.
const clientDialTimeout = 1 * time.Second

// SubmitCommand sends a command to the node at addr through its
// client-facing Submit RPC (server.go) and returns that node's reply. It's
// the building block of cmd/client: the caller inspects the reply to
// decide whether to follow the LeaderID hint, try another node, or report
// the result.
//
// Unlike SendRequestVote/SendAppendEntries, this bounds the whole call,
// not just the dial: the server may legitimately take up to
// submitCommitTimeout to answer, and a client must never hang forever on a
// node that accepted the connection but stopped responding.
func SubmitCommand(addr string, command []byte) (SubmitReply, error) {
	conn, err := net.DialTimeout("tcp", addr, clientDialTimeout)
	if err != nil {
		return SubmitReply{}, err
	}
	client := rpc.NewClient(conn)
	defer client.Close()

	var reply SubmitReply
	call := client.Go("RaftServer.Submit", &SubmitArgs{Command: command}, &reply, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			return SubmitReply{}, call.Error
		}
		return reply, nil
	case <-time.After(submitCommitTimeout + clientDialTimeout):
		return SubmitReply{}, fmt.Errorf("no reply from %s within %v", addr, submitCommitTimeout+clientDialTimeout)
	}
}
