// Command raft-kv launches a single Raft node as its own OS process,
// listening for RPCs on -addr and talking to whatever peers -peers names.
//
// This is deliberately one-node-per-process, not one process spawning
// several node goroutines: it's what makes "kill the leader" (the manual
// fault-tolerance test this project is building toward) a literal
// `kill <pid>` or Ctrl+C on one terminal, rather than something simulated
// inside a single program. To run a 3-node cluster locally, start three
// of these in three terminals — see the README for the exact commands.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pablojordab/raft-kv/node"
	raftrpc "github.com/pablojordab/raft-kv/rpc"
)

func main() {
	id := flag.Int("id", 0, "this node's own numeric ID (must be unique within the cluster)")
	addr := flag.String("addr", "", "address this node listens on, e.g. 127.0.0.1:9001")
	peersFlag := flag.String("peers", "", "comma-separated peerID=address pairs, e.g. 2=127.0.0.1:9002,3=127.0.0.1:9003")
	flag.Parse()

	if *addr == "" {
		log.Fatal("missing required -addr flag")
	}

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		log.Fatalf("invalid -peers: %v", err)
	}

	// NetRPCTransport (rpc/client.go) is how this node will call
	// RequestVote/AppendEntries on every peer — the real, over-the-wire
	// implementation of node.Transport.
	transport := raftrpc.NewNetRPCTransport()
	n := node.NewNode(*id, peers, transport)

	// RaftServer (rpc/server.go) is the receiving side: it's what every
	// OTHER node's NetRPCTransport will be calling into, over the
	// listener started below.
	server := raftrpc.NewRaftServer(n)
	if err := rpc.Register(server); err != nil {
		log.Fatalf("rpc.Register failed: %v", err)
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}
	log.Printf("[node %d] listening on %s, peers=%v", *id, *addr, peers)

	// Accept connections until the listener is closed during shutdown
	// (serveConnections returns cleanly in that case, logging nothing).
	go serveConnections(listener)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Every node starts as a Follower and runs its election timer from
	// the moment it comes up — exactly as if it had just heard from a
	// leader, per election.go's NewNode doc comment.
	go n.RunElectionTimer(ctx)

	// Print this node's role and term once a second: the only way to
	// "watch an election happen" across three separate terminals is to
	// have each one log its own state on its own, since there's no
	// shared process to inspect.
	go logStatePeriodically(ctx, n)

	waitForShutdownSignal()

	log.Printf("[node %d] shutting down", *id)
	cancel()
	listener.Close()
}

// serveConnections accepts incoming RPC connections one at a time and
// hands each one to rpc.ServeConn in its own goroutine, so one slow or
// stuck peer connection can't block RPCs from any other peer.
func serveConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			// Accept returns an error once the listener is closed during
			// shutdown — that's the expected way this loop ends, not a
			// failure worth logging loudly.
			return
		}
		go rpc.ServeConn(conn)
	}
}

// logStatePeriodically prints this node's current role, term, log length
// and commit index once a second until ctx is cancelled. node.NodeState implements Stringer (see
// node/types.go), so %s prints "Follower"/"Candidate"/"Leader" rather than
// a bare integer.
func logStatePeriodically(ctx context.Context, n *node.Node) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Keep "state=... term=..." first and unchanged: the test
			// scripts in test/ parse exactly that prefix.
			log.Printf("[node %d] state=%s term=%d log=%d commit=%d",
				n.ID(), n.State(), n.CurrentTerm(), n.LogLength(), n.CommitIndex())
		}
	}
}

// waitForShutdownSignal blocks until this process receives SIGINT
// (Ctrl+C) or SIGTERM — the two ways task 11's "kill the leader" test
// will stop a node.
func waitForShutdownSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
}

// parsePeers turns "2=127.0.0.1:9002,3=127.0.0.1:9003" into
// {2: "127.0.0.1:9002", 3: "127.0.0.1:9003"}. An empty string is valid and
// returns an empty map — meaning this node has no peers, i.e. it's a
// single-node cluster.
func parsePeers(s string) (map[int]string, error) {
	peers := make(map[int]string)
	if s == "" {
		return peers, nil
	}
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed peer entry %q, want id=address", pair)
		}
		peerID, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("malformed peer id in %q: %w", pair, err)
		}
		peers[peerID] = parts[1]
	}
	return peers, nil
}
