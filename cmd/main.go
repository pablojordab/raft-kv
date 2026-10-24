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
	"log/slog"
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
	logFormat := flag.String("log-format", "text", "log output format: text (human-readable key=value) or json (one JSON object per line)")
	logLevel := flag.String("log-level", "info", "minimum log level: debug, info, warn or error")
	flag.Parse()

	// Configure the process-wide default logger BEFORE creating the node:
	// node.NewNode derives its logger from slog.Default(). SetDefault also
	// routes anything written through the standard "log" package (e.g. by
	// net/rpc) into the same handler, so every line has the same format.
	logger, err := newLogger(*logFormat, *logLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	slog.SetDefault(logger)
	logger = logger.With("node", *id)

	if *addr == "" {
		fatal(logger, "missing required -addr flag")
	}

	peers, err := parsePeers(*peersFlag)
	if err != nil {
		fatal(logger, "invalid -peers", "err", err)
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
		fatal(logger, "rpc.Register failed", "err", err)
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		fatal(logger, "failed to listen", "addr", *addr, "err", err)
	}
	logger.Info("listening", "addr", *addr, "peers", fmt.Sprint(peers))

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
	go logStatePeriodically(ctx, n, logger)

	waitForShutdownSignal()

	logger.Info("shutting down")
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

// logStatePeriodically logs a "status" event once a second until ctx is
// cancelled, with this node's role, term, log length and commit index.
//
// The fields are read from one node.Status snapshot, so they always
// describe the same instant. Keep "state" and "term" as the first two
// fields, adjacent: in text format the line then contains
// "state=Leader term=3", which test/election_check.sh and .ps1 look for.
func logStatePeriodically(ctx context.Context, n *node.Node, logger *slog.Logger) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			st := n.Status()
			logger.Info("status", "state", st.State.String(), "term", st.Term,
				"log", len(st.Log), "commit", st.CommitIndex, "leader", st.LeaderID)
		}
	}
}

// newLogger builds the process's logger from the -log-format and
// -log-level flags. Text output (the default) is meant for people
// watching a terminal; JSON is meant for tools, such as the tests in test/,
// which merge the logs of several node processes into one timeline.
func newLogger(format, level string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid -log-level %q (want debug, info, warn or error)", level)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch format {
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	default:
		return nil, fmt.Errorf("invalid -log-format %q (want text or json)", format)
	}
}

// fatal logs an error and exits: slog has no Fatal level of its own.
func fatal(logger *slog.Logger, msg string, args ...any) {
	logger.Error(msg, args...)
	os.Exit(1)
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
