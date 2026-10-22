// Command client sends one command to a raft-kv cluster and reports whether
// the cluster committed it.
//
// Usage (from the repo root, with the cluster from the README running):
//
//	go run ./cmd/client -cluster="1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003" SET x 1
//
// The client doesn't need to know which node is the leader. It contacts
// any node; a follower answers "not leader, try node N" and the client
// follows that hint. If a node is down, or an election is in progress and
// nobody knows the leader yet, it tries the next node, until -timeout.
//
// Only SET is supported for now. Reading values back (GET) needs the
// key-value layer that applies committed commands, which comes later.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	raftrpc "github.com/pablojordab/raft-kv/rpc"
)

// retryPause is how long to wait before trying again when no node knows
// who the leader is. Elections take one election timeout (150-300ms), so
// retrying much faster than this would only produce more "no leader"
// answers.
const retryPause = 100 * time.Millisecond

func main() {
	clusterFlag := flag.String("cluster", "", "comma-separated id=address pairs for every node, e.g. 1=127.0.0.1:9001,2=127.0.0.1:9002")
	timeout := flag.Duration("timeout", 5*time.Second, "give up if no leader accepts the command within this time")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: client -cluster=id=addr,... SET <key> <value>")
		flag.PrintDefaults()
	}
	flag.Parse()

	cluster, err := parseCluster(*clusterFlag)
	if err != nil || len(cluster) == 0 {
		fmt.Fprintln(os.Stderr, "error: -cluster is required and must look like 1=127.0.0.1:9001,2=...")
		os.Exit(1)
	}

	command, err := buildCommand(flag.Args())
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		flag.Usage()
		os.Exit(1)
	}

	os.Exit(submit(cluster, command, *timeout))
}

// buildCommand validates the arguments and turns them into the bytes stored
// in the replicated log: "SET <key> <value>". The value may contain spaces
// (everything after the key is joined back together); the key may not,
// since a space is what separates key from value.
func buildCommand(args []string) ([]byte, error) {
	if len(args) < 3 || !strings.EqualFold(args[0], "SET") {
		return nil, fmt.Errorf("expected: SET <key> <value>")
	}
	key := args[1]
	value := strings.Join(args[2:], " ")
	return []byte("SET " + key + " " + value), nil
}

// submit tries nodes until a leader commits the command or the timeout
// expires. Returns the process exit code:
//
//	0: committed
//	1: no leader accepted it in time (the command was NOT applied)
//	2: a leader accepted it but couldn't confirm the commit (UNKNOWN)
func submit(cluster map[int]string, command []byte, timeout time.Duration) int {
	ids := make([]int, 0, len(cluster))
	for id := range cluster {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	// nodeAfter returns the id that follows `id` in sorted order, wrapping
	// around. Moving on always starts from the node that just failed, even
	// if we only reached it by following a redirect; otherwise a redirect
	// to a dead leader could send us back to that same dead node twice.
	nodeAfter := func(id int) int {
		for i, candidate := range ids {
			if candidate == id {
				return ids[(i+1)%len(ids)]
			}
		}
		return ids[0]
	}

	deadline := time.Now().Add(timeout)
	target := ids[0]

	// lastUnreachable is the last node we failed to contact. Right after a
	// leader crashes, followers keep naming it as leader until their own
	// election timeout fires (up to 300ms), so following that stale hint
	// would just hit the dead node again.
	lastUnreachable := -1

	for time.Now().Before(deadline) {
		reply, err := raftrpc.SubmitCommand(cluster[target], command)

		switch {
		case err != nil:
			// Node down or not responding: move on to the next one.
			fmt.Fprintf(os.Stderr, "node %d unreachable (%v), trying another node\n", target, err)
			lastUnreachable = target
			target = nodeAfter(target)
			time.Sleep(retryPause)

		case !reply.IsLeader:
			_, known := cluster[reply.LeaderID]
			if known && reply.LeaderID != target && reply.LeaderID != lastUnreachable {
				// The node told us who the leader is: go straight there.
				fmt.Fprintf(os.Stderr, "node %d is not the leader, redirecting to node %d\n", target, reply.LeaderID)
				target = reply.LeaderID
				continue
			}
			// Nobody knows the leader yet (an election is likely in
			// progress): wait a little and try the next node.
			fmt.Fprintf(os.Stderr, "node %d doesn't know the leader yet, retrying\n", target)
			target = nodeAfter(target)
			time.Sleep(retryPause)

		case reply.Committed:
			fmt.Printf("OK: committed at index %d (term %d) by leader node %d\n", reply.Index, reply.Term, target)
			return 0

		default:
			// The leader stored the command but couldn't get it committed
			// before its wait timed out (e.g. it lost contact with the
			// majority, or was replaced). The command may still commit
			// later, so retrying could apply it TWICE. Report the
			// uncertainty instead of guessing.
			fmt.Printf("UNKNOWN: leader node %d stored the command at index %d (term %d) but could not confirm it was committed; it may or may not be applied\n",
				target, reply.Index, reply.Term)
			return 2
		}
	}

	fmt.Println("FAILED: no leader accepted the command before the timeout; it was not applied")
	return 1
}

// parseCluster turns "1=127.0.0.1:9001,2=127.0.0.1:9002" into
// {1: "127.0.0.1:9001", 2: "127.0.0.1:9002"}.
func parseCluster(s string) (map[int]string, error) {
	cluster := make(map[int]string)
	if s == "" {
		return cluster, nil
	}
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed entry %q, want id=address", pair)
		}
		id, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("malformed id in %q: %w", pair, err)
		}
		cluster[id] = parts[1]
	}
	return cluster, nil
}
