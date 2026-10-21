// Package test holds end-to-end tests that run real raft-kv node
// processes, as opposed to the unit-level checks of individual handlers.
package test

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Task 11: kill the leader of a real 3-process cluster and verify the two
// survivors elect a new leader among themselves, in a strictly higher term.
//
// This is deliberately a process-level test, not an in-memory simulation:
// every node is a separate OS process built from cmd/main.go, talking over
// real TCP sockets, and "killing the leader" is an actual process kill.
// It's the same failure a real deployment would see when a machine dies.

const (
	basePort = 9201 // distinct from manual runs (9001-) and election_check (9101-)

	electionWait = 5 * time.Second // upper bound for any one election to settle
	stableFor    = 1 * time.Second // how long a result must hold to count as settled
	pollEvery    = 100 * time.Millisecond
)

// statusLine matches the periodic line cmd/main.go prints once a second.
var statusLine = regexp.MustCompile(`state=(\w+) term=(\d+)`)

// syncBuffer is a bytes.Buffer safe for one process writing to it while
// the test goroutine reads from it concurrently.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// testNode is one running node process plus its captured log output.
type testNode struct {
	id   int
	cmd  *exec.Cmd
	logs *syncBuffer
	dead bool
}

// lastStatus returns the node's most recently logged state and term, or
// ok=false if it hasn't printed a status line yet.
func (n *testNode) lastStatus() (state string, term int, ok bool) {
	matches := statusLine.FindAllStringSubmatch(n.logs.String(), -1)
	if len(matches) == 0 {
		return "", 0, false
	}
	last := matches[len(matches)-1]
	term, _ = strconv.Atoi(last[2])
	return last[1], term, true
}

func (n *testNode) kill() {
	if n.dead {
		return
	}
	_ = n.cmd.Process.Kill()
	_ = n.cmd.Wait()
	n.dead = true
}

// buildNodeBinary compiles cmd/main.go once into a temp dir, so the test
// runs exactly the binary a user would run, not a separately-compiled copy
// of the node logic.
func buildNodeBinary(t *testing.T) string {
	t.Helper()
	name := "raft-kv-node"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	bin := filepath.Join(t.TempDir(), name)
	build := exec.Command("go", "build", "-o", bin, "../cmd")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building node binary failed: %v\n%s", err, out)
	}
	return bin
}

func addr(id int) string { return fmt.Sprintf("127.0.0.1:%d", basePort+id) }

// startCluster launches `size` node processes, each knowing every other
// node's address, and registers cleanup so no process outlives the test.
func startCluster(t *testing.T, bin string, size int) []*testNode {
	t.Helper()
	nodes := make([]*testNode, 0, size)
	for id := 1; id <= size; id++ {
		peers := ""
		for other := 1; other <= size; other++ {
			if other == id {
				continue
			}
			if peers != "" {
				peers += ","
			}
			peers += fmt.Sprintf("%d=%s", other, addr(other))
		}
		logs := &syncBuffer{}
		cmd := exec.Command(bin, "-id="+strconv.Itoa(id), "-addr="+addr(id), "-peers="+peers)
		cmd.Stdout = logs
		cmd.Stderr = logs // Go's log package writes to stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("starting node %d: %v", id, err)
		}
		nodes = append(nodes, &testNode{id: id, cmd: cmd, logs: logs})
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.kill()
		}
	})
	return nodes
}

// waitForStableLeader polls the live nodes until exactly one reports
// Leader, every other live node reports Follower, all agree on the term,
// and that has held unchanged for stableFor. Returns the leader and term.
func waitForStableLeader(t *testing.T, nodes []*testNode) (*testNode, int) {
	t.Helper()
	deadline := time.Now().Add(electionWait)
	var candidate *testNode
	var candidateTerm int
	var since time.Time

	for time.Now().Before(deadline) {
		leader, term, ok := currentLeader(nodes)
		switch {
		case !ok:
			candidate = nil
		case candidate != leader || candidateTerm != term:
			candidate, candidateTerm, since = leader, term, time.Now()
		case time.Since(since) >= stableFor:
			return leader, term
		}
		time.Sleep(pollEvery)
	}
	t.Fatalf("no stable leader within %v; last logs:\n%s", electionWait, dumpLogs(nodes))
	return nil, 0
}

// currentLeader checks one snapshot of the live nodes' latest status.
func currentLeader(nodes []*testNode) (*testNode, int, bool) {
	var leader *testNode
	term := -1
	for _, n := range nodes {
		if n.dead {
			continue
		}
		state, t, ok := n.lastStatus()
		if !ok {
			return nil, 0, false
		}
		if term == -1 {
			term = t
		} else if t != term {
			return nil, 0, false // nodes disagree on the term
		}
		switch state {
		case "Leader":
			if leader != nil {
				return nil, 0, false // two leaders in one snapshot
			}
			leader = n
		case "Follower":
		default:
			return nil, 0, false // someone is still a Candidate
		}
	}
	return leader, term, leader != nil
}

func dumpLogs(nodes []*testNode) string {
	out := ""
	for _, n := range nodes {
		out += fmt.Sprintf("--- node %d (dead=%v) ---\n%s\n", n.id, n.dead, n.logs.String())
	}
	return out
}

func TestLeaderFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping process-level failover test in -short mode")
	}

	bin := buildNodeBinary(t)
	nodes := startCluster(t, bin, 3)

	oldLeader, oldTerm := waitForStableLeader(t, nodes)
	t.Logf("initial leader: node %d in term %d", oldLeader.id, oldTerm)

	oldLeader.kill()
	t.Logf("killed node %d", oldLeader.id)

	newLeader, newTerm := waitForStableLeader(t, nodes)
	t.Logf("new leader: node %d in term %d", newLeader.id, newTerm)

	if newLeader == oldLeader {
		t.Fatalf("dead node %d still reported as leader", oldLeader.id)
	}
	if newTerm <= oldTerm {
		t.Fatalf("new leader's term %d should be greater than the old term %d", newTerm, oldTerm)
	}
}
