// Package test holds end-to-end tests that run real raft-kv node
// processes, as opposed to the in-memory unit tests in node/.
//
// Every node here is a separate OS process built from cmd/main.go, talking
// to the others over real TCP sockets. Killing a node is an actual process
// kill, the same failure a real deployment sees when a machine dies. This
// file holds the helpers shared by every test in the package: building the
// binaries, launching a cluster, and waiting for it to settle.
package test

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	electionWait = 5 * time.Second // upper bound for any one election to settle
	stableFor    = 1 * time.Second // how long a result must hold to count as settled
	pollEvery    = 100 * time.Millisecond
)

// Each test uses its own port range, so tests never collide with each
// other, with manual runs (9001-), or with election_check (9101-).
const (
	failoverBasePort    = 9200
	replicationBasePort = 9300
)

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
	addr string
	cmd  *exec.Cmd
	logs *syncBuffer
	dead bool
}

// lastStatus returns the state and term from the node's most recent
// "status" log event (printed by cmd/main.go once a second), or ok=false if
// it hasn't logged one yet.
func (n *testNode) lastStatus() (state string, term int, ok bool) {
	events := parseEvents(n.logs.String())
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Msg == "status" {
			return events[i].str("state"), events[i].int("term"), true
		}
	}
	return "", 0, false
}

func (n *testNode) kill() {
	if n.dead {
		return
	}
	_ = n.cmd.Process.Kill()
	_ = n.cmd.Wait()
	n.dead = true
}

// buildBinary compiles the package at pkg (relative to this directory,
// e.g. "../cmd") into a temp dir, so tests run exactly the binaries a user
// would run, not a separately compiled copy of the logic.
func buildBinary(t *testing.T, pkg, name string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	bin := filepath.Join(t.TempDir(), name)
	build := exec.Command("go", "build", "-o", bin, pkg)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building %s failed: %v\n%s", pkg, err, out)
	}
	return bin
}

func buildNodeBinary(t *testing.T) string   { return buildBinary(t, "../cmd", "raft-kv-node") }
func buildClientBinary(t *testing.T) string { return buildBinary(t, "../cmd/client", "raft-kv-client") }

// startCluster launches `size` node processes on ports basePort+1 ..
// basePort+size, each knowing every other node's address, and registers
// cleanup so no process outlives the test.
func startCluster(t *testing.T, bin string, size, basePort int) []*testNode {
	t.Helper()
	addrOf := func(id int) string { return fmt.Sprintf("127.0.0.1:%d", basePort+id) }

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
			peers += fmt.Sprintf("%d=%s", other, addrOf(other))
		}
		logs := &syncBuffer{}
		cmd := startNodeProcess(t, bin, id, addrOf(id), peers, logs)
		nodes = append(nodes, &testNode{id: id, addr: addrOf(id), cmd: cmd, logs: logs})
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.kill()
		}
	})
	registerLogChecks(t, func() []*testNode { return nodes })
	return nodes
}

// startNodeProcess launches one node binary writing JSON logs at debug
// level into logs. JSON makes every log line a structured event the tests
// can parse (logevents_test.go); debug level makes sure the timeline
// printed when a test fails includes every vote, rejection and commit.
func startNodeProcess(t *testing.T, bin string, id int, addr, peers string, logs *syncBuffer) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(bin,
		"-id="+strconv.Itoa(id), "-addr="+addr, "-peers="+peers,
		"-log-format=json", "-log-level=debug")
	cmd.Stdout = logs
	cmd.Stderr = logs // slog writes to stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting node %d: %v", id, err)
	}
	return cmd
}

// clusterFlag renders the cluster in the -cluster format cmd/client takes:
// "1=127.0.0.1:9301,2=127.0.0.1:9302,...".
func clusterFlag(nodes []*testNode) string {
	out := ""
	for i, n := range nodes {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf("%d=%s", n.id, n.addr)
	}
	return out
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
	t.Fatalf("no stable leader within %v among nodes %v", electionWait, ids(nodes))
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
