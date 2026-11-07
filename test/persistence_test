package test

import (
	"fmt"
	"testing"
	"time"

	raftrpc "github.com/pablojordab/raft-kv/rpc"
)

// Persistence tests on real multi-process clusters whose nodes save their
// state to disk (-data-dir). Harness and helpers live in
// faults_helpers_test.go.

// TestRestartedNodeRecoversItsStateFromDisk kills a follower and restarts
// it, and checks it comes back with exactly the log, term and vote it had.
//
// The important detail is HOW it's restarted: cut off from the rest of the
// cluster by the link proxies. Restarting it connected would prove
// nothing, because the leader would simply send it the whole log again
// (which is what an in-memory node relies on, see
// TestFollowerCrashAndRecovery). Isolated, nobody can send it anything, so
// if it has its log, the log came from its own disk.
func TestRestartedNodeRecoversItsStateFromDisk(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level test")
	}
	c := startPersistentProxiedCluster(t, buildNodeBinary(t), 3, restartBasePort)
	leader, _ := waitForStableLeader(t, c.nodes)
	for i := 1; i <= 5; i++ {
		mustCommit(t, c.nodes, fmt.Sprintf("SET k%d v%d", i, i))
	}
	waitForConvergence(t, c.nodes, 5, 3*time.Second)

	follower := others(c.nodes, leader.id)[0]
	before := fetchStatus(t, follower)
	t.Logf("node %d before the crash: term=%d votedFor=%d log=%s",
		follower.id, before.Term, before.VotedFor, describeLog(before))

	follower.kill()
	c.partition([]int{follower.id}, ids(others(c.nodes, follower.id)))
	follower = c.restart(follower.id)
	t.Logf("restarted node %d isolated from the rest of the cluster", follower.id)

	// 1. What it loaded at startup, from its "restored state" log event:
	// logged before the node does anything else, so these are exactly the
	// values read from disk.
	restored := waitForEvent(t, follower, "restored state", 3*time.Second)
	if got := restored.int("term"); got != before.Term {
		t.Errorf("restored term = %d, want %d", got, before.Term)
	}
	if got := restored.int("votedFor"); got != before.VotedFor {
		t.Errorf("restored votedFor = %d, want %d", got, before.VotedFor)
	}
	if got := restored.int("logLength"); got != len(before.Log) {
		t.Errorf("restored log length = %d, want %d", got, len(before.Log))
	}

	// 2. Its log, read over RPC while still isolated: identical, entry
	// by entry. (Its term may have grown by now: alone, it times out and
	// campaigns. A candidate never changes its own log, though.)
	isolated := waitForStatus(t, follower, 3*time.Second)
	if !sameLog(isolated, before) {
		t.Fatalf("isolated restarted node's log = %s, want %s", describeLog(isolated), describeLog(before))
	}
	t.Logf("while still isolated, node %d holds the same %d entries: they came from its disk", follower.id, len(isolated.Log))

	// 3. Back in the cluster, it keeps working normally.
	c.heal()
	mustCommit(t, c.nodes, "SET after restart x")
	waitForConvergence(t, c.nodes, 6, 5*time.Second)
}

// TestWholeClusterRestartKeepsCommittedWrites kills every node at once, like
// a power cut in a whole datacenter, and restarts them all. Committed
// writes must survive: the cluster must come back with all of them, in
// order, and keep going from there, so the next write lands after them
// instead of overwriting them.
func TestWholeClusterRestartKeepsCommittedWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level test")
	}
	c := startPersistentProxiedCluster(t, buildNodeBinary(t), 3, fullRestartBasePort)
	waitForStableLeader(t, c.nodes)

	var want []string
	for i := 1; i <= 5; i++ {
		cmd := fmt.Sprintf("SET k%d v%d", i, i)
		mustCommit(t, c.nodes, cmd)
		want = append(want, cmd)
	}
	waitForConvergence(t, c.nodes, 5, 3*time.Second)

	for _, n := range c.nodes {
		n.kill()
	}
	t.Logf("killed all 3 nodes")
	for id := 1; id <= 3; id++ {
		c.restart(id)
	}
	t.Logf("restarted all 3 nodes from their data directories")

	newLeader, newTerm := waitForStableLeader(t, c.nodes)
	t.Logf("node %d elected in term %d after the full restart", newLeader.id, newTerm)

	index := mustCommit(t, c.nodes, "SET after outage x")
	if index != 6 {
		t.Fatalf("first write after the restart went to index %d, want 6 (right after the 5 saved ones)", index)
	}
	want = append(want, "SET after outage x")

	statuses := waitForConvergence(t, c.nodes, 6, 5*time.Second)
	for _, st := range statuses {
		for i, e := range st.Log {
			if string(e.Command) != want[i] {
				t.Fatalf("node %d entry %d = %q, want %q (log %s)", st.ID, i+1, e.Command, want[i], describeLog(st))
			}
		}
	}
	t.Logf("all 5 writes committed before the outage survived, followed by the new one")
}

// waitForEvent waits until the node has logged an event with the given
// message, and returns its most recent occurrence.
func waitForEvent(t *testing.T, n *testNode, msg string, timeout time.Duration) logEvent {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		events := parseEvents(n.logs.String())
		for i := len(events) - 1; i >= 0; i-- {
			if events[i].Msg == msg {
				return events[i]
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d never logged %q", n.id, msg)
		}
		time.Sleep(pollEvery)
	}
}

// waitForStatus polls a node's Status RPC until it answers, e.g. right
// after the process was started.
func waitForStatus(t *testing.T, n *testNode, timeout time.Duration) raftrpc.StatusReply {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		st, err := raftrpc.FetchStatus(n.addr)
		if err == nil {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d did not answer Status within %v: %v", n.id, timeout, err)
		}
		time.Sleep(pollEvery)
	}
}