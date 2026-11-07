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
	lastIndex := 0
	for i := 1; i <= 5; i++ {
		lastIndex = mustCommit(t, c.nodes, fmt.Sprintf("SET k%d v%d", i, i))
	}
	waitForConvergence(t, c.nodes, lastIndex, 3*time.Second)

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
	lastIndex = mustCommit(t, c.nodes, "SET after restart x")
	waitForConvergence(t, c.nodes, lastIndex, 5*time.Second)
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
	lastBefore := 0
	for i := 1; i <= 5; i++ {
		cmd := fmt.Sprintf("SET k%d v%d", i, i)
		lastBefore = mustCommit(t, c.nodes, cmd)
		want = append(want, cmd)
	}
	waitForConvergence(t, c.nodes, lastBefore, 3*time.Second)

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
	if index <= lastBefore {
		t.Fatalf("first write after the restart went to index %d, on top of the saved writes (which end at %d)", index, lastBefore)
	}
	want = append(want, "SET after outage x")

	statuses := waitForConvergence(t, c.nodes, index, 5*time.Second)
	requireCommands(t, statuses, want)
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

// TestSlowDiskCausesNoSpuriousElections reproduces, on any machine, the
// failure first seen on Windows: there a save (an fsync) can take hundreds
// of milliseconds, and a slow save made followers think the leader had
// died, start elections in the middle of a write, and leave that write
// stuck until the client gave up.
//
// Every save here takes 150ms longer than normal (-test-save-delay). With a
// healthy leader and no failures, the cluster must commit every write, end
// up with identical logs, and hold NO election after the first leader is
// in place: a slow disk is not a dead leader.
func TestSlowDiskCausesNoSpuriousElections(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level test")
	}
	c := startPersistentProxiedCluster(t, buildNodeBinary(t), 3, slowDiskBasePort, "-test-save-delay=150ms")
	leader, term := waitForStableLeader(t, c.nodes)
	stableSince := time.Now()
	t.Logf("leader: node %d in term %d; every save now takes 150ms extra", leader.id, term)

	var want []string
	lastIndex := 0
	for i := 1; i <= 10; i++ {
		cmd := fmt.Sprintf("SET k%d v%d", i, i)
		lastIndex = mustCommit(t, c.nodes, cmd)
		want = append(want, cmd)
	}
	statuses := waitForConvergence(t, c.nodes, lastIndex, 5*time.Second)
	requireCommands(t, statuses, want)

	var spurious []string
	for _, e := range allEvents(c.nodes) {
		if e.Time.After(stableSince) && e.Msg == "became candidate" {
			spurious = append(spurious, fmt.Sprintf("node %d at %s (term %d)", e.Node, e.Time.Format("15:04:05.000"), e.int("term")))
		}
	}
	if len(spurious) > 0 {
		t.Fatalf("slow saves caused %d spurious election(s) with a healthy leader: %v", len(spurious), spurious)
	}
	if st := fetchStatus(t, leader); st.State != "Leader" || st.Term != term {
		t.Fatalf("leader changed: node %d is now %s in term %d", leader.id, st.State, st.Term)
	}
	t.Logf("10 writes committed with a slow disk, no spurious elections, leader unchanged")
}
