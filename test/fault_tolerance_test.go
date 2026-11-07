package test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	raftrpc "github.com/pablojordab/raft-kv/rpc"
)

// Fault-tolerance tests on real multi-process clusters. Crashes are real
// process kills; partitions are real cut TCP links (netproxy_test.go).
// Harness and helpers live in faults_helpers_test.go.

// TestFollowerCrashAndRecovery: a follower crashes in the middle of a
// stream of writes and comes back later. While it's down, the other two
// nodes are still a majority, so every write must keep committing. Once it
// returns, it must catch up to exactly the same log as everyone else.
//
// This cluster deliberately runs WITHOUT -data-dir, so the follower
// restarts with an empty log, like a replacement machine with a blank
// disk. "Catching up" then means receiving the whole log again through
// the leader's conflict backtracking (heartbeat.go). Recovery from the
// node's own disk is covered by persistence_test.go.
func TestFollowerCrashAndRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level test")
	}
	c := startProxiedCluster(t, buildNodeBinary(t), 3, followerCrashBasePort)
	leader, _ := waitForStableLeader(t, c.nodes)
	follower := others(c.nodes, leader.id)[0]
	t.Logf("leader: node %d; follower to crash: node %d", leader.id, follower.id)

	// The writer only ever talks to the two nodes that stay up, so it never
	// touches the crashing node's state while the test kills and replaces
	// it (that would be a data race in the test itself).
	writeTo := others(c.nodes, follower.id)

	const total = 20
	committed := make(chan int, total)
	var writerErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= total; i++ {
			reply, ok := submitTo(writeTo, fmt.Sprintf("SET k%d v%d", i, i), 5*time.Second)
			if !ok || !reply.Committed {
				writerErr = fmt.Errorf("write %d not committed: %+v", i, reply)
				close(committed)
				return
			}
			committed <- reply.Index
		}
		close(committed)
	}()

	// Crash the follower after 5 writes, bring it back after 12.
	count, lastIndex := 0, 0
	for index := range committed {
		count++
		lastIndex = index
		switch count {
		case 5:
			follower.kill()
			t.Logf("killed follower node %d after %d committed writes", follower.id, count)
		case 12:
			down := fetchStatus(t, leader)
			if down.CommitIndex < lastIndex {
				t.Fatalf("writes should have kept committing without the follower; leader commit=%d, want >= %d", down.CommitIndex, lastIndex)
			}
			follower = c.restart(follower.id)
			t.Logf("restarted node %d (empty log) after %d committed writes", follower.id, count)
		}
	}
	wg.Wait()
	if writerErr != nil {
		t.Fatal(writerErr)
	}

	statuses := waitForConvergence(t, c.nodes, lastIndex, 5*time.Second)
	var want []string
	for i := 1; i <= total; i++ {
		want = append(want, fmt.Sprintf("SET k%d v%d", i, i))
	}
	requireCommands(t, statuses, want)
	t.Logf("restarted follower caught up: all 3 nodes hold the same %d committed entries", total)
}

// partitionLeaderAway runs the shared first half of the partition tests:
// commit two writes on a healthy 3-node cluster, then cut the leader off
// from the other two. Returns the old leader, its term, the two nodes on
// the majority side, and the log index of the last pre-partition write.
func partitionLeaderAway(t *testing.T, c *proxiedCluster) (*testNode, int, []*testNode, int) {
	t.Helper()
	oldLeader, oldTerm := waitForStableLeader(t, c.nodes)
	mustCommit(t, c.nodes, "SET before1 x")
	beforeIndex := mustCommit(t, c.nodes, "SET before2 x")
	waitForConvergence(t, c.nodes, beforeIndex, 3*time.Second)

	majority := others(c.nodes, oldLeader.id)
	c.partition([]int{oldLeader.id}, ids(majority))
	t.Logf("partitioned leader node %d (term %d) away from nodes %v", oldLeader.id, oldTerm, ids(majority))
	return oldLeader, oldTerm, majority, beforeIndex
}

// TestPartitionedLeaderReplaced: the leader is cut off from the other two
// nodes. The two of them are still a majority, so they must elect a new
// leader among themselves, in a higher term, and keep accepting writes.
// The isolated old leader must not be able to commit anything.
func TestPartitionedLeaderReplaced(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level test")
	}
	c := startProxiedCluster(t, buildNodeBinary(t), 3, partitionBasePort)
	oldLeader, oldTerm, majority, beforeIndex := partitionLeaderAway(t, c)

	newLeader, newTerm := waitForStableLeader(t, majority)
	t.Logf("majority side elected node %d in term %d", newLeader.id, newTerm)
	if newTerm <= oldTerm {
		t.Fatalf("new leader's term %d should be above the old term %d", newTerm, oldTerm)
	}

	for i := 1; i <= 3; i++ {
		mustCommit(t, majority, fmt.Sprintf("SET during%d x", i))
	}

	// The old leader is still running, and in basic Raft it doesn't know
	// it was deposed: nothing reaches it to tell it. It still accepts the
	// write, but with no majority it can never commit it.
	reply, err := raftrpc.SubmitCommand(oldLeader.addr, []byte("SET stale x"))
	if err != nil {
		t.Fatalf("submitting to the isolated old leader: %v", err)
	}
	if reply.IsLeader && reply.Committed {
		t.Fatal("the isolated old leader committed a write without a majority")
	}
	st := fetchStatus(t, oldLeader)
	t.Logf("isolated old leader: state=%s term=%d commit=%d (could not commit, as required)", st.State, st.Term, st.CommitIndex)
	if st.CommitIndex != beforeIndex {
		t.Fatalf("isolated old leader's commit index moved to %d; only the pre-partition writes (up to index %d) may be committed",
			st.CommitIndex, beforeIndex)
	}
}

// TestPartitionHealOldLeaderStepsDown continues the previous scenario: once
// the partition heals, the old leader must learn of the newer term, step
// down to follower, and throw away the write it accepted while isolated
// (it was never committed). Every node must end up with the same log:
// the pre-partition writes plus the ones the majority committed.
func TestPartitionHealOldLeaderStepsDown(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level test")
	}
	c := startProxiedCluster(t, buildNodeBinary(t), 3, healBasePort)
	oldLeader, oldTerm, majority, _ := partitionLeaderAway(t, c)

	// While isolated, the old leader stores a write it can't commit.
	stale := make(chan raftrpc.SubmitReply, 1)
	go func() {
		reply, _ := raftrpc.SubmitCommand(oldLeader.addr, []byte("SET stale x"))
		stale <- reply
	}()

	newLeader, newTerm := waitForStableLeader(t, majority)
	lastIndex := 0
	for i := 1; i <= 3; i++ {
		lastIndex = mustCommit(t, majority, fmt.Sprintf("SET during%d x", i))
	}
	if r := <-stale; r.Committed {
		t.Fatal("the isolated old leader reported its write as committed")
	}
	if st := fetchStatus(t, oldLeader); !strings.Contains(describeLog(st), "SET stale x") {
		t.Fatalf("expected the isolated old leader to hold the uncommitted stale write, log=%s", describeLog(st))
	}

	c.heal()
	t.Logf("healed the partition (new leader: node %d, term %d)", newLeader.id, newTerm)

	statuses := waitForConvergence(t, c.nodes, lastIndex, 5*time.Second)
	// The stale write must be gone everywhere; only these remain.
	requireCommands(t, statuses, []string{"SET before1 x", "SET before2 x", "SET during1 x", "SET during2 x", "SET during3 x"})
	for _, st := range statuses {
		if st.ID == oldLeader.id {
			if st.State != "Follower" || st.Term <= oldTerm {
				t.Fatalf("old leader should have stepped down: state=%s term=%d", st.State, st.Term)
			}
		}
	}
	t.Logf("old leader node %d stepped down and dropped its uncommitted write; all logs identical", oldLeader.id)
}

// TestFiveNodesSurviveTwoFailures: a 5-node cluster tolerates any 2
// failures (3 of 5 is still a majority). Killing the leader and one more
// node must leave a cluster that elects a new leader and keeps committing
// writes. Killing a third node takes it below a majority, and then no
// write may commit.
func TestFiveNodesSurviveTwoFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("process-level test")
	}
	nodes := startCluster(t, buildNodeBinary(t), 5, fiveNodeBasePort)
	leader, term := waitForStableLeader(t, nodes)
	for i := 1; i <= 3; i++ {
		mustCommit(t, nodes, fmt.Sprintf("SET before%d x", i))
	}

	victim := others(nodes, leader.id)[0]
	leader.kill()
	victim.kill()
	t.Logf("killed leader node %d (term %d) and node %d", leader.id, term, victim.id)

	survivors := others(nodes, leader.id, victim.id)
	newLeader, newTerm := waitForStableLeader(t, survivors)
	t.Logf("3 survivors %v elected node %d in term %d", ids(survivors), newLeader.id, newTerm)

	lastIndex := 0
	for i := 1; i <= 5; i++ {
		lastIndex = mustCommit(t, survivors, fmt.Sprintf("SET after%d x", i))
	}
	waitForConvergence(t, survivors, lastIndex, 5*time.Second)
	t.Logf("3 of 5 nodes kept serving writes: all survivors hold the same 8 committed entries")

	// A third failure leaves 2 of 5: no majority, so nothing may commit.
	third := others(survivors, newLeader.id)[0]
	third.kill()
	if reply, ok := submitTo(nodes, "SET nomajority x", 4*time.Second); ok && reply.Committed {
		t.Fatalf("a write committed with only 2 of 5 nodes alive: %+v", reply)
	}
	t.Logf("with 2 of 5 nodes alive, the write was correctly not committed")
}
