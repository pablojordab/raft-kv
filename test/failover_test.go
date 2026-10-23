package test

import "testing"

// TestLeaderFailover kills the leader of a real 3-process cluster and
// verifies the two survivors elect a new leader among themselves, in a
// strictly higher term. Shared helpers live in cluster_test.go.
func TestLeaderFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping process-level failover test in -short mode")
	}

	bin := buildNodeBinary(t)
	nodes := startCluster(t, bin, 3, failoverBasePort)

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
