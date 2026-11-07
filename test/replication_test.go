package test

import (
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"testing"
	"time"

	raftrpc "github.com/pablojordab/raft-kv/rpc"
)

// TestReplicationTenCommands sends 10 commands to a real 3-process cluster
// through the real CLI client, and verifies that all three nodes end up
// with exactly the same log: same length, and the same term, index and
// command at every position, in the order the commands were sent, all
// committed.
//
// Comparing contents matters: equal lengths alone would not show the logs
// agree. The contents are read from each node with the Status RPC, which
// returns a copy of that node's log.
func TestReplicationTenCommands(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping process-level replication test in -short mode")
	}

	nodeBin := buildNodeBinary(t)
	clientBin := buildClientBinary(t)
	nodes := startCluster(t, nodeBin, 3, replicationBasePort)

	leader, term := waitForStableLeader(t, nodes)
	t.Logf("leader: node %d in term %d", leader.id, term)

	// 1. Send 10 commands, one at a time, through the CLI. Each must be
	// confirmed as committed, at a later index than the one before it.
	// (Not necessarily index 1, 2, 3...: the leader's no-op entry from
	// winning its election comes first; see node/campaign.go.)
	const numCommands = 10
	sent := make([]string, numCommands)
	lastIndex := 0
	for i := 0; i < numCommands; i++ {
		key, value := fmt.Sprintf("key%d", i+1), fmt.Sprintf("value%d", i+1)
		sent[i] = "SET " + key + " " + value

		index, out := runClient(t, clientBin, clusterFlag(nodes), "SET", key, value)
		if index <= lastIndex {
			t.Fatalf("command %d committed at index %d, not after the previous one (%d)\nclient output:\n%s",
				i+1, index, lastIndex, out)
		}
		lastIndex = index
	}

	// 2. Followers learn the final commit index on the leader's next
	// round, so poll briefly until every node reports it committed.
	statuses := waitForAllCommitted(t, nodes, lastIndex)

	// 3. Every node's log must hold exactly the commands sent, in order,
	// at consistent positions, all from the leader's term (one leader
	// for the whole test, so its no-op is from that term too).
	requireCommands(t, statuses, sent)
	for _, st := range statuses {
		for i, entry := range st.Log {
			if entry.Index != i+1 {
				t.Errorf("node %d: entry %d has Index %d", st.ID, i+1, entry.Index)
			}
			if entry.Term != term {
				t.Errorf("node %d: entry %d has term %d, want leader's term %d", st.ID, i+1, entry.Term, term)
			}
		}
	}

	// 4. And all three logs must be identical to each other (implied by
	// step 3, but checked directly since it's the property under test).
	reference := statuses[0]
	for _, st := range statuses[1:] {
		for i := range reference.Log {
			a, b := reference.Log[i], st.Log[i]
			if a.Term != b.Term || a.Index != b.Index || string(a.Command) != string(b.Command) {
				t.Fatalf("logs differ at index %d: node %d has %+v, node %d has %+v",
					i+1, reference.ID, a, st.ID, b)
			}
		}
	}
	t.Logf("all 3 nodes hold the same %d committed entries", numCommands)
}

// clientOK matches the client's success line, e.g.
// "OK: committed at index 3 (term 1) by leader node 2".
var clientOK = regexp.MustCompile(`OK: committed at index (\d+) \(term \d+\)`)

// runClient runs the CLI client once and returns the index it reports the
// command was committed at. Fails the test unless the client reported OK.
func runClient(t *testing.T, bin, cluster string, args ...string) (int, string) {
	t.Helper()
	cmd := exec.Command(bin, append([]string{"-cluster=" + cluster}, args...)...)
	out, err := cmd.CombinedOutput()
	m := clientOK.FindSubmatch(out)
	if err != nil || m == nil {
		t.Fatalf("client %v did not report a commit (err=%v):\n%s", args, err, out)
	}
	index, _ := strconv.Atoi(string(m[1]))
	return index, string(out)
}

// waitForAllCommitted polls every node's Status until all of them report
// at least `want` committed entries, and returns those statuses.
func waitForAllCommitted(t *testing.T, nodes []*testNode, want int) []raftrpc.StatusReply {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		statuses := make([]raftrpc.StatusReply, 0, len(nodes))
		allDone := true
		for _, n := range nodes {
			st, err := raftrpc.FetchStatus(n.addr)
			if err != nil {
				t.Fatalf("Status RPC to node %d failed: %v", n.id, err)
			}
			statuses = append(statuses, st)
			if st.CommitIndex < want {
				allDone = false
			}
		}
		if allDone {
			return statuses
		}
		if time.Now().After(deadline) {
			for _, st := range statuses {
				t.Logf("node %d: state=%s commit=%d log=%d", st.ID, st.State, st.CommitIndex, len(st.Log))
			}
			t.Fatalf("not every node committed %d entries within 2s", want)
		}
		time.Sleep(pollEvery)
	}
}
