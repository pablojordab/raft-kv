package test

import (
	"fmt"
	"testing"
	"time"

	raftrpc "github.com/pablojordab/raft-kv/rpc"
)

// Port ranges for the fault-tolerance tests (see cluster_test.go for the
// others). A node's port is basePort+id; a restarted node moves to
// basePort+50+id, so a restart never has to rebind a port the killed
// process just released (Windows can refuse that for a while).
const (
	followerCrashBasePort = 9400
	partitionBasePort     = 9500
	healBasePort          = 9600
	fiveNodeBasePort      = 9700
)

// proxiedCluster is a cluster whose nodes reach each other only through
// linkProxies (netproxy_test.go), one per direction per pair of nodes, so
// the test can partition the network between live processes. Clients and
// Status calls still go straight to each node.
type proxiedCluster struct {
	t        *testing.T
	bin      string
	basePort int
	nodes    []*testNode
	// links[from][to] carries every RPC that node `from` sends to `to`.
	links map[int]map[int]*linkProxy
	// peersFlag[id] is the -peers value node id was started with (its
	// outgoing links), reused if the node is restarted.
	peersFlag map[int]string
}

func startProxiedCluster(t *testing.T, bin string, size, basePort int) *proxiedCluster {
	t.Helper()
	c := &proxiedCluster{
		t: t, bin: bin, basePort: basePort,
		links:     map[int]map[int]*linkProxy{},
		peersFlag: map[int]string{},
	}
	realAddr := func(id int) string { return fmt.Sprintf("127.0.0.1:%d", basePort+id) }

	for from := 1; from <= size; from++ {
		c.links[from] = map[int]*linkProxy{}
		peers := ""
		for to := 1; to <= size; to++ {
			if to == from {
				continue
			}
			link := newLinkProxy(t, realAddr(to))
			c.links[from][to] = link
			if peers != "" {
				peers += ","
			}
			peers += fmt.Sprintf("%d=%s", to, link.addr())
		}
		c.peersFlag[from] = peers
	}

	for id := 1; id <= size; id++ {
		c.nodes = append(c.nodes, c.launch(id, realAddr(id), &syncBuffer{}))
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.kill()
		}
	})
	registerLogChecks(t, func() []*testNode { return c.nodes })
	return c
}

func (c *proxiedCluster) launch(id int, addr string, logs *syncBuffer) *testNode {
	c.t.Helper()
	cmd := startNodeProcess(c.t, c.bin, id, addr, c.peersFlag[id], logs)
	return &testNode{id: id, addr: addr, cmd: cmd, logs: logs}
}

func (c *proxiedCluster) node(id int) *testNode { return c.nodes[id-1] }

// restart starts a fresh process for a killed node, on a new port, and
// points every other node's link to it at the new address. The node comes
// back with an EMPTY log and term 0: persistence to disk is not
// implemented yet, so a restart is effectively a brand-new node rejoining
// the cluster, and it has to recover everything from the leader.
func (c *proxiedCluster) restart(id int) *testNode {
	c.t.Helper()
	if !c.node(id).dead {
		c.t.Fatalf("restart(%d): node is still running", id)
	}
	newAddr := fmt.Sprintf("127.0.0.1:%d", c.basePort+50+id)
	for from, links := range c.links {
		if from != id {
			links[id].setTarget(newAddr)
		}
	}
	// The restarted process appends to the same log buffer as the killed
	// one, so the node's history (and the failure timeline) covers both
	// lives, in order.
	n := c.launch(id, newAddr, c.node(id).logs)
	c.nodes[id-1] = n
	return n
}

// partition splits the cluster into groups: links between nodes in
// different groups are cut, links within a group keep working.
func (c *proxiedCluster) partition(groups ...[]int) {
	groupOf := map[int]int{}
	for g, ids := range groups {
		for _, id := range ids {
			groupOf[id] = g
		}
	}
	for from, links := range c.links {
		for to, link := range links {
			link.setBlocked(groupOf[from] != groupOf[to])
		}
	}
}

// heal restores every link.
func (c *proxiedCluster) heal() {
	for _, links := range c.links {
		for _, link := range links {
			link.setBlocked(false)
		}
	}
}

// subset returns the nodes with the given ids, in that order.
func subset(nodes []*testNode, ids ...int) []*testNode {
	out := make([]*testNode, 0, len(ids))
	for _, id := range ids {
		out = append(out, nodes[id-1])
	}
	return out
}

// others returns every node except the ones with the given ids.
func others(nodes []*testNode, exclude ...int) []*testNode {
	skip := map[int]bool{}
	for _, id := range exclude {
		skip[id] = true
	}
	var out []*testNode
	for _, n := range nodes {
		if !skip[n.id] {
			out = append(out, n)
		}
	}
	return out
}

func ids(nodes []*testNode) []int {
	out := make([]int, len(nodes))
	for i, n := range nodes {
		out[i] = n.id
	}
	return out
}

// submitTo sends one command to the cluster formed by `nodes`, following
// leader hints the same way cmd/client does, until a leader answers or the
// timeout expires. It returns the leader's reply (check Committed) and
// whether any leader answered at all. Clients talk to nodes directly, not
// through the link proxies, so a client can still reach every node during
// a partition, like a client outside the broken part of the network.
func submitTo(nodes []*testNode, command string, timeout time.Duration) (raftrpc.SubmitReply, bool) {
	byID := map[int]*testNode{}
	var live []*testNode
	for _, n := range nodes {
		if !n.dead {
			byID[n.id] = n
			live = append(live, n)
		}
	}
	if len(live) == 0 {
		return raftrpc.SubmitReply{}, false
	}

	deadline := time.Now().Add(timeout)
	pos := 0
	target := live[0]
	for time.Now().Before(deadline) {
		reply, err := raftrpc.SubmitCommand(target.addr, []byte(command))
		switch {
		case err == nil && reply.IsLeader:
			return reply, true
		case err == nil && byID[reply.LeaderID] != nil && reply.LeaderID != target.id:
			target = byID[reply.LeaderID] // follow the hint
			continue
		default:
			pos = (pos + 1) % len(live)
			target = live[pos]
			time.Sleep(retryPause)
		}
	}
	return raftrpc.SubmitReply{}, false
}

const retryPause = 100 * time.Millisecond

// mustCommit submits a command and fails the test unless it commits.
func mustCommit(t *testing.T, nodes []*testNode, command string) int {
	t.Helper()
	reply, ok := submitTo(nodes, command, 5*time.Second)
	if !ok || !reply.Committed {
		t.Fatalf("command %q was not committed (leader answered: %v, reply: %+v)", command, ok, reply)
	}
	return reply.Index
}

// waitForConvergence polls the given live nodes until every one has at
// least `wantCommit` committed entries AND all their logs are identical
// (same length, and same term, index and command at every position).
// Returns the final statuses.
func waitForConvergence(t *testing.T, nodes []*testNode, wantCommit int, timeout time.Duration) []raftrpc.StatusReply {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var statuses []raftrpc.StatusReply
	for {
		statuses = statuses[:0]
		done := true
		for _, n := range nodes {
			st, err := raftrpc.FetchStatus(n.addr)
			if err != nil {
				done = false
				continue
			}
			statuses = append(statuses, st)
			if st.CommitIndex < wantCommit || !sameLog(st, statuses[0]) {
				done = false
			}
		}
		if done && len(statuses) == len(nodes) {
			return statuses
		}
		if time.Now().After(deadline) {
			for _, st := range statuses {
				t.Logf("node %d: state=%s term=%d commit=%d log=%s", st.ID, st.State, st.Term, st.CommitIndex, describeLog(st))
			}
			t.Fatalf("nodes %v did not converge on %d identical committed entries within %v", ids(nodes), wantCommit, timeout)
		}
		time.Sleep(pollEvery)
	}
}

func sameLog(a, b raftrpc.StatusReply) bool {
	if len(a.Log) != len(b.Log) {
		return false
	}
	for i := range a.Log {
		x, y := a.Log[i], b.Log[i]
		if x.Term != y.Term || x.Index != y.Index || string(x.Command) != string(y.Command) {
			return false
		}
	}
	return true
}

// describeLog renders a log compactly for failure messages, e.g.
// "[1:t1:SET a 1, 2:t1:SET b 2]".
func describeLog(st raftrpc.StatusReply) string {
	out := "["
	for i, e := range st.Log {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%d:t%d:%s", e.Index, e.Term, e.Command)
	}
	return out + "]"
}

func fetchStatus(t *testing.T, n *testNode) raftrpc.StatusReply {
	t.Helper()
	st, err := raftrpc.FetchStatus(n.addr)
	if err != nil {
		t.Fatalf("Status RPC to node %d failed: %v", n.id, err)
	}
	return st
}
