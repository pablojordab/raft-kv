package test

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// Nodes in these tests log in JSON (see startNodeProcess), one event per
// line, each with a timestamp, a level, a message and named fields such as
// node, term and state. This file turns those lines back into events, so
// the tests can:
//
//   - read a node's latest status without regex-matching text,
//   - merge the logs of every node process into ONE time-ordered timeline
//     and print it when a test fails (much easier to debug a chaos test
//     from than five separate logs), and
//   - check a Raft safety property over the whole run, from the logs
//     alone, in every process-level test.

// logEvent is one parsed log line.
type logEvent struct {
	Time   time.Time
	Level  string
	Msg    string
	Node   int
	Fields map[string]any // every field, including the ones above
}

func (e logEvent) str(key string) string {
	if v, ok := e.Fields[key].(string); ok {
		return v
	}
	return ""
}

// int reads a numeric field (encoding/json decodes every number as a
// float64), or -1 if the field is missing or not a number.
func (e logEvent) int(key string) int {
	if v, ok := e.Fields[key].(float64); ok {
		return int(v)
	}
	return -1
}

// parseEvents decodes every JSON line in a node's output. Lines that
// aren't JSON (there shouldn't be any, but a crashing process could print
// a Go panic trace) are skipped rather than failing the parse.
func parseEvents(output string) []logEvent {
	var events []logEvent
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			continue
		}
		e := logEvent{Fields: fields}
		e.Level = e.str("level")
		e.Msg = e.str("msg")
		e.Node = e.int("node")
		e.Time, _ = time.Parse(time.RFC3339Nano, e.str("time"))
		events = append(events, e)
	}
	return events
}

// allEvents merges every node's events into one list sorted by time. All
// node processes run on the same machine, so their clocks agree and the
// merged order is the real order (to within the clock's resolution).
func allEvents(nodes []*testNode) []logEvent {
	var events []logEvent
	for _, n := range nodes {
		events = append(events, parseEvents(n.logs.String())...)
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].Time.Before(events[j].Time) })
	return events
}

// timeline renders the merged events as one line each, e.g.
//
//	19:45:20.225 node=1 INFO  became leader  term=1 lastLogIndex=0
//
// The once-a-second "status" events are left out: they're noise when
// reading what happened. maxEvents keeps only the most recent ones.
func timeline(nodes []*testNode, maxEvents int) string {
	var lines []string
	for _, e := range allEvents(nodes) {
		if e.Msg == "status" {
			continue
		}
		var extra []string
		for k, v := range e.Fields {
			switch k {
			case "time", "level", "msg", "node":
				continue
			}
			extra = append(extra, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(extra) // map order is random; keep lines stable
		lines = append(lines, fmt.Sprintf("%s node=%d %-5s %-24s %s",
			e.Time.Format("15:04:05.000"), e.Node, e.Level, e.Msg, strings.Join(extra, " ")))
	}
	if len(lines) > maxEvents {
		lines = append([]string{fmt.Sprintf("... (%d earlier events omitted)", len(lines)-maxEvents)},
			lines[len(lines)-maxEvents:]...)
	}
	return strings.Join(lines, "\n")
}

// electionSafetyViolations checks the Raft paper's Election Safety property
// (Figure 3): "at most one leader can be elected in a given term". It looks
// at every "became leader" event from every node and reports any term in
// which two different nodes claim to have won.
func electionSafetyViolations(nodes []*testNode) []string {
	leaderOf := map[int]int{} // term -> node that won it
	var violations []string
	for _, e := range allEvents(nodes) {
		if e.Msg != "became leader" {
			continue
		}
		term := e.int("term")
		if prev, seen := leaderOf[term]; seen && prev != e.Node {
			violations = append(violations, fmt.Sprintf(
				"term %d has two leaders: node %d and node %d", term, prev, e.Node))
			continue
		}
		leaderOf[term] = e.Node
	}
	return violations
}

// registerLogChecks runs at the end of every process-level test (it's
// registered by startCluster and startProxiedCluster):
//   - it fails the test if the logs show two leaders in the same term,
//     whatever the test itself was checking, and
//   - if the test failed for any reason, it prints the merged timeline of
//     every node, so the failure can be debugged from the test output.
//
// nodes is a function, not a slice, because a proxied cluster replaces
// entries when it restarts a node; the check must see the final set.
func registerLogChecks(t *testing.T, nodes func() []*testNode) {
	t.Cleanup(func() {
		for _, v := range electionSafetyViolations(nodes()) {
			t.Errorf("Election Safety violated: %s", v)
		}
		if t.Failed() {
			t.Logf("merged timeline of all nodes (most recent last):\n%s", timeline(nodes(), 200))
		}
	})
}

// fakeNode builds a testNode whose "log output" is the given JSON lines,
// to test the log helpers without starting any process.
func fakeNode(id int, lines ...string) *testNode {
	buf := &syncBuffer{}
	for _, l := range lines {
		buf.Write([]byte(l + "\n"))
	}
	return &testNode{id: id, logs: buf}
}

func TestElectionSafetyCheckerFlagsTwoLeadersInOneTerm(t *testing.T) {
	ok := []*testNode{
		fakeNode(1, `{"time":"2026-10-23T10:00:00.1Z","level":"INFO","msg":"became leader","node":1,"term":1}`),
		fakeNode(2, `{"time":"2026-10-23T10:00:01.1Z","level":"INFO","msg":"became leader","node":2,"term":2}`),
	}
	if v := electionSafetyViolations(ok); len(v) != 0 {
		t.Fatalf("one leader per term was flagged: %v", v)
	}

	bad := append(ok, fakeNode(3,
		`not json: a stray line must be ignored`,
		`{"time":"2026-10-23T10:00:01.2Z","level":"INFO","msg":"became leader","node":3,"term":2}`))
	v := electionSafetyViolations(bad)
	if len(v) != 1 || !strings.Contains(v[0], "term 2") {
		t.Fatalf("expected one violation for term 2, got %v", v)
	}
}

func TestTimelineMergesNodesInTimeOrderAndSkipsStatus(t *testing.T) {
	nodes := []*testNode{
		fakeNode(1,
			`{"time":"2026-10-23T10:00:00.300Z","level":"INFO","msg":"became leader","node":1,"term":1}`,
			`{"time":"2026-10-23T10:00:00.400Z","level":"INFO","msg":"status","node":1,"state":"Leader","term":1}`),
		fakeNode(2,
			`{"time":"2026-10-23T10:00:00.100Z","level":"DEBUG","msg":"granted vote","node":2,"term":1,"candidate":1}`),
	}
	got := strings.Split(timeline(nodes, 10), "\n")
	if len(got) != 2 {
		t.Fatalf("want 2 lines (status skipped), got %d:\n%s", len(got), strings.Join(got, "\n"))
	}
	if !strings.Contains(got[0], "granted vote") || !strings.Contains(got[1], "became leader") {
		t.Fatalf("events not in time order across nodes:\n%s", strings.Join(got, "\n"))
	}
	if !strings.Contains(got[0], "candidate=1 term=1") {
		t.Fatalf("fields missing or unsorted: %q", got[0])
	}
}
