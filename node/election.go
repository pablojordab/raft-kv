package node

import (
	"context"
	"log"
	"math/rand"
	"time"
)

// electionTimeoutMin and electionTimeoutMax bound the randomized election
// timeout, per the Raft paper's recommended range (section 5.6): broadcast
// time << election timeout << mean time between failures. 150-300ms is
// the range the original paper benchmarks against; it's generous for a
// single-machine, localhost-RPC setup like this project's, where
// broadcast time is sub-millisecond.
//
// Randomizing within this range (rather than using one fixed timeout for
// every node) is what makes split votes rare: if all followers timed out
// at exactly the same instant, they'd all become candidates simultaneously
// every single election, and no candidate would ever gather a majority.
const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
)

// electionTimerPollInterval is how often RunElectionTimer wakes up to
// check whether the timeout has elapsed. It's unrelated to the election
// timeout itself — it just needs to be short enough that the timer's
// actual firing time is close to the randomized deadline, without busy
// looping. 10ms gives sub-10ms precision on a 150-300ms timeout, which is
// more than tight enough here.
const electionTimerPollInterval = 10 * time.Millisecond

// randomElectionTimeout returns a random duration in
// [electionTimeoutMin, electionTimeoutMax), using the package-level math/rand
// source. Not cryptographically random, and doesn't need to be — this is
// scheduling jitter, not a security boundary.
func randomElectionTimeout() time.Duration {
	span := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

// resetElectionTimer records "now" as the last time this node had a
// reason to NOT start an election. Call this whenever:
//   - this node grants a vote to a candidate (election.go's future
//     RequestVote handler, task 6), or
//   - this node receives a valid AppendEntries from the current leader
//     (log.go's future handler, task 13).
//
// Must be called with n.mu held, since it's always called from code paths
// that are already holding the lock to validate the RPC that triggered
// the reset.
func (n *Node) resetElectionTimerLocked() {
	n.electionResetEvent = time.Now()
}

// RunElectionTimer is the goroutine that watches for this node's election
// timeout. It's started once per term (see becomeCandidateLocked below,
// which starts a fresh one for the new term whenever a node times out) and
// exits on its own once that term is no longer current for this node —
// either because this node became a candidate for a NEWER term, or
// because it became (or heard from) a Leader.
//
// This is deliberately NOT built on time.Timer.Reset. Reset has a
// well-documented race: if a Timer has already fired and its value hasn't
// been drained from its channel, a concurrent Reset can race with a
// receive on that channel, and the two standard fixes (draining
// unconditionally, or draining in a select with a default) both have edge
// cases that are easy to get subtly wrong under concurrent access. Instead,
// this follows the pattern used by several reference Raft implementations
// (e.g. Eli Bendersky's raft toy implementation, and the approach
// recommended in MIT 6.824's "Students' Guide to Raft"): a single
// long-lived goroutine wakes up periodically, and compares time.Since(the
// last reset event) against a timeout chosen once when the goroutine
// started. Resetting the timer is then just writing a timestamp under the
// lock — no channels, no race.
func (n *Node) RunElectionTimer(ctx context.Context) {
	timeout := randomElectionTimeout()

	n.mu.Lock()
	termStarted := n.persistent.CurrentTerm
	n.mu.Unlock()

	log.Printf("[node %d] election timer started for term %d (timeout %v)", n.id, termStarted, timeout)

	ticker := time.NewTicker(electionTimerPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		n.mu.Lock()

		// This goroutine was started for termStarted specifically. If
		// the node's term has since moved on — e.g. because this very
		// timeout already fired once and spawned a newer election
		// timer for the next term — this instance's job is done. It
		// exits rather than firing a second, stale election.
		if n.persistent.CurrentTerm != termStarted {
			n.mu.Unlock()
			return
		}

		// Leaders don't run an election timer — they're the ones
		// sending heartbeats, not waiting for them (task 8 will add
		// the heartbeat loop). If this node won the election that
		// started under termStarted, this timer has nothing left to
		// do.
		if n.state == Leader {
			n.mu.Unlock()
			return
		}

		if elapsed := time.Since(n.electionResetEvent); elapsed >= timeout {
			n.becomeCandidateLocked(ctx)
			n.mu.Unlock()
			return
		}

		n.mu.Unlock()
	}
}

// becomeCandidateLocked performs the state transition the Raft paper
// describes at the start of section 5.2: on election timeout, a node
// increments its term, transitions to Candidate, and votes for itself.
// Must be called with n.mu held.
//
// NOTE: this does not yet send RequestVote RPCs to the other peers — that
// campaign logic (contacting every peer, counting votes, becoming Leader
// on a majority) is task 7, not this one. What's here is the minimal,
// self-contained state change the Raft paper ties directly to the timeout
// firing; task 7 will extend this method to actually run the election.
//
// It does spawn the NEXT election timer, for the new term — a candidate
// still needs a timeout in case this election is inconclusive (a split
// vote), per the paper's retry-with-new-randomized-timeout rule.
func (n *Node) becomeCandidateLocked(ctx context.Context) {
	n.persistent.CurrentTerm++
	n.state = Candidate
	n.persistent.VotedFor = n.id // a candidate always votes for itself
	n.electionResetEvent = time.Now()

	log.Printf("[node %d] election timeout: becoming Candidate for term %d", n.id, n.persistent.CurrentTerm)

	// TODO (task 7): request votes from every peer here, count the
	// results, and transition to Leader on a majority.

	go n.RunElectionTimer(ctx)
}