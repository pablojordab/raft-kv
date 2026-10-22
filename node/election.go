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

// RunElectionTimer is the single, long-lived goroutine that watches for
// this node's election timeout, for as long as the node lives — it's
// meant to be started exactly ONCE per process (see cmd/main.go, task 9),
// not restarted per term.
//
// An earlier version of this method exited and relied on becomeCandidateLocked
// to spawn a replacement for the new term. That had a real bug, caught
// only by running a 3-process cluster end-to-end (task 9's verification):
// a Follower's term also advances when it simply GRANTS a vote to a
// candidate (vote.go's HandleRequestVote) or hears a valid heartbeat from
// a leader (task 13) — neither of those paths spawns a new timer. A node
// that only ever advanced its term that way, and never itself became a
// Candidate, was left with NO election timer running at all: it would sit
// as a Follower forever, even after the leader it last heard from had been
// dead for a long time, because nothing was left watching the clock. This
// version fixes that by never exiting except on shutdown: whenever it
// notices the term moved on for ANY reason, it simply adopts a fresh
// randomized timeout for the new term and keeps polling, so exactly one
// timer is always watching, regardless of how a node's term got updated.
//
// This is deliberately NOT built on time.Timer.Reset. Reset has a
// well-documented race: if a Timer has already fired and its value hasn't
// been drained from its channel, a concurrent Reset can race with a
// receive on that channel, and the two standard fixes (draining
// unconditionally, or draining in a select with a default) both have edge
// cases that are easy to get subtly wrong under concurrent access. Instead,
// this follows the pattern used by several reference Raft implementations
// (e.g. Eli Bendersky's raft toy implementation, and the approach
// recommended in MIT 6.824's "Students' Guide to Raft"): a long-lived
// goroutine wakes up periodically, and compares time.Since(the last reset
// event) against a timeout chosen when it last noticed the term change.
// Resetting the timer is then just writing a timestamp under the lock —
// no channels, no race.
func (n *Node) RunElectionTimer(ctx context.Context) {
	n.mu.Lock()
	timeout := randomElectionTimeout()
	termWatched := n.persistent.CurrentTerm
	n.mu.Unlock()

	log.Printf("[node %d] election timer started for term %d (timeout %v)", n.id, termWatched, timeout)

	ticker := time.NewTicker(electionTimerPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		n.mu.Lock()

		// The term has moved on since we last picked a timeout — for
		// ANY reason: this node started a new election itself, granted
		// a vote to someone else, heard from a new leader, or stepped
		// down after losing an election. Whatever the cause, the
		// timeout we were tracking belongs to a stale term. Adopt a
		// fresh one for the new term and keep polling, rather than
		// exiting — this IS the only timer this node will ever get, so
		// nothing else is going to restart it.
		if n.persistent.CurrentTerm != termWatched {
			timeout = randomElectionTimeout()
			termWatched = n.persistent.CurrentTerm
			log.Printf("[node %d] election timer resynced for term %d (timeout %v)", n.id, termWatched, timeout)
			n.mu.Unlock()
			continue
		}

		// Leaders don't start elections — they're the ones sending
		// heartbeats, not waiting for them (heartbeat.go). Keep
		// polling rather than exiting: if this node later steps down
		// to Follower, this same goroutine needs to still be here to
		// notice it.
		if n.state == Leader {
			n.mu.Unlock()
			continue
		}

		if elapsed := time.Since(n.electionResetEvent); elapsed >= timeout {
			n.becomeCandidateLocked(ctx)
			// becomeCandidateLocked just incremented CurrentTerm; adopt
			// the new term's timeout immediately instead of waiting for
			// the next tick to notice the mismatch above.
			timeout = randomElectionTimeout()
			termWatched = n.persistent.CurrentTerm
		}

		n.mu.Unlock()
	}
}

// becomeCandidateLocked performs the state transition the Raft paper
// describes at the start of section 5.2: on election timeout, a node
// increments its term, transitions to Candidate, and votes for itself.
// Must be called with n.mu held.
//
// It launches runElectionCampaign (campaign.go) to actually request votes
// from every peer and promote this node to Leader on a majority. It does
// NOT spawn a new RunElectionTimer — see that method's doc comment for why
// a single long-lived timer, not one per term, is what this project uses.
func (n *Node) becomeCandidateLocked(ctx context.Context) {
	n.persistent.CurrentTerm++
	n.state = Candidate
	n.persistent.VotedFor = n.id // a candidate always votes for itself
	n.leaderID = -1              // new term: no leader known for it yet
	n.electionResetEvent = time.Now()

	term := n.persistent.CurrentTerm
	log.Printf("[node %d] election timeout: becoming Candidate for term %d", n.id, term)

	go n.runElectionCampaign(ctx, term)
}
