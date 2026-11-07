package node

import (
	"context"
	"math/rand"
	"time"
)

// DefaultElectionTimeout is the default lower bound of the randomized
// election timeout; each timeout is drawn from [min, 2*min). The Raft paper
// (section 5.6) requires
//
//	broadcast time << election timeout << mean time between failures
//
// where broadcast time is how long a node takes to send RPCs to every
// server and get their replies, INCLUDING writing to stable storage before
// replying. 150-300ms is the range the paper benchmarks against. It is
// right for an in-memory cluster on one machine, where broadcast time is
// well under a millisecond, but too short when nodes persist to a slow
// disk: see SetElectionTimeout, and cmd/main.go, which uses a longer
// default whenever -data-dir is set.
//
// Randomizing the timeout (rather than giving every node the same one) is
// what makes split votes rare: if all followers timed out at exactly the
// same instant, they'd all become candidates at once in every election,
// and no candidate would ever gather a majority.
const DefaultElectionTimeout = 150 * time.Millisecond

// electionTimerPollInterval is how often RunElectionTimer wakes up to
// check whether the timeout has elapsed. It's unrelated to the election
// timeout itself — it just needs to be short enough that the timer's
// actual firing time is close to the randomized deadline, without busy
// looping. 10ms gives sub-10ms precision on a 150-300ms timeout, which is
// more than tight enough here.
const electionTimerPollInterval = 10 * time.Millisecond

// SetElectionTimeout sets the lower bound of this node's randomized
// election timeout; each timeout is then drawn from [min, 2*min). Call it
// before the node starts.
//
// The timeout has to be much longer than the slowest round of RPCs,
// including each node's fsync before it replies. A vote, for example,
// needs the candidate's save of its new term plus the voter's save of its
// vote. If that takes longer than the candidate's timeout, the candidate
// gives up and starts a new election before the votes for the previous one
// arrive, and the cluster can go on electing forever without ever getting
// a leader: TestSlowDiskCausesNoSpuriousElections reproduces exactly that
// with 150ms saves and the in-memory default.
func (n *Node) SetElectionTimeout(min time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.electionTimeoutMin = min
}

// randomElectionTimeoutLocked returns a random duration in
// [min, 2*min) for this node's configured minimum, using the
// package-level math/rand source. Not cryptographically random, and
// doesn't need to be: this is scheduling jitter, not a security boundary.
// Must be called with n.mu held.
func (n *Node) randomElectionTimeoutLocked() time.Duration {
	min := n.electionTimeoutMin
	return min + time.Duration(rand.Int63n(int64(min)))
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
	timeout := n.randomElectionTimeoutLocked()
	termWatched := n.persistent.CurrentTerm
	n.mu.Unlock()

	n.logger.Debug("election timer started", "term", termWatched, "timeout", timeout.String())

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
			timeout = n.randomElectionTimeoutLocked()
			termWatched = n.persistent.CurrentTerm
			n.logger.Debug("election timer resynced", "term", termWatched, "timeout", timeout.String())
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
			timeout = n.randomElectionTimeoutLocked()
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

	// Save the new term and the vote for ourselves BEFORE asking anyone
	// for a vote. Otherwise a crash right after the RequestVotes go out
	// could bring this node back in the old term, unaware it already
	// campaigned (and voted) in the new one.
	n.persistLocked()

	term := n.persistent.CurrentTerm
	n.logger.Info("became candidate", "term", term, "reason", "election timeout")

	go n.runElectionCampaign(ctx, term)
}
