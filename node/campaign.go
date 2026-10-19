package node

import (
	"context"
	"log"
)

// runElectionCampaign is the Candidate-side half of leader election (the
// Raft paper, section 5.2): it sends RequestVote RPCs to every peer in
// parallel, tallies the replies as they arrive, and promotes this node to
// Leader the moment a majority is reached. It's launched as its own
// goroutine by becomeCandidateLocked, since the RPCs it makes block on the
// network and must never be made while n.mu is held — holding the lock
// across a network call would stall every other RPC handler and the
// election timer for as long as the slowest peer takes to respond.
//
// term is the term this candidacy started under. It's passed in (rather
// than re-read from n.persistent.CurrentTerm) and re-checked under the lock
// at every step, because by the time a reply comes back this node may have
// moved on — stepped down, started a newer election, or already won.
func (n *Node) runElectionCampaign(ctx context.Context, term int) {
	n.mu.Lock()
	// Guard against a stale campaign: if this node isn't a Candidate for
	// `term` anymore, there is nothing left to campaign for. This can
	// happen if, between becomeCandidateLocked scheduling this goroutine
	// and it actually running, the node already heard from a leader or
	// started a newer term.
	if n.state != Candidate || n.persistent.CurrentTerm != term {
		n.mu.Unlock()
		return
	}
	lastLogIndex, lastLogTerm := n.lastLogIndexAndTermLocked()
	peerAddrs := make(map[int]string, len(n.peers))
	for id, addr := range n.peers {
		peerAddrs[id] = addr
	}
	n.mu.Unlock()

	totalNodes := len(peerAddrs) + 1 // every peer, plus this node itself
	votesNeeded := totalNodes/2 + 1
	votes := 1 // this node already voted for itself in becomeCandidateLocked

	if votes >= votesNeeded {
		// Degenerate case: a single-node cluster. A candidate of one is
		// already its own majority — no RPCs needed.
		n.mu.Lock()
		if n.state == Candidate && n.persistent.CurrentTerm == term {
			n.becomeLeaderLocked()
		}
		n.mu.Unlock()
		return
	}

	type voteResult struct {
		peerID      int
		peerTerm    int
		voteGranted bool
		err         error
	}
	results := make(chan voteResult, len(peerAddrs))

	for peerID, addr := range peerAddrs {
		peerID, addr := peerID, addr // capture per-iteration values for the goroutine below
		go func() {
			peerTerm, granted, err := n.transport.SendRequestVote(addr, term, n.id, lastLogIndex, lastLogTerm)
			results <- voteResult{peerID: peerID, peerTerm: peerTerm, voteGranted: granted, err: err}
		}()
	}

	// Count replies as they arrive, rather than waiting for all of them:
	// a candidate wins as soon as it has a majority, even if slower or
	// unreachable peers haven't answered yet.
	for i := 0; i < len(peerAddrs); i++ {
		select {
		case <-ctx.Done():
			return
		case res := <-results:
			if res.err != nil {
				log.Printf("[node %d] RequestVote to peer %d failed: %v", n.id, res.peerID, res.err)
				continue
			}

			n.mu.Lock()
			// Stop campaigning the moment this candidacy is no longer
			// current, for any reason (stepped down already, a newer
			// term started, this election was already won).
			if n.state != Candidate || n.persistent.CurrentTerm != term {
				n.mu.Unlock()
				return
			}

			if res.peerTerm > n.persistent.CurrentTerm {
				// A peer is ahead of us: we've lost, and this term is
				// stale. Per the paper's §5.1 "Rules for Servers", step
				// down to Follower and adopt the newer term.
				n.persistent.CurrentTerm = res.peerTerm
				n.state = Follower
				n.persistent.VotedFor = -1
				n.mu.Unlock()
				return
			}

			if res.voteGranted {
				votes++
			}

			if votes >= votesNeeded {
				n.becomeLeaderLocked()
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
		}
	}

	// Every reply is in (or failed) and no majority was reached: this
	// election is inconclusive, most likely a split vote across several
	// candidates. Nothing more to do here — the election timer already
	// running for this term (started alongside this campaign, in
	// becomeCandidateLocked) will fire and start a fresh election with a
	// new randomized timeout, per the paper's retry rule.
}

// becomeLeaderLocked performs the state transition the Raft paper
// describes at the end of a successful election (section 5.2 / Figure 2):
// a Candidate that wins a majority immediately becomes Leader and
// (re-)initializes its leader-only volatile state. Must be called with
// n.mu held.
func (n *Node) becomeLeaderLocked() {
	n.state = Leader

	// NextIndex/MatchIndex are leader-only bookkeeping for log
	// replication (Block 2): NextIndex starts optimistically at "one past
	// my last log entry" for every peer, MatchIndex starts at 0 (nothing
	// confirmed replicated yet). Figure 2 specifies exactly this
	// initialization.
	lastLogIndex, _ := n.lastLogIndexAndTermLocked()
	nextIndex := make(map[int]int, len(n.peers))
	matchIndex := make(map[int]int, len(n.peers))
	for peerID := range n.peers {
		nextIndex[peerID] = lastLogIndex + 1
		matchIndex[peerID] = 0
	}
	n.volatile.NextIndex = nextIndex
	n.volatile.MatchIndex = matchIndex

	log.Printf("[node %d] won election for term %d — becoming Leader", n.id, n.persistent.CurrentTerm)

	// TODO (task 8): start sending heartbeat AppendEntries to every peer
	// here, so followers don't time out and start a competing election.
}