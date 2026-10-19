package node

import (
	"context"
	"time"
)

// heartbeatInterval is how often a Leader sends an AppendEntries RPC
// (empty, for now — real replication is task 13) to every peer. It must
// stay well under electionTimeoutMin (election.go), per the Raft paper's
// §5.6 timing requirement: broadcast time << election timeout. If
// heartbeats took longer to arrive than a follower's election timeout, a
// perfectly healthy Leader would still get challenged by needless
// elections. 50ms against a 150-300ms timeout leaves a wide safety margin
// for this project's localhost-RPC setup.
const heartbeatInterval = 50 * time.Millisecond

// runHeartbeatLoop is the goroutine a freshly elected Leader runs for as
// long as it remains Leader in `term`: every heartbeatInterval, it sends a
// heartbeat AppendEntries to every peer in parallel. It's launched by
// becomeLeaderLocked and exits on its own once this Leader is no longer
// current for `term` — either because it stepped down (e.g. a heartbeat
// reply revealed a higher term) or because ctx was cancelled.
func (n *Node) runHeartbeatLoop(ctx context.Context, term int) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		n.mu.Lock()
		// If this node isn't Leader for `term` anymore, this loop's job
		// is done — either a newer term already started elsewhere, or a
		// previous round of this same loop already stepped it down.
		if n.state != Leader || n.persistent.CurrentTerm != term {
			n.mu.Unlock()
			return
		}
		peerAddrs := make(map[int]string, len(n.peers))
		for id, addr := range n.peers {
			peerAddrs[id] = addr
		}
		leaderCommit := n.volatile.CommitIndex
		n.mu.Unlock()

		n.sendHeartbeatRound(ctx, term, peerAddrs, leaderCommit)
	}
}

// sendHeartbeatRound fires one round of heartbeat AppendEntries RPCs at
// every peer in parallel, and steps this node down to Follower if any
// reply reveals a term higher than its own — symmetric with how
// runElectionCampaign (campaign.go) handles the same situation for a
// Candidate.
//
// PrevLogIndex, PrevLogTerm and Entries are left at their zero values:
// this task only has to keep followers from timing out while a Leader is
// alive, not actually replicate anything yet (that's task 13).
func (n *Node) sendHeartbeatRound(ctx context.Context, term int, peerAddrs map[int]string, leaderCommit int) {
	type heartbeatResult struct {
		peerTerm int
		err      error
	}
	results := make(chan heartbeatResult, len(peerAddrs))

	for _, addr := range peerAddrs {
		addr := addr // capture per-iteration value for the goroutine below
		go func() {
			peerTerm, _, err := n.transport.SendAppendEntries(addr, term, n.id, 0, 0, nil, leaderCommit)
			results <- heartbeatResult{peerTerm: peerTerm, err: err}
		}()
	}

	for i := 0; i < len(peerAddrs); i++ {
		select {
		case <-ctx.Done():
			return
		case res := <-results:
			if res.err != nil {
				// An unreachable peer, or one whose AppendEntries RPC
				// isn't wired up yet (task 13), just misses this
				// heartbeat — not fatal to this node remaining Leader.
				continue
			}

			n.mu.Lock()
			if res.peerTerm > n.persistent.CurrentTerm {
				// A peer is ahead of us: this Leader is stale (e.g. it
				// was partitioned away and a new election already
				// happened without it). Step down per the paper's §5.1
				// "Rules for Servers".
				n.persistent.CurrentTerm = res.peerTerm
				n.state = Follower
				n.persistent.VotedFor = -1
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
		}
	}
}