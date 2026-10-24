package node

import (
	"context"
	"time"
)

// heartbeatInterval is how often a Leader sends AppendEntries to every
// peer. It must stay well under electionTimeoutMin (election.go), per the
// Raft paper's §5.6 timing requirement: broadcast time << election
// timeout. If heartbeats took longer to arrive than a follower's election
// timeout, a perfectly healthy Leader would still get challenged by
// needless elections. 50ms against a 150-300ms timeout leaves a wide
// safety margin for this project's localhost-RPC setup.
//
// It's also the upper bound on replication latency: a command submitted
// to the leader (Submit, submit.go) goes out with the next round, so it
// reaches followers within ~50ms.
const heartbeatInterval = 50 * time.Millisecond

// runHeartbeatLoop is the goroutine a freshly elected Leader runs for as
// long as it remains Leader in `term`: every heartbeatInterval, it sends
// each peer an AppendEntries RPC carrying whatever log entries that peer
// is still missing (none, if it's up to date — which makes the RPC a plain
// heartbeat). It's launched by becomeLeaderLocked and exits on its own
// once this Leader is no longer current for `term` — either because it
// stepped down (a reply revealed a higher term) or because ctx was
// cancelled.
//
// Heartbeats and log replication are deliberately the same RPC on the same
// schedule, exactly as in the paper: a heartbeat is just an AppendEntries
// with nothing new to send. That way there's one code path that both
// asserts leadership and pushes entries, instead of two that could drift
// apart.
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
		// previous round already stepped it down.
		if n.state != Leader || n.persistent.CurrentTerm != term {
			n.mu.Unlock()
			return
		}
		requests := n.buildAppendEntriesLocked()
		n.mu.Unlock()

		n.replicateRound(ctx, term, requests)
	}
}

// appendEntriesRequest is everything needed to send one AppendEntries RPC
// to one peer, captured under the lock so the network call can happen
// after releasing it.
type appendEntriesRequest struct {
	peerID       int
	addr         string
	prevLogIndex int
	prevLogTerm  int
	entries      []LogEntry
	leaderCommit int
}

// buildAppendEntriesLocked prepares this round's AppendEntries for every
// peer, based on that peer's NextIndex (Figure 2, "Volatile state on
// leaders"): the follower is sent every entry from NextIndex onward, and
// prevLogIndex/prevLogTerm describe the entry just before that — which is
// what the follower checks against its own log (append.go, rule 2).
//
// Must be called with n.mu held, by a Leader.
func (n *Node) buildAppendEntriesLocked() []appendEntriesRequest {
	lastIndex, _ := n.lastLogIndexAndTermLocked()
	requests := make([]appendEntriesRequest, 0, len(n.peers))

	for peerID, addr := range n.peers {
		next, ok := n.volatile.NextIndex[peerID]
		// Clamp NextIndex into [1, lastIndex+1]. It can't legitimately be
		// outside that range (a leader's own log never shrinks while it
		// leads), but a bad value here would make termAtLocked fail, so
		// fail safe instead of sending a malformed RPC.
		if !ok || next > lastIndex+1 {
			next = lastIndex + 1
		}
		if next < 1 {
			next = 1
		}

		prevLogIndex := next - 1
		prevLogTerm, _ := n.termAtLocked(prevLogIndex) // always ok after the clamp

		requests = append(requests, appendEntriesRequest{
			peerID:       peerID,
			addr:         addr,
			prevLogIndex: prevLogIndex,
			prevLogTerm:  prevLogTerm,
			entries:      n.entriesFromLocked(next), // a copy; safe to send unlocked
			leaderCommit: n.volatile.CommitIndex,
		})
	}
	return requests
}

// replicateRound sends one round of AppendEntries RPCs, one per peer, in
// parallel, and folds each reply back into the leader's state as it
// arrives (handleAppendEntriesReply).
func (n *Node) replicateRound(ctx context.Context, term int, requests []appendEntriesRequest) {
	type result struct {
		req   appendEntriesRequest
		reply AppendEntriesResult
		err   error
	}
	results := make(chan result, len(requests))

	for _, req := range requests {
		req := req // capture per-iteration value for the goroutine below
		go func() {
			reply, err := n.transport.SendAppendEntries(
				req.addr, term, n.id, req.prevLogIndex, req.prevLogTerm, req.entries, req.leaderCommit)
			results <- result{req: req, reply: reply, err: err}
		}()
	}

	for i := 0; i < len(requests); i++ {
		select {
		case <-ctx.Done():
			return
		case res := <-results:
			if res.err != nil {
				// An unreachable peer just misses this round; the next
				// round will retry with the same (or newer) entries.
				continue
			}
			n.mu.Lock()
			stillLeader := n.handleAppendEntriesReplyLocked(term, res.req, res.reply)
			n.mu.Unlock()
			if !stillLeader {
				return
			}
		}
	}
}

// handleAppendEntriesReplyLocked updates leader state from one peer's
// reply. Returns false if this node is no longer Leader for `term` (so the
// caller can stop processing the rest of the round).
//
// Must be called with n.mu held.
func (n *Node) handleAppendEntriesReplyLocked(term int, req appendEntriesRequest, reply AppendEntriesResult) bool {
	if reply.Term > n.persistent.CurrentTerm {
		// A peer is ahead of us: this Leader is stale (e.g. it was
		// partitioned away and a new election already happened without
		// it). Step down per the paper's §5.1 "Rules for Servers".
		n.stepDownLocked(reply.Term)
		return false
	}

	// The reply may arrive after this node already stepped down, or
	// even after it lost and later won a NEWER term. Either way it
	// describes a request from a leadership that no longer exists, so
	// it must not touch the current NextIndex/MatchIndex.
	if n.state != Leader || n.persistent.CurrentTerm != term {
		return false
	}

	if !reply.Success {
		n.backUpNextIndexLocked(req, reply)
		return true
	}

	// Success: the follower's log now matches ours up to and including
	// the last entry we sent.
	//
	// Only ever move MatchIndex/NextIndex FORWARD. Rounds fire every
	// 50ms, so several RPCs to the same peer can be in flight at once and
	// their replies can arrive out of order. A late reply to an older,
	// shorter request must not roll back progress a newer reply already
	// recorded — MatchIndex in particular is what commit decisions will
	// be based on (task 16), and Figure 2 requires it to increase
	// monotonically.
	matched := req.prevLogIndex + len(req.entries)
	if matched > n.volatile.MatchIndex[req.peerID] {
		n.volatile.MatchIndex[req.peerID] = matched
	}
	if matched+1 > n.volatile.NextIndex[req.peerID] {
		n.volatile.NextIndex[req.peerID] = matched + 1
	}

	// A MatchIndex may have just grown, so a new index may now be on a
	// majority: see whether CommitIndex can move forward (commit.go).
	// Followers learn the new CommitIndex from the leaderCommit field of
	// the next round's AppendEntries (append.go, rule 5).
	n.advanceCommitIndexLocked()
	return true
}

// backUpNextIndexLocked handles a follower rejecting AppendEntries because
// its log didn't match at prevLogIndex: it moves that follower's NextIndex
// back, so the next round sends from an earlier point. Repeated across
// rounds, this walks back to the last index where the two logs agree; from
// there the follower accepts, and its log converges to the leader's.
//
// The paper's baseline is NextIndex-- per rejection, which needs one round
// trip per mismatching entry. Instead this uses the follower's
// ConflictTerm/ConflictIndex hint (append.go) to skip a whole term at a
// time — the optimization described at the end of §5.3:
//
//   - ConflictTerm == 0: the follower's log is too short. Jump straight to
//     ConflictIndex, one past its last entry.
//   - Otherwise, if the leader's own log also has entries from
//     ConflictTerm, resume right after the leader's last entry of that
//     term (the two logs may agree up to there).
//   - Otherwise the leader never had that term at all, so the follower's
//     whole run of it is wrong: jump to ConflictIndex, where the run
//     starts.
//
// Must be called with n.mu held, by the Leader of the term the request was
// sent in.
func (n *Node) backUpNextIndexLocked(req appendEntriesRequest, reply AppendEntriesResult) {
	// Only react to a rejection of the request that matches this
	// follower's CURRENT NextIndex. Rounds overlap, so an older
	// rejection can arrive after a newer round already moved NextIndex;
	// acting on it again would back up further than needed (or, after a
	// later success, undo progress).
	if req.prevLogIndex+1 != n.volatile.NextIndex[req.peerID] {
		return
	}

	// The follower lost entries we had recorded as replicated on it: it
	// reports a log SHORTER than its MatchIndex. With persistent storage
	// this can't happen, but without it (see Block 4) a follower that
	// crashes and restarts comes back with an empty log. Our MatchIndex
	// for it is then false, so correct it down to what the follower
	// actually has. Lowering it is safe: MatchIndex only feeds commit
	// counting (commit.go), where a lower value can only make the leader
	// more conservative, and entries already committed stay committed.
	// It also lifts the MatchIndex+1 floor below, which would otherwise
	// force the leader to walk back one entry per round to an amnesiac
	// follower — found by TestFollowerCrashAndRecovery, where a follower
	// that lost 5 entries took 5 rounds to recover instead of 1.
	if reply.ConflictTerm == 0 && reply.ConflictIndex-1 < n.volatile.MatchIndex[req.peerID] {
		n.logger.Warn("follower lost its log", "term", n.persistent.CurrentTerm, "peer", req.peerID,
			"followerLogLength", reply.ConflictIndex-1, "matchIndex", n.volatile.MatchIndex[req.peerID])
		n.volatile.MatchIndex[req.peerID] = reply.ConflictIndex - 1
	}

	next := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		if last := n.lastIndexOfTermLocked(reply.ConflictTerm); last > 0 {
			next = last + 1
		}
	}

	// Safety bounds. Never go below MatchIndex+1: entries up to
	// MatchIndex are already confirmed to match on this follower, so
	// resending them is pointless. Never go above prevLogIndex: the
	// follower just told us it doesn't match there, so the retry must
	// start strictly earlier (this also guarantees progress, so a
	// malformed hint can never make the leader retry the same index
	// forever). And never below 1.
	if floor := n.volatile.MatchIndex[req.peerID] + 1; next < floor {
		next = floor
	}
	if next > req.prevLogIndex {
		next = req.prevLogIndex
	}
	if next < 1 {
		next = 1
	}

	n.logger.Debug("append entries rejected", "term", n.persistent.CurrentTerm, "peer", req.peerID,
		"prevLogIndex", req.prevLogIndex, "conflictTerm", reply.ConflictTerm,
		"conflictIndex", reply.ConflictIndex, "nextIndex", next)
	n.volatile.NextIndex[req.peerID] = next
}

// lastIndexOfTermLocked returns the index of the last entry in this node's
// log with the given term, or 0 if the log holds no entry from that term.
// Must be called with n.mu held.
func (n *Node) lastIndexOfTermLocked(term int) int {
	for i := len(n.persistent.Log) - 1; i >= 0; i-- {
		switch t := n.persistent.Log[i].Term; {
		case t == term:
			return i + 1
		case t < term:
			// Terms in a log never decrease, so once we've passed below
			// `term` it can't appear any earlier.
			return 0
		}
	}
	return 0
}
