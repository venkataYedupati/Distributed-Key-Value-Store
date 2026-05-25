package raft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	kvgrpc "distributed-kv-store/internal/grpc"
)

func (n *Node) startReplicationLoops(ctx context.Context) {
	peers := n.clusterPeers()
	for _, peer := range peers {
		if peer.ID == n.cfg.NodeID {
			continue
		}
		peerID := peer.ID
		ch := n.replicateCh[peerID]
		go n.replicationLoop(ctx, peerID, ch)
	}
}

func (n *Node) replicationLoop(ctx context.Context, peerID string, signalCh <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-signalCh:
			if !n.IsLeader() {
				continue
			}
			_, _ = n.replicatePeer(ctx, peerID)
		}
	}
}

func (n *Node) replicateEntry(ctx context.Context, entry LogEntry) error {
	if !n.IsLeader() {
		return errors.New("not leader")
	}
	peers := n.clusterPeers()
	// replicate to peers concurrently and stop once quorum is reached
	total := len(peers)
	needed := n.quorum()
	ackCh := make(chan bool, total)
	// launch replication goroutines
	remaining := 0
	for _, peer := range peers {
		if peer.ID == n.cfg.NodeID {
			continue
		}
		remaining++
		peerID := peer.ID
		go func(pid string) {
			ok, err := n.replicatePeer(ctx, pid)
			if err != nil {
				ackCh <- false
				return
			}
			ackCh <- ok
		}(peerID)
	}

	// count acknowledgements (include self)
	acks := 1
	for i := 0; i < remaining; i++ {
		ok := <-ackCh
		if ok {
			acks++
		}
		if acks >= needed {
			n.mu.Lock()
			n.advanceCommitIndexLocked()
			n.mu.Unlock()
			return nil
		}
	}
	n.mu.Lock()
	n.advanceCommitIndexLocked()
	n.mu.Unlock()
	return errors.New("quorum not reached")
}

func (n *Node) replicatePeer(ctx context.Context, peerID string) (bool, error) {
	for attempts := 0; attempts < 8; attempts++ {
		n.mu.RLock()
		if !n.IsLeader() {
			n.mu.RUnlock()
			return false, errors.New("not leader")
		}
		nextIndex := n.nextIndex[peerID]
		snapshotIndex := n.snapshotIndex
		leaderCommit := n.commitIndex
		term := n.currentTerm
		leaderID := n.cfg.NodeID
		n.mu.RUnlock()

		if nextIndex <= snapshotIndex && snapshotIndex > 0 {
			data, lastIncludedIndex, lastIncludedTerm, err := n.snapshotBytes()
			if err != nil {
				return false, err
			}
			log.Printf("replicatePeer: installing snapshot to %s term=%d snapshotIndex=%d lastIncludedIndex=%d", peerID, term, snapshotIndex, lastIncludedIndex)
			resp, err := n.clients.InstallSnapshot(context.Background(), peerID, &kvgrpc.SnapshotRequest{
				Term:              uint64(term),
				LeaderID:          leaderID,
				LastIncludedIndex: uint64(lastIncludedIndex),
				LastIncludedTerm:  uint64(lastIncludedTerm),
				Data:              data,
			})
			if err != nil {
				log.Printf("replicatePeer: InstallSnapshot to %s failed: %v", peerID, err)
				return false, err
			}
			log.Printf("replicatePeer: InstallSnapshot response from %s: term=%d", peerID, resp.Term)
			if int64(resp.Term) > term {
				n.mu.Lock()
				n.becomeFollowerLocked(int64(resp.Term), "")
				n.mu.Unlock()
				return false, nil
			}
			n.mu.Lock()
			n.matchIndex[peerID] = snapshotIndex
			n.nextIndex[peerID] = snapshotIndex + 1
			n.mu.Unlock()
			continue
		}

		prevIndex := nextIndex - 1
		prevTerm := int64(0)
		if prevIndex > 0 && n.logStore != nil {
			if entry, err := n.logStore.Get(prevIndex); err == nil {
				prevTerm = entry.Term
			}
		}
		lastIndex := n.logStore.LastIndex()
		entries, err := n.logStore.GetRange(nextIndex, lastIndex)
		if err != nil {
			return false, err
		}
		peerEntries := make([]kvgrpc.LogEntry, 0, len(entries))
		for i := range entries {
			peerEntries = append(peerEntries, kvgrpc.LogEntry{
				Index:      uint64(entries[i].Index),
				Term:       uint64(entries[i].Term),
				Op:         entries[i].Op,
				Key:        entries[i].Key,
				Value:      entries[i].Value,
				TTLSeconds: entries[i].TTL,
			})
		}
		log.Printf("replicatePeer: AppendEntries -> %s nextIndex=%d prevIndex=%d prevTerm=%d entries=%d leaderCommit=%d", peerID, nextIndex, prevIndex, prevTerm, len(entries), leaderCommit)
		resp, err := n.clients.AppendEntries(context.Background(), peerID, &kvgrpc.AppendEntriesRequest{
			Term:         uint64(term),
			LeaderID:     leaderID,
			PrevLogIndex: uint64(prevIndex),
			PrevLogTerm:  uint64(prevTerm),
			Entries:      peerEntries,
			LeaderCommit: uint64(leaderCommit),
		})
		if err != nil {
			log.Printf("replicatePeer: AppendEntries to %s failed: %v", peerID, err)
			return false, err
		}
		log.Printf("replicatePeer: AppendEntries response from %s: term=%d success=%v matchIndex=%d conflictIndex=%d conflictTerm=%d", peerID, resp.Term, resp.Success, resp.MatchIndex, resp.ConflictIndex, resp.ConflictTerm)
		if int64(resp.Term) > term {
			n.mu.Lock()
			n.becomeFollowerLocked(int64(resp.Term), "")
			n.mu.Unlock()
			return false, nil
		}
		if resp.Success {
			matchIndex := prevIndex + int64(len(entries))
			n.mu.Lock()
			n.matchIndex[peerID] = matchIndex
			n.nextIndex[peerID] = matchIndex + 1
			n.advanceCommitIndexLocked()
			n.mu.Unlock()
			return true, nil
		}
		// handle conflict hints from follower (keep existing logic, response fields map)
		if resp.ConflictTerm == 0 {
			n.mu.Lock()
			n.nextIndex[peerID] = maxInt64(1, int64(resp.ConflictIndex))
			n.mu.Unlock()
			continue
		}
		if backtrackIndex, ok := n.logStore.SearchLastTerm(int64(resp.ConflictTerm)); ok {
			n.mu.Lock()
			n.nextIndex[peerID] = backtrackIndex + 1
			n.mu.Unlock()
			continue
		}
		n.mu.Lock()
		n.nextIndex[peerID] = maxInt64(1, int64(resp.ConflictIndex))
		n.mu.Unlock()
	}
	return false, fmt.Errorf("peer %s did not catch up", peerID)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (n *Node) AppendEntries(ctx context.Context, req *kvgrpc.AppendEntriesRequest) (*kvgrpc.AppendEntriesResponse, error) {
	entries := make([]LogEntry, 0, len(req.Entries))
	for _, entry := range req.Entries {
		entries = append(entries, LogEntry{
			Index: int64(entry.Index),
			Term:  int64(entry.Term),
			Op:    entry.Op,
			Key:   entry.Key,
			Value: entry.Value,
			TTL:   entry.TTLSeconds,
		})
	}
	success, conflictIndex, conflictTerm, matchIndex := n.handleAppendEntries(int64(req.Term), req.LeaderID, int64(req.PrevLogIndex), int64(req.PrevLogTerm), entries, int64(req.LeaderCommit))
	return &kvgrpc.AppendEntriesResponse{Term: uint64(n.CurrentTerm()), Success: success, MatchIndex: uint64(matchIndex), ConflictIndex: uint64(conflictIndex), ConflictTerm: uint64(conflictTerm)}, nil
}

func (n *Node) Heartbeat(ctx context.Context, req *kvgrpc.HeartbeatRequest) (*kvgrpc.HeartbeatResponse, error) {
	return n.handleLeaderHeartbeat(req), nil
}

func (n *Node) RequestVote(ctx context.Context, req *kvgrpc.RequestVoteRequest) (*kvgrpc.RequestVoteResponse, error) {
	return n.handleVoteRequest(req), nil
}

func (n *Node) InstallSnapshot(ctx context.Context, req *kvgrpc.SnapshotRequest) (*kvgrpc.SnapshotResponse, error) {
	if req == nil {
		return &kvgrpc.SnapshotResponse{Term: uint64(n.CurrentTerm())}, nil
	}
	// apply snapshot bytes and update state
	if err := n.applySnapshotBytes(req.Data, int64(req.LastIncludedIndex), int64(req.LastIncludedTerm)); err != nil {
		return &kvgrpc.SnapshotResponse{Term: uint64(n.CurrentTerm())}, err
	}
	return &kvgrpc.SnapshotResponse{Term: uint64(n.CurrentTerm())}, nil
}

func (n *Node) handleAppendEntries(term int64, leaderID string, prevLogIndex int64, prevLogTerm int64, entries []LogEntry, leaderCommit int64) (bool, int64, int64, int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if term < n.currentTerm {
		return false, n.logStore.LastIndex() + 1, n.logStore.LastTerm(), n.lastApplied
	}
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.persistTermAndVoteLocked()
	}
	n.state = Follower
	n.leaderID = leaderID
	n.lastHeartbeat = time.Now().UTC()
	n.metrics.SetLeader(false)
	n.metrics.SetTerm(uint64(n.currentTerm))

	if prevLogIndex > 0 {
		prevEntry, err := n.logStore.Get(prevLogIndex)
		if err != nil {
			return false, n.logStore.LastIndex() + 1, n.logStore.LastTerm(), n.lastApplied
		}
		if prevEntry.Term != prevLogTerm {
			conflictTerm := prevEntry.Term
			conflictIndex := prevLogIndex
			for conflictIndex > 1 {
				entry, err := n.logStore.Get(conflictIndex - 1)
				if err != nil || entry.Term != conflictTerm {
					break
				}
				conflictIndex--
			}
			return false, conflictIndex, conflictTerm, n.lastApplied
		}
	}

	if len(entries) > 0 {
		if err := n.logStore.DeleteFrom(entries[0].Index); err != nil {
			return false, n.logStore.LastIndex() + 1, n.logStore.LastTerm(), n.lastApplied
		}
		for _, entry := range entries {
			if err := n.logStore.Append(entry); err != nil {
				return false, n.logStore.LastIndex() + 1, n.logStore.LastTerm(), n.lastApplied
			}
		}
	}
	if leaderCommit > n.commitIndex {
		lastIndex := n.logStore.LastIndex()
		if leaderCommit < lastIndex {
			n.commitIndex = leaderCommit
		} else {
			n.commitIndex = lastIndex
		}
		n.metrics.SetCommitIndex(n.commitIndex)
		n.signalCommit(n.commitIndex)
	}
	return true, 0, 0, n.lastApplied
}

func (n *Node) takeSnapshotOnFollower() {
	if !n.snapshotEligible() {
		return
	}
	n.takeSnapshot()
}
