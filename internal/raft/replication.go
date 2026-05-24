package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	kvgrpc "distributed-kv-store/internal/grpc"
	kvproto "distributed-kv-store/proto/kv"
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
	var wg sync.WaitGroup
	errCh := make(chan error, len(peers))
	for _, peer := range peers {
		if peer.ID == n.cfg.NodeID {
			continue
		}
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()
			if _, err := n.replicatePeer(ctx, peerID); err != nil {
				errCh <- err
			}
		}(peer.ID)
	}
	wg.Wait()
	close(errCh)
	n.mu.Lock()
	n.advanceCommitIndexLocked()
	n.mu.Unlock()
	if len(errCh) > 0 {
		return <-errCh
	}
	return nil
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
			resp, err := n.clients.SendInstallSnapshot(peerID, &kvproto.SnapshotRequest{
				Term:              term,
				LeaderId:          leaderID,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  lastIncludedTerm,
				Data:              data,
			})
			if err != nil {
				return false, err
			}
			if resp.Term > term {
				n.mu.Lock()
				n.becomeFollowerLocked(resp.Term, "")
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
		protoEntries := make([]*kvproto.LogEntry, 0, len(entries))
		for i := range entries {
			protoEntries = append(protoEntries, &kvproto.LogEntry{
				Index:      entries[i].Index,
				Term:       entries[i].Term,
				Op:         entries[i].Op,
				Key:        entries[i].Key,
				Value:      entries[i].Value,
				TtlSeconds: entries[i].TTL,
			})
		}
		resp, err := n.clients.SendAppendEntries(peerID, &kvproto.AppendEntriesRequest{
			Term:         term,
			LeaderId:     leaderID,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      protoEntries,
			LeaderCommit: leaderCommit,
		})
		if err != nil {
			return false, err
		}
		if resp.Term > term {
			n.mu.Lock()
			n.becomeFollowerLocked(resp.Term, "")
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
		if resp.ConflictTerm == 0 {
			n.mu.Lock()
			n.nextIndex[peerID] = maxInt64(1, resp.ConflictIndex)
			n.mu.Unlock()
			continue
		}
		if backtrackIndex, ok := n.logStore.SearchLastTerm(resp.ConflictTerm); ok {
			n.mu.Lock()
			n.nextIndex[peerID] = backtrackIndex + 1
			n.mu.Unlock()
			continue
		}
		n.mu.Lock()
		n.nextIndex[peerID] = maxInt64(1, resp.ConflictIndex)
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
			Op:    entry.Operation,
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
