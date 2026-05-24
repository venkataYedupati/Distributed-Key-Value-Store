package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	kvgrpc "distributed-kv-store/internal/grpc"
)

func electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(151)) * time.Millisecond
}

func (n *Node) electionLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-time.After(25 * time.Millisecond):
			if n.IsLeader() {
				continue
			}
			n.mu.RLock()
			lastHeartbeat := n.lastHeartbeat
			n.mu.RUnlock()
			if time.Since(lastHeartbeat) < electionTimeout() {
				continue
			}
			n.startElection(ctx)
		}
	}
}

func (n *Node) startElection(ctx context.Context) {
	n.mu.Lock()
	n.becomeCandidateLocked()
	term := n.currentTerm
	lastIndex, lastTerm := n.lastLogPosition()
	peers := n.clusterPeers()
	n.mu.Unlock()

	votes := 1
	voteNeeded := n.quorum()
	var mu sync.Mutex
	var once sync.Once
	voteCh := make(chan bool, len(peers))
	termCh := make(chan int64, len(peers))
	for _, peer := range peers {
		if peer.ID == n.cfg.NodeID {
			continue
		}
		go func(peerID string) {
			resp, err := n.clients.RequestVote(ctx, peerID, &kvgrpc.RequestVoteRequest{
				Term:        uint64(term),
				CandidateID: n.cfg.NodeID,
				LastLogIndex: uint64(lastIndex),
				LastLogTerm:  uint64(lastTerm),
			})
			if err != nil {
				voteCh <- false
				return
			}
			if resp.Term > uint64(term) {
				termCh <- int64(resp.Term)
				return
			}
			voteCh <- resp.VoteGranted
		}(peer.ID)
	}

	peerCount := len(peers) - 1
	for received := 0; received < peerCount; received++ {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case higherTerm := <-termCh:
			n.mu.Lock()
			n.becomeFollowerLocked(higherTerm, "")
			n.mu.Unlock()
			return
		case granted := <-voteCh:
			if granted {
				mu.Lock()
				votes++
				if votes >= voteNeeded {
					once.Do(func() {
						n.mu.Lock()
						n.becomeLeaderLocked()
						n.mu.Unlock()
						n.broadcastHeartbeat(ctx)
					})
				}
				mu.Unlock()
			}
		}
	}
}

func (n *Node) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-ticker.C:
			if !n.IsLeader() {
				continue
			}
			n.broadcastHeartbeat(ctx)
		}
	}
}

func (n *Node) broadcastHeartbeat(ctx context.Context) {
	peers := n.clusterPeers()
	for _, peer := range peers {
		if peer.ID == n.cfg.NodeID {
			continue
		}
		go n.sendHeartbeat(ctx, peer.ID)
	}
}

func (n *Node) sendHeartbeat(ctx context.Context, peerID string) {
	n.mu.RLock()
	term := n.currentTerm
	leaderID := n.cfg.NodeID
	prevIndex := n.nextIndex[peerID] - 1
	leaderCommit := n.commitIndex
	n.mu.RUnlock()

	prevTerm := uint64(0)
	if prevIndex > 0 && n.logStore != nil {
		if entry, err := n.logStore.Get(prevIndex); err == nil {
			prevTerm = uint64(entry.Term)
		}
	}
	_, err := n.clients.AppendEntries(ctx, peerID, &kvgrpc.AppendEntriesRequest{
		Term:         uint64(term),
		LeaderID:     leaderID,
		PrevLogIndex: uint64(prevIndex),
		PrevLogTerm:  prevTerm,
		Entries:      nil,
		LeaderCommit: uint64(leaderCommit),
	})
	if err != nil {
		return
	}
}

func (n *Node) handleLeaderHeartbeat(req *kvgrpc.HeartbeatRequest) *kvgrpc.HeartbeatResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if int64(req.Term) < n.currentTerm {
		return &kvgrpc.HeartbeatResponse{Term: uint64(n.currentTerm), Accepted: false}
	}
	if int64(req.Term) > n.currentTerm {
		n.currentTerm = int64(req.Term)
		n.votedFor = ""
		n.persistTermAndVoteLocked()
	}
	n.state = Follower
	n.leaderID = req.LeaderID
	n.lastHeartbeat = time.Now().UTC()
	if req.CommitIndex > uint64(n.commitIndex) {
		n.commitIndex = int64(req.CommitIndex)
		n.signalCommit(n.commitIndex)
	}
	n.metrics.SetTerm(uint64(n.currentTerm))
	n.metrics.SetLeader(false)
	return &kvgrpc.HeartbeatResponse{Term: uint64(n.currentTerm), Accepted: true}
}

func (n *Node) handleVoteRequest(req *kvgrpc.RequestVoteRequest) *kvgrpc.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	candidateTerm := int64(req.Term)
	lastIndex, lastTerm := n.lastLogPosition()
	upToDate := int64(req.LastLogTerm) > lastTerm || (int64(req.LastLogTerm) == lastTerm && int64(req.LastLogIndex) >= lastIndex)
	if candidateTerm < n.currentTerm {
		return &kvgrpc.RequestVoteResponse{Term: uint64(n.currentTerm), VoteGranted: false}
	}
	if candidateTerm > n.currentTerm {
		n.currentTerm = candidateTerm
		n.votedFor = ""
		n.state = Follower
		n.persistTermAndVoteLocked()
	}
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		n.lastHeartbeat = time.Now().UTC()
		n.persistTermAndVoteLocked()
		return &kvgrpc.RequestVoteResponse{Term: uint64(n.currentTerm), VoteGranted: true}
	}
	return &kvgrpc.RequestVoteResponse{Term: uint64(n.currentTerm), VoteGranted: false}
}
