package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"distributed-kv-store/internal/config"
	kvgrpc "distributed-kv-store/internal/grpc"
	"distributed-kv-store/internal/hash"
	"distributed-kv-store/internal/store"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "follower"
	}
}

type Node struct {
	cfg      config.Config
	engine   *store.Engine
	ring     *hash.Ring
	clients  *kvgrpc.Client
	logStore *LogStore

	mu             sync.RWMutex
	state          Role
	currentTerm    int64
	votedFor       string
	leaderID       string
	commitIndex    int64
	lastApplied    int64
	snapshotIndex  int64
	snapshotTerm   int64
	nextIndex      map[string]int64
	matchIndex     map[string]int64
	lastHeartbeat  time.Time
	started        bool
	metrics        *Metrics
	commitCh       chan int64
	stopCh         chan struct{}
	replicateCh    map[string]chan struct{}
}

func NewNode(cfg config.Config, engine *store.Engine, ring *hash.Ring, clients *kvgrpc.Client) *Node {
	logStore, _ := NewLogStore(cfg.DataDir)
	node := &Node{
		cfg:         cfg,
		engine:      engine,
		ring:        ring,
		clients:     clients,
		logStore:    logStore,
		state:       Follower,
		metrics:     newMetrics(cfg.NodeID),
		commitCh:    make(chan int64, 1024),
		stopCh:      make(chan struct{}),
		replicateCh: make(map[string]chan struct{}),
		nextIndex:   make(map[string]int64),
		matchIndex:  make(map[string]int64),
	}
	if logStore != nil {
		if term, err := logStore.LoadTerm(); err == nil {
			node.currentTerm = term
		}
		if votedFor, err := logStore.LoadVotedFor(); err == nil {
			node.votedFor = votedFor
		}
	}
	node.resetReplicationStateLocked()
	return node
}

func (n *Node) Start(ctx context.Context) error {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return nil
	}
	n.started = true
	n.lastHeartbeat = time.Now().UTC()
	n.resetReplicationStateLocked()
	n.mu.Unlock()

	if n.logStore != nil {
		if err := n.replayLog(); err != nil {
			return err
		}
	}

	go n.applyLoop(ctx)
	go n.startReplicationLoops(ctx)
	go n.electionLoop(ctx)
	go n.heartbeatLoop(ctx)
	if _, err := kvgrpc.Serve(ctx, n.cfg.BindGRPCAddr(), n); err != nil {
		return err
	}
	return nil
}

func (n *Node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	select {
	case <-n.stopCh:
	default:
		close(n.stopCh)
	}
	if n.logStore != nil {
		_ = n.logStore.Close()
	}
	return nil
}

func (n *Node) Metrics() *Metrics { return n.metrics }

func (n *Node) Role() Role {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *Node) CurrentTerm() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

func (n *Node) CommitIndex() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex
}

func (n *Node) Term() int64 { return n.CurrentTerm() }

func (n *Node) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}

func (n *Node) IsLeader() bool { return n.Role() == Leader }

func (n *Node) Status() map[string]any {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return map[string]any{
		"node_id":        n.cfg.NodeID,
		"role":           n.state.String(),
		"term":           n.currentTerm,
		"leader_id":      n.leaderID,
		"commit_index":   n.commitIndex,
		"last_applied":    n.lastApplied,
		"snapshot_index":  n.snapshotIndex,
		"snapshot_term":   n.snapshotTerm,
		"last_heartbeat":  n.lastHeartbeat,
	}
}

func (n *Node) routeWriteTarget(key string) string {
	if owner, ok := n.ring.GetNode(key); ok {
		return owner.ID
	}
	return n.LeaderID()
}

func (n *Node) routeReadTarget(key string) string {
	if owner, ok := n.ring.GetNode(key); ok {
		return owner.ID
	}
	return n.LeaderID()
}

func (n *Node) resetReplicationStateLocked() {
	if n.nextIndex == nil {
		n.nextIndex = make(map[string]int64)
	}
	if n.matchIndex == nil {
		n.matchIndex = make(map[string]int64)
	}
	lastIndex := int64(0)
	if n.logStore != nil {
		lastIndex = n.logStore.LastIndex()
	}
	peers := n.clusterPeers()
	for _, peer := range peers {
		if peer.ID == n.cfg.NodeID {
			continue
		}
		n.nextIndex[peer.ID] = lastIndex + 1
		n.matchIndex[peer.ID] = 0
		if _, ok := n.replicateCh[peer.ID]; !ok {
			n.replicateCh[peer.ID] = make(chan struct{}, 1)
		}
	}
}

func (n *Node) clusterPeers() []hash.PhysicalNode {
	return n.ring.Nodes()
}

func (n *Node) replicaSetFor(key string) []hash.PhysicalNode {
	return n.ring.GetReplicaSet(key)
}

func (n *Node) signalCommit(index int64) {
	select {
	case n.commitCh <- index:
	default:
	}
}

func (n *Node) notifyReplicators() {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for peerID, ch := range n.replicateCh {
		if peerID == n.cfg.NodeID {
			continue
		}
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (n *Node) replayLog() error {
	if n.logStore == nil {
		return nil
	}
	entries, err := n.logStore.GetRange(1, n.logStore.LastIndex())
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := n.applyEntry(entry); err != nil {
			return err
		}
		n.lastApplied = entry.Index
		n.commitIndex = entry.Index
	}
	return nil
}

func (n *Node) applyLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case index := <-n.commitCh:
			n.applyCommitted(index)
		}
	}
}

func (n *Node) applyCommitted(target int64) {
	for {
		n.mu.Lock()
		if n.lastApplied >= target {
			n.metrics.SetCommitIndex(n.commitIndex)
			n.mu.Unlock()
			return
		}
		nextIndex := n.lastApplied + 1
		entry, err := n.logStore.Get(nextIndex)
		if err != nil {
			n.mu.Unlock()
			return
		}
		if err := n.applyEntry(entry); err != nil {
			n.mu.Unlock()
			return
		}
		n.lastApplied = entry.Index
		n.metrics.SetCommitIndex(n.commitIndex)
		if n.lastApplied-n.snapshotIndex >= 1000 {
			n.mu.Unlock()
			n.takeSnapshot()
			continue
		}
		n.mu.Unlock()
	}
}

func (n *Node) persistTermAndVoteLocked() {
	if n.logStore == nil {
		return
	}
	_ = n.logStore.SaveTerm(n.currentTerm)
	_ = n.logStore.SaveVotedFor(n.votedFor)
}

func (n *Node) setTermLocked(term int64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.persistTermAndVoteLocked()
		n.metrics.SetTerm(uint64(n.currentTerm))
	}
}

func (n *Node) setVotedForLocked(candidate string) {
	n.votedFor = candidate
	n.persistTermAndVoteLocked()
}

func (n *Node) becomeFollowerLocked(term int64, leaderID string) {
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
}

func (n *Node) becomeCandidateLocked() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.cfg.NodeID
	n.metrics.IncElection()
	n.persistTermAndVoteLocked()
	n.metrics.SetTerm(uint64(n.currentTerm))
	n.metrics.SetLeader(false)
}

func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	n.leaderID = n.cfg.NodeID
	n.lastHeartbeat = time.Now().UTC()
	n.votedFor = n.cfg.NodeID
	n.persistTermAndVoteLocked()
	n.metrics.SetLeader(true)
	n.metrics.SetTerm(uint64(n.currentTerm))
	lastIndex := int64(0)
	if n.logStore != nil {
		lastIndex = n.logStore.LastIndex()
	}
	for peerID := range n.replicateCh {
		if peerID == n.cfg.NodeID {
			continue
		}
		n.nextIndex[peerID] = lastIndex + 1
		if _, ok := n.matchIndex[peerID]; !ok {
			n.matchIndex[peerID] = 0
		}
	}
}

func (n *Node) quorum() int {
	return len(n.clusterPeers())/2 + 1
}

func (n *Node) majorityReached(votes int) bool {
	return votes >= n.quorum()
}

func (n *Node) lastLogPosition() (int64, int64) {
	if n.logStore == nil {
		return 0, 0
	}
	return n.logStore.LastIndex(), n.logStore.LastTerm()
}

func (n *Node) snapshotEligible() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastApplied-n.snapshotIndex >= 1000
}

func (n *Node) takeSnapshot() {
	data, lastIncludedIndex, lastIncludedTerm, err := n.snapshotBytes()
	if err != nil {
		return
	}
	if err := n.applySnapshotBytes(data, lastIncludedIndex, lastIncludedTerm); err != nil {
		return
	}
	if n.logStore != nil && lastIncludedIndex > 0 {
		_ = n.logStore.DeleteThrough(lastIncludedIndex)
	}
	n.mu.Lock()
	n.snapshotIndex = lastIncludedIndex
	n.snapshotTerm = lastIncludedTerm
	if n.commitIndex < lastIncludedIndex {
		n.commitIndex = lastIncludedIndex
	}
	if n.lastApplied < lastIncludedIndex {
		n.lastApplied = lastIncludedIndex
	}
	n.mu.Unlock()
}

func (n *Node) advanceCommitIndexLocked() {
	lastIndex := n.logStore.LastIndex()
	for candidate := lastIndex; candidate > n.commitIndex; candidate-- {
		entry, err := n.logStore.Get(candidate)
		if err != nil {
			continue
		}
		if entry.Term != n.currentTerm {
			continue
		}
		votes := 1
		for peerID, matched := range n.matchIndex {
			if peerID == n.cfg.NodeID {
				continue
			}
			if matched >= candidate {
				votes++
			}
		}
		if votes >= n.quorum() {
			n.commitIndex = candidate
			n.signalCommit(candidate)
			return
		}
	}
}

func (n *Node) String() string {
	return fmt.Sprintf("node[%s role=%s term=%d]", n.cfg.NodeID, n.Role().String(), n.CurrentTerm())
}

func (n *Node) writeLatency(op string, started time.Time, status string) {
	n.metrics.Inc(op, status)
	n.metrics.Observe(op, time.Since(started).Seconds())
}

func (n *Node) stopRequested() bool {
	select {
	case <-n.stopCh:
		return true
	default:
		return false
	}
}

func (n *Node) ensureStarted() error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.started {
		return errors.New("node not started")
	}
	return nil
}

func (n *Node) signalHeartbeat() {
	n.mu.Lock()
	n.lastHeartbeat = time.Now().UTC()
	n.mu.Unlock()
}

func (n *Node) applyEntry(entry LogEntry) error {
	switch entry.Op {
	case "set", "SET":
		return n.engine.Set(entry.Key, entry.Value, time.Duration(entry.TTL)*time.Second)
	case "delete", "DELETE":
		return n.engine.Delete(entry.Key)
	default:
		return nil
	}
}

func (n *Node) Get(ctx context.Context, key string) (store.Entry, error) {
	start := time.Now()
	defer func() {
		n.writeLatency("get", start, "ok")
	}()
	if !n.IsLeader() && n.LeaderID() != "" && n.LeaderID() != n.cfg.NodeID {
		resp, err := n.clients.Read(ctx, n.LeaderID(), &kvgrpc.ReadRequest{Key: key})
		if err != nil {
			return store.Entry{}, err
		}
		if !resp.Found {
			return store.Entry{}, store.ErrNotFound
		}
		entry := store.Entry{Key: key, Value: resp.Value, Deleted: resp.Deleted}
		return entry, nil
	}
	entry, err := n.engine.Get(key)
	if err != nil {
		return store.Entry{}, err
	}
	return entry, nil
}

func (n *Node) Write(ctx context.Context, key, value string, ttl time.Duration) error {
	start := time.Now()
	defer func() {
		n.writeLatency("set", start, "ok")
	}()
	if !n.IsLeader() {
		leader := n.LeaderID()
		if leader == "" || leader == n.cfg.NodeID {
			return errors.New("not leader")
		}
		resp, err := n.clients.Propose(ctx, leader, &kvgrpc.ProposeRequest{Key: key, Value: value, Operation: "set", TTLSeconds: int64(ttl.Seconds())})
		if err != nil {
			return err
		}
		if !resp.Success {
			return errors.New(resp.Error)
		}
		return nil
	}
	return n.proposeCommand(ctx, "set", key, value, ttl)
}

func (n *Node) Delete(ctx context.Context, key string) error {
	start := time.Now()
	defer func() {
		n.writeLatency("delete", start, "ok")
	}()
	if !n.IsLeader() {
		leader := n.LeaderID()
		if leader == "" || leader == n.cfg.NodeID {
			return errors.New("not leader")
		}
		resp, err := n.clients.Propose(ctx, leader, &kvgrpc.ProposeRequest{Key: key, Operation: "delete"})
		if err != nil {
			return err
		}
		if !resp.Success {
			return errors.New(resp.Error)
		}
		return nil
	}
	return n.proposeCommand(ctx, "delete", key, "", 0)
}

func (n *Node) proposeCommand(ctx context.Context, op, key, value string, ttl time.Duration) error {
	n.mu.Lock()
	if n.state != Leader {
		leader := n.leaderID
		n.mu.Unlock()
		if leader == "" || leader == n.cfg.NodeID {
			return errors.New("not leader")
		}
		_, err := n.clients.Propose(ctx, leader, &kvgrpc.ProposeRequest{Key: key, Value: value, Operation: op, TTLSeconds: int64(ttl.Seconds())})
		return err
	}
	term := n.currentTerm
	index := int64(1)
	if n.logStore != nil {
		index = n.logStore.LastIndex() + 1
	}
	entry := LogEntry{Index: index, Term: term, Op: op, Key: key, Value: value, TTL: int64(ttl.Seconds())}
	if n.logStore != nil {
		if err := n.logStore.Append(entry); err != nil {
			n.mu.Unlock()
			return err
		}
		n.metrics.IncLogEntry()
	}
	if self := n.cfg.NodeID; self != "" {
		n.matchIndex[self] = entry.Index
		n.nextIndex[self] = entry.Index + 1
	}
	n.mu.Unlock()

	if err := n.replicateEntry(ctx, entry); err != nil {
		return err
	}
	n.mu.Lock()
	n.commitIndex = entry.Index
	n.metrics.SetCommitIndex(n.commitIndex)
	n.signalCommit(entry.Index)
	n.mu.Unlock()
	n.notifyReplicators()
	return nil
}

func (n *Node) ReadRPC(ctx context.Context, req *kvgrpc.ReadRequest) (*kvgrpc.ReadResponse, error) {
	entry, err := n.Get(ctx, req.Key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &kvgrpc.ReadResponse{Found: false}, nil
		}
		return nil, err
	}
	resp := &kvgrpc.ReadResponse{Found: true, Value: entry.Value, Deleted: entry.Deleted}
	if entry.ExpiresAt != nil {
		resp.ExpiresAt = *entry.ExpiresAt
	}
	return resp, nil
}

func (n *Node) ProposeRPC(ctx context.Context, req *kvgrpc.ProposeRequest) (*kvgrpc.ProposeResponse, error) {
	if req.Operation == "delete" {
		if err := n.Delete(ctx, req.Key); err != nil {
			return &kvgrpc.ProposeResponse{Success: false, LeaderID: n.LeaderID(), Error: err.Error()}, nil
		}
	} else {
		if err := n.Write(ctx, req.Key, req.Value, time.Duration(req.TTLSeconds)*time.Second); err != nil {
			return &kvgrpc.ProposeResponse{Success: false, LeaderID: n.LeaderID(), Error: err.Error()}, nil
		}
	}
	return &kvgrpc.ProposeResponse{Success: true, LeaderID: n.cfg.NodeID}, nil
}

func (n *Node) HealthRPC(ctx context.Context, req *struct{}) (*kvgrpc.HealthResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return &kvgrpc.HealthResponse{OK: true, NodeID: n.cfg.NodeID, Role: n.state.String(), LeaderID: n.leaderID, Term: uint64(n.currentTerm)}, nil
}

func (n *Node) Propose(ctx context.Context, req *kvgrpc.ProposeRequest) (*kvgrpc.ProposeResponse, error) {
	if req.Operation == "delete" {
		if err := n.Delete(ctx, req.Key); err != nil {
			return &kvgrpc.ProposeResponse{Success: false, LeaderID: n.LeaderID(), Error: err.Error()}, nil
		}
	} else {
		if err := n.Write(ctx, req.Key, req.Value, time.Duration(req.TTLSeconds)*time.Second); err != nil {
			return &kvgrpc.ProposeResponse{Success: false, LeaderID: n.LeaderID(), Error: err.Error()}, nil
		}
	}
	return &kvgrpc.ProposeResponse{Success: true, LeaderID: n.cfg.NodeID}, nil
}

func (n *Node) Read(ctx context.Context, req *kvgrpc.ReadRequest) (*kvgrpc.ReadResponse, error) {
	return n.ReadRPC(ctx, req)
}

func (n *Node) Health(ctx context.Context, req *struct{}) (*kvgrpc.HealthResponse, error) {
	return n.HealthRPC(ctx, req)
}
