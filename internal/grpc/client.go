package grpc

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"distributed-kv-store/internal/config"
	kvproto "distributed-kv-store/proto/kv"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type Client struct {
	mu    sync.RWMutex
	peers map[string]config.PeerConfig
	conns map[string]*grpc.ClientConn
}

func NewClient(cfg config.Config) (*Client, error) {
	client := &Client{
		peers: make(map[string]config.PeerConfig, len(cfg.Peers)+1),
		conns: make(map[string]*grpc.ClientConn, len(cfg.Peers)+1),
	}
	client.peers[cfg.NodeID] = cfg.SelfPeer()
	for _, peer := range cfg.Peers {
		client.peers[peer.ID] = peer
	}
	return client, nil
}

func (c *Client) dialPeer(peerID string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[peerID]; ok {
		return conn, nil
	}
	peer, ok := c.peers[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer %s", peerID)
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		conn, err := grpc.Dial(peer.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			c.conns[peerID] = conn
			return conn, nil
		}
		lastErr = err
		if attempt == 0 {
			time.Sleep(jitterBackoff(100*time.Millisecond, 0.2, attempt))
		}
	}
	return nil, lastErr
}

func jitterBackoff(base time.Duration, jitter float64, attempt int) time.Duration {
	backoff := base << attempt
	if backoff > 2*time.Second {
		backoff = 2 * time.Second
	}
	spread := int64(float64(backoff) * jitter)
	if spread <= 0 {
		return backoff
	}
	offset := time.Duration(rand.Int63n(spread*2+1) - spread)
	return backoff + offset
}

func isTransientGRPCError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}

func withRetry[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		result, err := fn(callCtx)
		cancel()
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !isTransientGRPCError(err) || attempt == 1 {
			break
		}
		time.Sleep(jitterBackoff(100*time.Millisecond, 0.2, attempt))
	}
	return zero, lastErr
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for id, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close conn %s: %w", id, err)
		}
	}
	return firstErr
}

func (c *Client) connFor(peerID string) (*grpc.ClientConn, error) {
	return c.dialPeer(peerID)
}

func (c *Client) kvClient(peerID string) (kvproto.KVServiceClient, error) {
	conn, err := c.dialPeer(peerID)
	if err != nil {
		return nil, err
	}
	return kvproto.NewKVServiceClient(conn), nil
}

func (c *Client) raftClient(peerID string) (kvproto.RaftServiceClient, error) {
	conn, err := c.dialPeer(peerID)
	if err != nil {
		return nil, err
	}
	return kvproto.NewRaftServiceClient(conn), nil
}

func (c *Client) SendRequestVote(peerID string, req *kvproto.VoteRequest) (*kvproto.VoteResponse, error) {
	client, err := c.raftClient(peerID)
	if err != nil {
		return nil, err
	}
	return withRetry(context.Background(), func(ctx context.Context) (*kvproto.VoteResponse, error) {
		return client.RequestVote(ctx, req)
	})
}

func (c *Client) SendAppendEntries(peerID string, req *kvproto.AppendEntriesRequest) (*kvproto.AppendEntriesResponse, error) {
	client, err := c.raftClient(peerID)
	if err != nil {
		return nil, err
	}
	return withRetry(context.Background(), func(ctx context.Context) (*kvproto.AppendEntriesResponse, error) {
		return client.AppendEntries(ctx, req)
	})
}

func (c *Client) SendInstallSnapshot(peerID string, req *kvproto.SnapshotRequest) (*kvproto.SnapshotResponse, error) {
	client, err := c.raftClient(peerID)
	if err != nil {
		return nil, err
	}
	return withRetry(context.Background(), func(ctx context.Context) (*kvproto.SnapshotResponse, error) {
		return client.InstallSnapshot(ctx, req)
	})
}

func (c *Client) ForwardRequest(peerID string, req *kvproto.ForwardRequest) (*kvproto.ForwardResponse, error) {
	client, err := c.kvClient(peerID)
	if err != nil {
		return nil, err
	}
	return withRetry(context.Background(), func(ctx context.Context) (*kvproto.ForwardResponse, error) {
		return client.Forward(ctx, req)
	})
}

func (c *Client) Heartbeat(ctx context.Context, peerID string, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	conn, err := c.connFor(peerID)
	if err != nil {
		return nil, err
	}
	return NewPeerServiceClient(conn).Heartbeat(ctx, req)
}

func (c *Client) RequestVote(ctx context.Context, peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	conn, err := c.connFor(peerID)
	if err != nil {
		return nil, err
	}
	return NewPeerServiceClient(conn).RequestVote(ctx, req)
}

func (c *Client) AppendEntries(ctx context.Context, peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	conn, err := c.connFor(peerID)
	if err != nil {
		return nil, err
	}
	return NewPeerServiceClient(conn).AppendEntries(ctx, req)
}

func (c *Client) Propose(ctx context.Context, peerID string, req *ProposeRequest) (*ProposeResponse, error) {
	conn, err := c.connFor(peerID)
	if err != nil {
		return nil, err
	}
	return NewPeerServiceClient(conn).Propose(ctx, req)
}

func (c *Client) Read(ctx context.Context, peerID string, req *ReadRequest) (*ReadResponse, error) {
	conn, err := c.connFor(peerID)
	if err != nil {
		return nil, err
	}
	return NewPeerServiceClient(conn).Read(ctx, req)
}

func (c *Client) Health(ctx context.Context, peerID string) (*HealthResponse, error) {
	conn, err := c.connFor(peerID)
	if err != nil {
		return nil, err
	}
	return NewPeerServiceClient(conn).Health(ctx, &struct{}{})
}

func (c *Client) Peer(peerID string) (config.PeerConfig, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	peer, ok := c.peers[peerID]
	return peer, ok
}

func (c *Client) PeerIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]string, 0, len(c.peers))
	for id := range c.peers {
		ids = append(ids, id)
	}
	return ids
}
