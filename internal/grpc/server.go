package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	kvproto "distributed-kv-store/proto/kv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Backend interface {
	Get(ctx context.Context, req *kvproto.GetRequest) (*kvproto.GetResponse, error)
	Set(ctx context.Context, req *kvproto.SetRequest) (*kvproto.SetResponse, error)
	Delete(ctx context.Context, req *kvproto.DeleteRequest) (*kvproto.DeleteResponse, error)
	Forward(ctx context.Context, req *kvproto.ForwardRequest) (*kvproto.ForwardResponse, error)
	RequestVote(ctx context.Context, req *kvproto.VoteRequest) (*kvproto.VoteResponse, error)
	AppendEntries(ctx context.Context, req *kvproto.AppendEntriesRequest) (*kvproto.AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, req *kvproto.SnapshotRequest) (*kvproto.SnapshotResponse, error)
}

type Server struct {
	addr       string
	backend    Backend
	grpcServer *grpc.Server
	once       sync.Once
	listener   net.Listener
}

type kvService struct {
	kvproto.UnimplementedKVServiceServer
	backend Backend
}

type raftService struct {
	kvproto.UnimplementedRaftServiceServer
	backend Backend
}

func DefaultGRPCPort(nodeIndex int) int {
	return 9090 + nodeIndex
}

func DefaultGRPCAddr(host string, nodeIndex int) string {
	return fmt.Sprintf("%s:%d", host, DefaultGRPCPort(nodeIndex))
}

func NewServer(addr string, backend Backend) *Server {
	grpcServer := grpc.NewServer()
	server := &Server{addr: addr, backend: backend, grpcServer: grpcServer}
	kvproto.RegisterKVServiceServer(grpcServer, &kvService{backend: backend})
	kvproto.RegisterRaftServiceServer(grpcServer, &raftService{backend: backend})
	reflection.Register(grpcServer)
	return server
}

func NewServerForNode(host string, nodeIndex int, backend Backend) *Server {
	return NewServer(DefaultGRPCAddr(host, nodeIndex), backend)
}

func (s *Server) Addr() string {
	return s.addr
}

func (s *Server) Serve(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = lis
	go func() {
		<-ctx.Done()
		s.Stop()
	}()
	return s.grpcServer.Serve(lis)
}

func (s *Server) Stop() {
	s.once.Do(func() {
		if s.grpcServer != nil {
			s.grpcServer.GracefulStop()
		}
		if s.listener != nil {
			_ = s.listener.Close()
		}
	})
}

func (s *Server) GracefulStop(timeout time.Duration) {
	if timeout <= 0 {
		s.Stop()
		return
	}
	go func() {
		time.Sleep(timeout)
		s.Stop()
	}()
}

func (s *kvService) Get(ctx context.Context, req *kvproto.GetRequest) (*kvproto.GetResponse, error) {
	if s.backend == nil {
		return &kvproto.GetResponse{Found: false, Error: "backend unavailable"}, nil
	}
	return s.backend.Get(ctx, req)
}

func (s *kvService) Set(ctx context.Context, req *kvproto.SetRequest) (*kvproto.SetResponse, error) {
	if s.backend == nil {
		return &kvproto.SetResponse{Success: false, Error: "backend unavailable"}, nil
	}
	return s.backend.Set(ctx, req)
}

func (s *kvService) Delete(ctx context.Context, req *kvproto.DeleteRequest) (*kvproto.DeleteResponse, error) {
	if s.backend == nil {
		return &kvproto.DeleteResponse{Success: false, Error: "backend unavailable"}, nil
	}
	return s.backend.Delete(ctx, req)
}

func (s *kvService) Forward(ctx context.Context, req *kvproto.ForwardRequest) (*kvproto.ForwardResponse, error) {
	if s.backend == nil {
		return &kvproto.ForwardResponse{Success: false, Error: "backend unavailable"}, nil
	}
	return s.backend.Forward(ctx, req)
}

func (s *raftService) RequestVote(ctx context.Context, req *kvproto.VoteRequest) (*kvproto.VoteResponse, error) {
	if s.backend == nil {
		return &kvproto.VoteResponse{Term: req.Term, VoteGranted: false}, nil
	}
	return s.backend.RequestVote(ctx, req)
}

func (s *raftService) AppendEntries(ctx context.Context, req *kvproto.AppendEntriesRequest) (*kvproto.AppendEntriesResponse, error) {
	if s.backend == nil {
		return &kvproto.AppendEntriesResponse{Term: req.Term, Success: false}, nil
	}
	return s.backend.AppendEntries(ctx, req)
}

func (s *raftService) InstallSnapshot(ctx context.Context, req *kvproto.SnapshotRequest) (*kvproto.SnapshotResponse, error) {
	if s.backend == nil {
		return &kvproto.SnapshotResponse{Term: req.Term}, nil
	}
	return s.backend.InstallSnapshot(ctx, req)
}
