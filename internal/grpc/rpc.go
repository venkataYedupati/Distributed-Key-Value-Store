package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

type jsonCodec struct{}

func (jsonCodec) Name() string { return "json" }

func (jsonCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

var codec = jsonCodec{}

func init() {
	encoding.RegisterCodec(codec)
}

func JSONCodec() encoding.Codec { return codec }

type LogEntry struct {
	Index      uint64    `json:"index"`
	Term       uint64    `json:"term"`
	Key        string    `json:"key"`
	Value      string    `json:"value"`
	Operation  string    `json:"operation"`
	TTLSeconds int64     `json:"ttl_seconds"`
	Timestamp  time.Time `json:"timestamp"`
}

type HeartbeatRequest struct {
	Term            uint64    `json:"term"`
	LeaderID        string    `json:"leader_id"`
	LeaderHTTPAddr  string    `json:"leader_http_addr"`
	LeaderGRPCAddr  string    `json:"leader_grpc_addr"`
	CommitIndex     uint64    `json:"commit_index"`
	Timestamp       time.Time `json:"timestamp"`
}

type HeartbeatResponse struct {
	Term     uint64 `json:"term"`
	Accepted bool   `json:"accepted"`
}

type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID   string `json:"candidate_id"`
	LastLogIndex  uint64 `json:"last_log_index"`
	LastLogTerm   uint64 `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	MatchIndex    uint64 `json:"match_index"`
	ConflictIndex  uint64 `json:"conflict_index"`
	ConflictTerm   uint64 `json:"conflict_term"`
}

type ProposeRequest struct {
	Key        string `json:"key"`
	Value      string `json:"value"`
	Operation  string `json:"operation"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

type ProposeResponse struct {
	Success   bool   `json:"success"`
	LeaderID  string `json:"leader_id"`
	Error     string `json:"error,omitempty"`
	Index     uint64 `json:"index"`
	Term      uint64 `json:"term"`
}

type ReadRequest struct {
	Key string `json:"key"`
}

type ReadResponse struct {
	Found     bool      `json:"found"`
	Value     string    `json:"value,omitempty"`
	Deleted   bool      `json:"deleted,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

type HealthResponse struct {
	OK       bool   `json:"ok"`
	NodeID   string `json:"node_id"`
	Role     string `json:"role"`
	LeaderID string `json:"leader_id,omitempty"`
	Term     uint64 `json:"term"`
}

type PeerServiceClient interface {
	Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error)
	RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error)
	AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error)
	Propose(ctx context.Context, in *ProposeRequest, opts ...grpc.CallOption) (*ProposeResponse, error)
	Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (*ReadResponse, error)
	Health(ctx context.Context, in *struct{}, opts ...grpc.CallOption) (*HealthResponse, error)
}

type peerServiceClient struct {
	cc *grpc.ClientConn
}

func NewPeerServiceClient(cc *grpc.ClientConn) PeerServiceClient {
	return &peerServiceClient{cc: cc}
}

func (c *peerServiceClient) Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error) {
	out := new(HeartbeatResponse)
	if err := c.cc.Invoke(ctx, "/distributedkv.PeerService/Heartbeat", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *peerServiceClient) RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error) {
	out := new(RequestVoteResponse)
	if err := c.cc.Invoke(ctx, "/distributedkv.PeerService/RequestVote", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *peerServiceClient) AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error) {
	out := new(AppendEntriesResponse)
	if err := c.cc.Invoke(ctx, "/distributedkv.PeerService/AppendEntries", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *peerServiceClient) Propose(ctx context.Context, in *ProposeRequest, opts ...grpc.CallOption) (*ProposeResponse, error) {
	out := new(ProposeResponse)
	if err := c.cc.Invoke(ctx, "/distributedkv.PeerService/Propose", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *peerServiceClient) Read(ctx context.Context, in *ReadRequest, opts ...grpc.CallOption) (*ReadResponse, error) {
	out := new(ReadResponse)
	if err := c.cc.Invoke(ctx, "/distributedkv.PeerService/Read", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *peerServiceClient) Health(ctx context.Context, in *struct{}, opts ...grpc.CallOption) (*HealthResponse, error) {
	out := new(HealthResponse)
	if err := c.cc.Invoke(ctx, "/distributedkv.PeerService/Health", in, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

type PeerServiceServer interface {
	Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error)
	Propose(context.Context, *ProposeRequest) (*ProposeResponse, error)
	Read(context.Context, *ReadRequest) (*ReadResponse, error)
	Health(context.Context, *struct{}) (*HealthResponse, error)
}

func RegisterPeerServiceServer(s *grpc.Server, srv PeerServiceServer) {
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "distributedkv.PeerService",
		HandlerType: (*PeerServiceServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "Heartbeat", Handler: heartbeatHandler},
			{MethodName: "RequestVote", Handler: requestVoteHandler},
			{MethodName: "AppendEntries", Handler: appendEntriesHandler},
			{MethodName: "Propose", Handler: proposeHandler},
			{MethodName: "Read", Handler: readHandler},
			{MethodName: "Health", Handler: healthHandler},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "kv.proto",
	}, srv)
}

func heartbeatHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(HeartbeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PeerServiceServer).Heartbeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/distributedkv.PeerService/Heartbeat"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(PeerServiceServer).Heartbeat(ctx, req.(*HeartbeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func requestVoteHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(RequestVoteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PeerServiceServer).RequestVote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/distributedkv.PeerService/RequestVote"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(PeerServiceServer).RequestVote(ctx, req.(*RequestVoteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func appendEntriesHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(AppendEntriesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PeerServiceServer).AppendEntries(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/distributedkv.PeerService/AppendEntries"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(PeerServiceServer).AppendEntries(ctx, req.(*AppendEntriesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func proposeHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(ProposeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PeerServiceServer).Propose(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/distributedkv.PeerService/Propose"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(PeerServiceServer).Propose(ctx, req.(*ProposeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func readHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(ReadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PeerServiceServer).Read(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/distributedkv.PeerService/Read"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(PeerServiceServer).Read(ctx, req.(*ReadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func healthHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(struct{})
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PeerServiceServer).Health(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/distributedkv.PeerService/Health"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(PeerServiceServer).Health(ctx, req.(*struct{}))
	}
	return interceptor(ctx, in, info, handler)
}

func Serve(ctx context.Context, addr string, srv PeerServiceServer) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer(grpc.ForceServerCodec(codec))
	RegisterPeerServiceServer(server, srv)
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	go func() {
		_ = server.Serve(lis)
	}()
	return server, nil
}

func Dial(addr string) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(codec)))
}

func DialContext(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(codec)))
}

func MustDial(addr string) *grpc.ClientConn {
	conn, err := Dial(addr)
	if err != nil {
		panic(fmt.Sprintf("dial %s: %v", addr, err))
	}
	return conn
}
