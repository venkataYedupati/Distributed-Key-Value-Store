package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"net/http"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"distributed-kv-store/internal/api"
	"distributed-kv-store/internal/config"
	kvgrpc "distributed-kv-store/internal/grpc"
	"distributed-kv-store/internal/hash"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	var (
		nodeID     = flag.String("node-id", envOrDefault("NODE_ID", "node1"), "node identifier")
		host       = flag.String("host", envOrDefault("NODE_ADDR", "node1:8081"), "advertise host:port")
		httpPort   = flag.Int("http-port", envInt("HTTP_PORT", 8081), "HTTP port")
		grpcPort   = flag.Int("grpc-port", envInt("GRPC_PORT", 9091), "gRPC peer port")
		metricsPort = flag.Int("metrics-port", envInt("METRICS_PORT", 2112), "Prometheus metrics port")
		dataDir    = flag.String("data-dir", envOrDefault("DATA_DIR", filepath.Join("data", "node1")), "data directory")
		bindHost   = flag.String("bind-host", envOrDefault("BIND_HOST", "0.0.0.0"), "bind host")
		peersInput = flag.String("peers", envOrDefault("PEER_ADDRS", ""), "comma-separated peer definitions host:httpPort")
	)
	flag.Parse()
	advertiseHost, advertiseHTTPPort := splitHostPort(*host, *httpPort)

	cfg := config.Config{
		NodeID:            *nodeID,
		DataDir:           *dataDir,
		Host:              advertiseHost,
		BindHost:          *bindHost,
		HTTPPort:          *httpPort,
		GRPCPort:          *grpcPort,
		MetricsPort:       *metricsPort,
		VNodes:            150,
		ReplicationFactor: 3,
		SnapshotThreshold: 1000,
		ElectionTimeout:   1500 * time.Millisecond,
		HeartbeatInterval: 350 * time.Millisecond,
		Peers:             config.ParsePeerEndpoints(*peersInput),
		ClusterName:       "distributed-kv-store",
	}
	if advertiseHTTPPort > 0 {
		cfg.HTTPPort = advertiseHTTPPort
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	engine, err := store.NewEngine(cfg.DataDir, cfg.SnapshotThreshold)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer engine.Close()

	metricsServer := &http.Server{Addr: cfg.MetricsAddr(), Handler: promhttp.Handler()}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server: %v", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}()

	ring := hash.NewRing(cfg.VNodes, cfg.ReplicationFactor)
	for _, peer := range cfg.AllPeers() {
		ring.AddNode(hash.PhysicalNode{
			ID:       peer.ID,
			HTTPAddr: peer.HTTPAddr(),
			GRPCAddr: peer.GRPCAddr(),
		})
	}

	peerClient, err := kvgrpc.NewClient(cfg)
	if err != nil {
		log.Fatalf("create peer client: %v", err)
	}
	defer peerClient.Close()

	node := raft.NewNode(cfg, engine, ring, peerClient)
	defer node.Close()
	if err := node.Start(ctx); err != nil {
		log.Fatalf("start raft node: %v", err)
	}

	handlers := api.NewHandlers(cfg, node, engine, ring)
	server := api.NewServer(cfg.BindHTTPAddr(), handlers)

	fmt.Printf("node %s listening on %s and %s\n", cfg.NodeID, cfg.HTTPAddr(), cfg.GRPCAddr())
	if err := server.Start(ctx); err != nil {
		log.Fatalf("start http server: %v", err)
	}
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		var parsed int
		if _, err := fmt.Sscanf(value, "%d", &parsed); err == nil {
			return parsed
		}
	}
	return fallback
}

func splitHostPort(value string, fallbackPort int) (string, int) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return value, fallbackPort
	}
	var port int
	if _, err := fmt.Sscanf(parts[1], "%d", &port); err != nil {
		port = fallbackPort
	}
	return parts[0], port
}
