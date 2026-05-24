package config

import (
	"fmt"
	"strings"
	"time"
)

type PeerConfig struct {
	ID       string
	Host     string
	HTTPPort int
	GRPCPort int
}

func (p PeerConfig) HTTPAddr() string {
	return fmt.Sprintf("%s:%d", p.Host, p.HTTPPort)
}

func (p PeerConfig) GRPCAddr() string {
	return fmt.Sprintf("%s:%d", p.Host, p.GRPCPort)
}

type Config struct {
	NodeID            string
	DataDir           string
	HTTPPort          int
	GRPCPort          int
	MetricsPort       int
	Host              string
	BindHost          string
	VNodes            int
	ReplicationFactor int
	SnapshotThreshold int
	ElectionTimeout   time.Duration
	HeartbeatInterval  time.Duration
	Peers             []PeerConfig
	LeaderHint        string
	ClusterName       string
}

func (c Config) HTTPAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.HTTPPort)
}

func (c Config) GRPCAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.GRPCPort)
}

func (c Config) BindHTTPAddr() string {
	host := c.BindHost
	if host == "" {
		host = c.Host
	}
	return fmt.Sprintf("%s:%d", host, c.HTTPPort)
}

func (c Config) BindGRPCAddr() string {
	host := c.BindHost
	if host == "" {
		host = c.Host
	}
	return fmt.Sprintf("%s:%d", host, c.GRPCPort)
}

func (c Config) MetricsAddr() string {
	host := c.BindHost
	if host == "" {
		host = c.Host
	}
	return fmt.Sprintf("%s:%d", host, c.MetricsPort)
}

func (c Config) SelfPeer() PeerConfig {
	return PeerConfig{ID: c.NodeID, Host: c.Host, HTTPPort: c.HTTPPort, GRPCPort: c.GRPCPort}
}

func (c Config) PeerByID(id string) (PeerConfig, bool) {
	for _, peer := range c.Peers {
		if peer.ID == id {
			return peer, true
		}
	}
	return PeerConfig{}, false
}

func (c Config) AllPeers() []PeerConfig {
	peers := make([]PeerConfig, 0, len(c.Peers)+1)
	peers = append(peers, c.SelfPeer())
	peers = append(peers, c.Peers...)
	return peers
}

func ParsePeers(input string) []PeerConfig {
	if strings.TrimSpace(input) == "" {
		return nil
	}
	parts := strings.Split(input, ",")
	peers := make([]PeerConfig, 0, len(parts))
	for _, part := range parts {
		fields := strings.Split(part, ":")
		if len(fields) != 4 {
			continue
		}
		peers = append(peers, PeerConfig{ID: fields[0], Host: fields[1], HTTPPort: mustAtoi(fields[2]), GRPCPort: mustAtoi(fields[3])})
	}
	return peers
}

func ParsePeerEndpoints(input string) []PeerConfig {
	if strings.TrimSpace(input) == "" {
		return nil
	}
	parts := strings.Split(input, ",")
	peers := make([]PeerConfig, 0, len(parts))
	for _, part := range parts {
		fields := strings.Split(strings.TrimSpace(part), ":")
		if len(fields) != 2 {
			continue
		}
		host := fields[0]
		httpPort := mustAtoi(fields[1])
		peers = append(peers, PeerConfig{ID: host, Host: host, HTTPPort: httpPort, GRPCPort: httpPort + 1000})
	}
	return peers
}

func mustAtoi(value string) int {
	var parsed int
	_, _ = fmt.Sscanf(value, "%d", &parsed)
	return parsed
}
