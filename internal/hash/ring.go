package hash

import (
	"sort"
	"strconv"
	"sync"
)

type PhysicalNode struct {
	ID       string `json:"id"`
	HTTPAddr string `json:"http_addr"`
	GRPCAddr string `json:"grpc_addr"`
}

type Ring struct {
	mu               sync.RWMutex
	vnodes           int
	replicationFactor int
	positions        map[uint32]PhysicalNode
	sortedHashes     []uint32
	nodes            map[string]PhysicalNode
}

type RingStats struct {
	Nodes             int               `json:"nodes"`
	VirtualNodes      int               `json:"virtual_nodes"`
	ReplicationFactor  int               `json:"replication_factor"`
	Distribution      map[string]int    `json:"distribution"`
}

func NewRing(vnodes, replicationFactor int) *Ring {
	return &Ring{
		vnodes:           vnodes,
		replicationFactor: replicationFactor,
		positions:        make(map[uint32]PhysicalNode),
		nodes:            make(map[string]PhysicalNode),
	}
}

func (r *Ring) AddNode(node PhysicalNode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[node.ID] = node
	for i := 0; i < r.vnodes; i++ {
		h := hashString(node.ID + "#" + strconv.Itoa(i))
		r.positions[h] = node
		r.sortedHashes = append(r.sortedHashes, h)
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool { return r.sortedHashes[i] < r.sortedHashes[j] })
}

func (r *Ring) RemoveNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, id)
	filtered := r.sortedHashes[:0]
	for _, hashValue := range r.sortedHashes {
		if r.positions[hashValue].ID == id {
			delete(r.positions, hashValue)
			continue
		}
		filtered = append(filtered, hashValue)
	}
	r.sortedHashes = filtered
}

func (r *Ring) GetNode(key string) (PhysicalNode, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sortedHashes) == 0 {
		return PhysicalNode{}, false
	}
	h := hashString(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool { return r.sortedHashes[i] >= h })
	if idx == len(r.sortedHashes) {
		idx = 0
	}
	return r.positions[r.sortedHashes[idx]], true
}

func (r *Ring) GetReplicaSet(key string) []PhysicalNode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sortedHashes) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	replicas := make([]PhysicalNode, 0, r.replicationFactor)
	h := hashString(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool { return r.sortedHashes[i] >= h })
	if idx == len(r.sortedHashes) {
		idx = 0
	}
	for i := 0; i < len(r.sortedHashes) && len(replicas) < r.replicationFactor; i++ {
		candidate := r.positions[r.sortedHashes[(idx+i)%len(r.sortedHashes)]]
		if _, ok := seen[candidate.ID]; ok {
			continue
		}
		seen[candidate.ID] = struct{}{}
		replicas = append(replicas, candidate)
	}
	return replicas
}

func (r *Ring) Stats() RingStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	distribution := make(map[string]int, len(r.nodes))
	for _, node := range r.nodes {
		distribution[node.ID] = 0
	}
	for _, pos := range r.sortedHashes {
		distribution[r.positions[pos].ID]++
	}
	return RingStats{
		Nodes:            len(r.nodes),
		VirtualNodes:     len(r.sortedHashes),
		ReplicationFactor: r.replicationFactor,
		Distribution:     distribution,
	}
}

func (r *Ring) Nodes() []PhysicalNode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make([]PhysicalNode, 0, len(r.nodes))
	for _, node := range r.nodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

func hashString(value string) uint32 {
	var hash uint32 = 2166136261
	for i := 0; i < len(value); i++ {
		hash ^= uint32(value[i])
		hash *= 16777619
	}
	return hash
}
