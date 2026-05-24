package hash

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRingReplicaSetUsesUniqueNodes(t *testing.T) {
	ring := NewRing(5, 3)
	ring.AddNode(PhysicalNode{ID: "node1"})
	ring.AddNode(PhysicalNode{ID: "node2"})
	ring.AddNode(PhysicalNode{ID: "node3"})
	ring.AddNode(PhysicalNode{ID: "node4"})
	set := ring.GetReplicaSet("alpha")
	require.Len(t, set, 3)
	require.NotEqual(t, set[0].ID, set[1].ID)
	require.NotEqual(t, set[1].ID, set[2].ID)
}

func TestRingReturnsNodeForKey(t *testing.T) {
	ring := NewRing(10, 3)
	ring.AddNode(PhysicalNode{ID: "node1"})
	ring.AddNode(PhysicalNode{ID: "node2"})
	node, ok := ring.GetNode("alpha")
	require.True(t, ok)
	require.NotEmpty(t, node.ID)
}
