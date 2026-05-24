package raft

import "fmt"

func (n *Node) snapshotBytes() ([]byte, int64, int64, error) {
	if n.engine == nil {
		return nil, 0, 0, fmt.Errorf("engine unavailable")
	}
	n.mu.RLock()
	lastIncludedIndex := n.lastApplied
	lastIncludedTerm := n.snapshotTerm
	if lastIncludedIndex == 0 {
		lastIncludedIndex = n.commitIndex
	}
	if lastIncludedIndex > 0 && n.logStore != nil {
		if entry, err := n.logStore.Get(lastIncludedIndex); err == nil {
			lastIncludedTerm = entry.Term
		}
	}
	n.mu.RUnlock()
	data, err := n.engine.TakeSnapshot(lastIncludedIndex, lastIncludedTerm)
	if err != nil {
		return nil, 0, 0, err
	}
	return data, lastIncludedIndex, lastIncludedTerm, nil
}

func (n *Node) applySnapshotBytes(data []byte, lastIncludedIndex int64, lastIncludedTerm int64) error {
	if n.engine == nil {
		return fmt.Errorf("engine unavailable")
	}
	if err := n.engine.ApplySnapshot(data, lastIncludedIndex, lastIncludedTerm); err != nil {
		return err
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
	return nil
}
