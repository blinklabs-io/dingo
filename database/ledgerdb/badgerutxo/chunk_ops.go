// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package badgerutxo

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/v4"
)

// Entries one reward block op writes: a row and its journal entry for each
// row it touches, which is four rows for a counted block, three for an
// opened epoch and one for fees. An overlay block counts in two rows only.
const (
	countBlockEntries   = 8
	overlayBlockEntries = 4
	openEpochEntries    = 6
	feeEntries          = 2
	snapshotPutEntries  = 3
	// A deleted snapshot row also deletes its capture index entry.
	snapshotDeleteEntries = 2
	rewardDeleteEntries   = 1
	// deleteBytes is the estimate for one deleted key, which carries no
	// value.
	deleteBytes = 128
)

func isSnapshotDelete(op SnapshotOp) bool {
	return op.Kind == SnapshotDeleteEpochType ||
		op.Kind == SnapshotDeleteBefore
}

func isRewardPrune(op RewardBlockOp) bool { return op.PruneBelow > 0 }

// snapshotMatcher returns the prefix a delete op scans and the filter that
// selects its rows.
func snapshotMatcher(op SnapshotOp) ([]byte, func(k []byte) bool) {
	if op.Kind == SnapshotDeleteEpochType {
		return snapshotTypePrefix(op.Row.Epoch, op.Row.Type),
			func([]byte) bool { return true }
	}
	return []byte{SnapshotTag}, func(k []byte) bool {
		return binary.BigEndian.Uint64(k[1:9]) < op.Row.Epoch
	}
}

func rewardBlockOpCost(op RewardBlockOp) cost {
	var entries int64
	switch {
	case op.Open:
		entries = openEpochEntries
	case len(op.Pool) > 0 && op.Overlay:
		entries = overlayBlockEntries
	case len(op.Pool) > 0:
		entries = countBlockEntries
	case op.Fee > 0:
		entries = feeEntries
	}
	return cost{entries: entries, bytes: entries / 2 * rowBytes}
}

func snapshotPutCost() cost {
	return cost{entries: snapshotPutEntries, bytes: rowBytes}
}

// bulkCost is the weight of deleting n keys at perKey entries each.
func bulkCost(n, perKey int64) cost {
	return cost{entries: n * perKey, bytes: n * deleteBytes}
}

// opsCost estimates the snapshot and reward block ops of a batch. A delete
// is as heavy as the rows it finds in the store, which is why the estimate
// reads it.
func (s *Store) opsCost(
	snaps []SnapshotOp,
	blocks []RewardBlockOp,
) (cost, error) {
	var c cost
	var scans []func(txn *badger.Txn) error
	for _, op := range snaps {
		if !isSnapshotDelete(op) {
			c = c.add(snapshotPutCost())
			continue
		}
		prefix, keep := snapshotMatcher(op)
		scans = append(scans, func(txn *badger.Txn) error {
			c = c.add(
				bulkCost(countKeys(txn, prefix, keep), snapshotDeleteEntries),
			)
			return nil
		})
	}
	for _, op := range blocks {
		if !isRewardPrune(op) {
			c = c.add(rewardBlockOpCost(op))
			continue
		}
		below := op.PruneBelow
		scans = append(scans, func(txn *badger.Txn) error {
			c = c.add(bulkCost(countKeys(txn, []byte{RewardBlocksTag},
				func(k []byte) bool {
					return len(k) >= 9 &&
						binary.BigEndian.Uint64(k[1:9]) < below
				}), rewardDeleteEntries))
			return nil
		})
	}
	if len(scans) == 0 {
		return c, nil
	}
	err := s.view(func(txn *badger.Txn) error {
		for _, scan := range scans {
			if err := scan(txn); err != nil {
				return err
			}
		}
		return nil
	})
	return c, err
}

func countKeys(
	txn *badger.Txn,
	prefix []byte,
	keep func(k []byte) bool,
) int64 {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	var n int64
	for it.Rewind(); it.Valid(); it.Next() {
		if keep(it.Item().Key()) {
			n++
		}
	}
	return n
}

// takeSnapshotOps returns how many of ops, from the front and none of them a
// delete, a transaction takes within the budget, at least one.
func takeSnapshotOps(ops []SnapshotOp, entries, bytes int64) int {
	var c cost
	n := 0
	for _, op := range ops {
		if isSnapshotDelete(op) {
			break
		}
		if n > 0 && !c.add(snapshotPutCost()).fits(entries, bytes) {
			break
		}
		c = c.add(snapshotPutCost())
		n++
	}
	return n
}

// takeRewardBlockOps is takeSnapshotOps for reward block ops, with a prune
// as the delete.
func takeRewardBlockOps(ops []RewardBlockOp, entries, bytes int64) int {
	var c cost
	n := 0
	for _, op := range ops {
		if isRewardPrune(op) {
			break
		}
		if n > 0 && !c.add(rewardBlockOpCost(op)).fits(entries, bytes) {
			break
		}
		c = c.add(rewardBlockOpCost(op))
		n++
	}
	return n
}

// deleteKeysLimited deletes up to limit of the keys under prefix that keep
// selects, and reports whether more are left.
func deleteKeysLimited(
	txn *badger.Txn,
	prefix []byte,
	keep func(k []byte) bool,
	limit int64,
	del func(txn *badger.Txn, keys [][]byte) error,
) (bool, error) {
	var keys [][]byte
	more := false
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	for it.Rewind(); it.Valid(); it.Next() {
		k := it.Item().Key()
		if !keep(k) {
			continue
		}
		if int64(len(keys)) >= limit {
			more = true
			break
		}
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	it.Close()
	return more, del(txn, keys)
}

// deleteSnapshotLimited applies a snapshot delete op to at most limit rows.
func deleteSnapshotLimited(
	txn *badger.Txn,
	op SnapshotOp,
	limit int64,
) (bool, error) {
	prefix, keep := snapshotMatcher(op)
	return deleteKeysLimited(txn, prefix, keep, limit, deleteKeys)
}

// pruneRewardBlocksLimited is pruneRewardBlocks for at most limit rows.
func pruneRewardBlocksLimited(
	txn *badger.Txn,
	below uint64,
	limit int64,
) (bool, error) {
	return deleteKeysLimited(txn, []byte{RewardBlocksTag},
		func(k []byte) bool {
			return len(k) >= 9 && binary.BigEndian.Uint64(k[1:9]) < below
		}, limit, deleteKeys)
}
