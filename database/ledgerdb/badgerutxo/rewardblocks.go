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
	"errors"
	"fmt"
	"math"

	"github.com/dgraph-io/badger/v4"
)

// Reward block rows. Every row of an epoch shares the prefix
// E <epoch:8 BE>, so dropping an epoch is one prefix delete:
//
//	E <epoch> 0 <pool:28>   blocks the pool minted in the epoch, 8 BE
//	E <epoch> 1             blocks counted in the epoch, then the first slot
//	                        a block may still be counted at, 8 BE each
//	E <epoch> 2             fees the epoch collected, 8 BE
//	E <epoch> 3 <pool:28>   blocks the pool minted, overlay slots included
//	E <epoch> 4             the same total and next slot, overlay included
//
// The total row is present exactly when the epoch's counts are known, and
// the fees row when its fees are, so an absent row is "unknown" rather than
// zero. A counted block raises the total and its pool's count and moves the
// first countable slot past the block's slot, which makes counting a block
// twice a no-op. rewardBlocksKey is set once the rows are loaded.
const (
	rewardBlockCountKind byte = 0
	rewardBlockTotalKind byte = 1
	rewardBlockFeesKind  byte = 2
	// The all-slot kinds count a block in an overlay slot too, which the
	// epoch-boundary capture does and the reward round does not.
	rewardBlockAllCountKind byte = 3
	rewardBlockAllTotalKind byte = 4
)

// RewardBlocksEpochPrefix is the prefix of every reward block row of epoch.
func RewardBlocksEpochPrefix(epoch uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{RewardBlocksTag}, epoch)
}

// RewardBlockCountKey keys the blocks pool minted in epoch.
func RewardBlockCountKey(epoch uint64, pool []byte) []byte {
	return append(
		append(RewardBlocksEpochPrefix(epoch), rewardBlockCountKind),
		pool...,
	)
}

// RewardBlockTotalKey keys the blocks counted in epoch.
func RewardBlockTotalKey(epoch uint64) []byte {
	return append(RewardBlocksEpochPrefix(epoch), rewardBlockTotalKind)
}

// RewardBlockAllCountKey keys the blocks pool minted in epoch, overlay slots
// included.
func RewardBlockAllCountKey(epoch uint64, pool []byte) []byte {
	return append(
		append(RewardBlocksEpochPrefix(epoch), rewardBlockAllCountKind),
		pool...,
	)
}

// RewardBlockAllTotalKey keys the blocks counted in epoch, overlay slots
// included.
func RewardBlockAllTotalKey(epoch uint64) []byte {
	return append(RewardBlocksEpochPrefix(epoch), rewardBlockAllTotalKind)
}

// RewardBlockFeesKey keys the fees epoch collected.
func RewardBlockFeesKey(epoch uint64) []byte {
	return append(RewardBlocksEpochPrefix(epoch), rewardBlockFeesKind)
}

// RewardBlockRowKind names what a reward block row holds.
type RewardBlockRowKind uint8

// The reward block row kinds.
const (
	RewardBlockPoolCount RewardBlockRowKind = iota
	RewardBlockEpochTotal
	RewardBlockEpochFees
	RewardBlockAllPoolCount
	RewardBlockAllEpochTotal
)

// DecodeRewardBlockKey splits a reward block row key into its epoch, kind
// and, for a pool count, the pool.
func DecodeRewardBlockKey(
	k []byte,
) (epoch uint64, kind RewardBlockRowKind, pool []byte, err error) {
	if len(k) < 10 || k[0] != RewardBlocksTag {
		return 0, 0, nil, errors.New("badgerutxo: malformed reward block key")
	}
	epoch = binary.BigEndian.Uint64(k[1:9])
	switch k[9] {
	case rewardBlockCountKind:
		return epoch, RewardBlockPoolCount, k[10:], nil
	case rewardBlockTotalKind:
		return epoch, RewardBlockEpochTotal, nil, nil
	case rewardBlockFeesKind:
		return epoch, RewardBlockEpochFees, nil, nil
	case rewardBlockAllCountKind:
		return epoch, RewardBlockAllPoolCount, k[10:], nil
	case rewardBlockAllTotalKind:
		return epoch, RewardBlockAllEpochTotal, nil, nil
	}
	return 0, 0, nil, fmt.Errorf(
		"badgerutxo: unknown reward block row kind %d", k[9],
	)
}

// EncodeRewardBlockCount encodes a pool count or the fees value.
func EncodeRewardBlockCount(n uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, n)
}

// DecodeRewardBlockCount decodes a pool count or the fees value.
func DecodeRewardBlockCount(v []byte) (uint64, error) {
	if len(v) != 8 {
		return 0, errors.New("badgerutxo: malformed reward block count")
	}
	return binary.BigEndian.Uint64(v), nil
}

// EncodeRewardBlockTotal encodes an epoch's total and the first slot a
// block may still be counted at.
func EncodeRewardBlockTotal(total, next uint64) []byte {
	return binary.BigEndian.AppendUint64(
		binary.BigEndian.AppendUint64(nil, total), next,
	)
}

// DecodeRewardBlockTotal decodes an epoch total row.
func DecodeRewardBlockTotal(v []byte) (total, next uint64, err error) {
	if len(v) != 16 {
		return 0, 0, errors.New("badgerutxo: malformed reward block total")
	}
	return binary.BigEndian.Uint64(v), binary.BigEndian.Uint64(v[8:]), nil
}

// RewardBlockOp is one change to the reward block rows, applied in order
// inside the transaction that holds the batch, so it reads the rows as a
// rollback or an earlier op of the same batch left them.
type RewardBlockOp struct {
	// Slot is the slot of the commit the change belongs to, which a
	// rollback past it undoes. It need not lie in Epoch: a closure
	// transaction's fee belongs to the epoch of its context slot and is
	// undone with the block that certified it.
	Slot  uint64
	Epoch uint64
	// Open makes Epoch's total and fees known and zero where it has no
	// rows; an epoch that already has them is unchanged.
	Open bool
	// Pool counts one block it minted at Slot in Epoch. The block is
	// ignored when the epoch's total is unknown or Slot is below the first
	// slot the epoch may still count.
	Pool []byte
	// Overlay marks Pool's block as one in a slot TPraos reserves for the
	// overlay schedule: it counts in the all-slot rows only.
	Overlay bool
	// Fee adds to Epoch's fees. It is ignored when they are unknown.
	Fee uint64
	// PruneBelow, when non-zero, deletes every row of every epoch below it,
	// without a journal entry: a rollback does not bring them back.
	PruneBelow uint64
}

func putRewardBlocks(txn *badger.Txn, ops []RewardBlockOp) error {
	for _, op := range ops {
		var err error
		switch {
		case op.PruneBelow > 0:
			err = pruneRewardBlocks(txn, op.PruneBelow)
		case op.Open:
			err = openRewardBlockEpoch(txn, op)
		case len(op.Pool) > 0:
			err = countRewardBlock(txn, op)
		case op.Fee > 0:
			err = addRewardBlockFees(txn, op)
		}
		if err != nil {
			return fmt.Errorf(
				"badgerutxo: reward block change at slot %d: %w", op.Slot, err,
			)
		}
	}
	return nil
}

func putJournaled(txn *badger.Txn, slot uint64, k, v []byte) error {
	if err := journalRow(txn, slot, k); err != nil {
		return err
	}
	return txn.Set(k, v)
}

func openRewardBlockEpoch(txn *badger.Txn, op RewardBlockOp) error {
	for _, row := range []struct{ k, v []byte }{
		{RewardBlockTotalKey(op.Epoch), EncodeRewardBlockTotal(0, 0)},
		{RewardBlockAllTotalKey(op.Epoch), EncodeRewardBlockTotal(0, 0)},
		{RewardBlockFeesKey(op.Epoch), EncodeRewardBlockCount(0)},
	} {
		_, ok, err := getRow(txn, row.k)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		if err := putJournaled(txn, op.Slot, row.k, row.v); err != nil {
			return err
		}
	}
	return nil
}

func countRewardBlock(txn *badger.Txn, op RewardBlockOp) error {
	if err := countRewardBlockIn(
		txn, op, RewardBlockAllTotalKey(op.Epoch),
		RewardBlockAllCountKey(op.Epoch, op.Pool),
	); err != nil || op.Overlay {
		return err
	}
	return countRewardBlockIn(
		txn, op, RewardBlockTotalKey(op.Epoch),
		RewardBlockCountKey(op.Epoch, op.Pool),
	)
}

// countRewardBlockIn counts op's block in the total and pool rows given.
func countRewardBlockIn(
	txn *badger.Txn,
	op RewardBlockOp,
	totalKey, countKey []byte,
) error {
	raw, ok, err := getRow(txn, totalKey)
	if err != nil || !ok {
		return err
	}
	total, next, err := DecodeRewardBlockTotal(raw)
	if err != nil {
		return err
	}
	if op.Slot < next {
		return nil
	}
	if total == math.MaxUint64 {
		return errors.New("epoch block total overflow")
	}
	var count uint64
	if raw, ok, err = getRow(txn, countKey); err != nil {
		return err
	} else if ok {
		if count, err = DecodeRewardBlockCount(raw); err != nil {
			return err
		}
	}
	if err := putJournaled(
		txn, op.Slot, totalKey, EncodeRewardBlockTotal(total+1, op.Slot+1),
	); err != nil {
		return err
	}
	return putJournaled(
		txn, op.Slot, countKey, EncodeRewardBlockCount(count+1),
	)
}

func addRewardBlockFees(txn *badger.Txn, op RewardBlockOp) error {
	k := RewardBlockFeesKey(op.Epoch)
	raw, ok, err := getRow(txn, k)
	if err != nil || !ok {
		return err
	}
	fees, err := DecodeRewardBlockCount(raw)
	if err != nil {
		return err
	}
	if op.Fee > math.MaxUint64-fees {
		return errors.New("epoch fee total overflow")
	}
	return putJournaled(txn, op.Slot, k, EncodeRewardBlockCount(fees+op.Fee))
}

func pruneRewardBlocks(txn *badger.Txn, below uint64) error {
	var keys [][]byte
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte{RewardBlocksTag}
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	for it.Rewind(); it.Valid(); it.Next() {
		k := it.Item().Key()
		if len(k) < 9 || binary.BigEndian.Uint64(k[1:9]) >= below {
			break
		}
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	it.Close()
	return deleteKeys(txn, keys)
}

// LoadRewardBlocks replaces the store's reward block rows with the ones feed
// supplies, each as a primary key and value, and marks them loaded, for a
// store from before it kept them.
func (s *Store) LoadRewardBlocks(
	feed func(put func(k, v []byte) error) error,
) error {
	if err := s.loadFamily(
		RewardBlocksTag, rewardBlocksKey, "reward blocks", feed,
	); err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Delete(legacyRewardBlocksKey)
	})
}

// RewardBlocksLoaded reports whether the store holds the reward block rows
// up to its tip.
func (s *Store) RewardBlocksLoaded() (bool, error) {
	return s.hasMarker(rewardBlocksKey)
}
