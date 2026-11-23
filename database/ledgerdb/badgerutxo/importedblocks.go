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

	"github.com/dgraph-io/badger/v4"
)

// Imported block rows are the counts a snapshot import carried for the epochs
// below its anchor, kept as imported. RewardBlocksTag rows are pruned to a
// window and fold the imported counts into the observed ones, so they cannot
// be exported back as the rows the metadata store held. Every row of an epoch
// shares the prefix Y <epoch:8 BE>:
//
//	Y <epoch> 0 <pool:28>   blocks the pool minted, then the slot the import
//	                        captured them at, 8 BE each
//	Y <epoch> 1             the epoch total the pool rows sum to, then the
//	                        capture slot, 8 BE each
//
// The total row is present exactly when the import said anything about the
// epoch, so an epoch with no pool rows and a total of zero is a certified
// zero-block epoch. A rollback below the capture slot drops the epoch's rows,
// through a tombstone recorded at load.
const (
	importedBlockPoolKind  byte = 0
	importedBlockTotalKind byte = 1
)

// ImportedBlocksEpochPrefix is the prefix of every imported block row of
// epoch.
func ImportedBlocksEpochPrefix(epoch uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{ImportedBlocksTag}, epoch)
}

// ImportedBlockPoolKey keys the blocks pool minted in epoch, as imported.
func ImportedBlockPoolKey(epoch uint64, pool []byte) []byte {
	return append(
		append(ImportedBlocksEpochPrefix(epoch), importedBlockPoolKind),
		pool...,
	)
}

// ImportedBlockTotalKey keys the total of the blocks imported for epoch.
func ImportedBlockTotalKey(epoch uint64) []byte {
	return append(ImportedBlocksEpochPrefix(epoch), importedBlockTotalKind)
}

// ImportedBlockRowKind names what an imported block row holds.
type ImportedBlockRowKind uint8

// The imported block row kinds.
const (
	ImportedBlockPool ImportedBlockRowKind = iota
	ImportedBlockTotal
)

// DecodeImportedBlockKey splits an imported block row key into its epoch,
// kind and, for a pool row, the pool.
func DecodeImportedBlockKey(
	k []byte,
) (epoch uint64, kind ImportedBlockRowKind, pool []byte, err error) {
	if len(k) < 10 || k[0] != ImportedBlocksTag {
		return 0, 0, nil, errors.New("badgerutxo: malformed imported block key")
	}
	epoch = binary.BigEndian.Uint64(k[1:9])
	switch k[9] {
	case importedBlockPoolKind:
		return epoch, ImportedBlockPool, k[10:], nil
	case importedBlockTotalKind:
		return epoch, ImportedBlockTotal, nil, nil
	}
	return 0, 0, nil, fmt.Errorf(
		"badgerutxo: unknown imported block row kind %d", k[9],
	)
}

// EncodeImportedBlockValue encodes a pool's imported count, or an epoch's
// imported total, with the slot the import captured it at.
func EncodeImportedBlockValue(blocks, capturedSlot uint64) []byte {
	return binary.BigEndian.AppendUint64(
		binary.BigEndian.AppendUint64(nil, blocks), capturedSlot,
	)
}

// DecodeImportedBlockValue decodes an imported block row's value.
func DecodeImportedBlockValue(
	v []byte,
) (blocks, capturedSlot uint64, err error) {
	if len(v) != 16 {
		return 0, 0, errors.New("badgerutxo: malformed imported block value")
	}
	return binary.BigEndian.Uint64(v), binary.BigEndian.Uint64(v[8:]), nil
}

// LoadImportedBlocks replaces the store's imported block rows with the ones
// feed supplies, each as a primary key and value, and marks them loaded.
func (s *Store) LoadImportedBlocks(
	feed func(
		put func(k, v []byte) error,
		tombstone func(slot uint64, prefix []byte) error,
	) error,
) error {
	tags := []byte{ImportedBlocksTag}
	if err := s.dropPrefix([]byte{ImportedBlocksTag}); err != nil {
		return fmt.Errorf("badgerutxo: drop imported blocks: %w", err)
	}
	wb := s.newLoadSink()
	defer wb.Cancel()
	err := feed(func(k, v []byte) error {
		if len(k) == 0 || k[0] != ImportedBlocksTag {
			return fmt.Errorf("badgerutxo: %x is not an imported block key", k)
		}
		return wb.Set(k, v)
	}, wb.tombstoneFor(tags))
	if err != nil {
		return fmt.Errorf("badgerutxo: load imported blocks: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("badgerutxo: load imported blocks: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return txn.Set(importedBlocksKey, loadedMarker)
	})
}

// ImportedBlocksLoaded reports whether the store holds the imported block
// rows of every epoch a snapshot import carried them for.
func (s *Store) ImportedBlocksLoaded() (bool, error) {
	return s.hasMarker(importedBlocksKey)
}

// ReplaceImportedBlocks replaces the imported block rows of epoch with the
// per-pool counts, keyed by pool key hash, and their total, as captured at
// slot. A rollback past slot deletes them, as it would a loaded epoch's.
func (s *Store) ReplaceImportedBlocks(
	epoch uint64,
	pools map[string]uint64,
	total, slot uint64,
) error {
	prefix := ImportedBlocksEpochPrefix(epoch)
	rows := make([]RowPut, 0, 3+len(pools))
	rows = append(rows,
		RowPut{Slot: slot, Key: prefix, WipePrefix: true},
		RowPut{Slot: slot, Key: prefix, TombstonePrefix: true},
		RowPut{
			Slot:  slot,
			Key:   ImportedBlockTotalKey(epoch),
			Value: EncodeImportedBlockValue(total, slot),
		},
	)
	for pool, n := range pools {
		rows = append(rows, RowPut{
			Slot:  slot,
			Key:   ImportedBlockPoolKey(epoch, []byte(pool)),
			Value: EncodeImportedBlockValue(n, slot),
		})
	}
	_, err := s.ApplyLedgerBatch(nil, LedgerBatch{Rows: rows})
	return err
}
