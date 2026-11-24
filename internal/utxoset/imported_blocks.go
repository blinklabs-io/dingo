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

package utxoset

import (
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/ledger"
)

// ensureImportedBlocks loads the block counts a snapshot import carried for
// the epochs below its anchor, as imported, into a set from before it kept
// them. A drained metadata store holds none, and the load then marks the set
// as having lost them: what the drain removed cannot be read back, and the
// windowed reward block rows already fold it in. The set then refuses to
// export the counts rather than export none.
func (s *Set) ensureImportedBlocks(
	ctx context.Context,
	ldb ledgerdb.LedgerDB,
) error {
	loaded, err := s.store.ImportedBlocksLoaded()
	if err != nil {
		return fmt.Errorf("utxoset: read the imported block marker: %w", err)
	}
	if loaded {
		return nil
	}
	var rows map[uint64]ledgerdb.ImportedBlockRows
	lost := false
	if r, ok := ldb.(ledgerdb.ImportedBlockRowsReader); ok {
		txn := ldb.Begin(ctx, false)
		rows, err = r.ImportedBlockRows(txn)
		txn.Release()
		if errors.Is(err, database.ErrNoMetadataStore) {
			lost, err = true, nil
		}
		if err != nil {
			return fmt.Errorf("utxoset: read imported block counts: %w", err)
		}
	}
	err = s.store.LoadImportedBlocks(lost, func(
		put func(k, v []byte) error,
		tombstone func(slot uint64, prefix []byte) error,
	) error {
		for epoch, row := range rows {
			err := put(
				badgerutxo.ImportedBlockTotalKey(epoch),
				badgerutxo.EncodeImportedBlockValue(row.Total, row.CapturedSlot),
			)
			if err != nil {
				return err
			}
			for pool, n := range row.Pools {
				err := put(
					badgerutxo.ImportedBlockPoolKey(epoch, []byte(pool)),
					badgerutxo.EncodeImportedBlockValue(n, row.CapturedSlot),
				)
				if err != nil {
					return err
				}
			}
			err = tombstone(
				row.CapturedSlot, badgerutxo.ImportedBlocksEpochPrefix(epoch),
			)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("utxoset: load imported block counts: %w", err)
	}
	s.logger.Info(
		"loaded imported block counts into the live UTxO set",
		"epochs", len(rows),
	)
	return nil
}

// ImportedBlocksLoaded reports whether the set holds the imported block
// counts of every epoch a snapshot import carried them for.
func (s *Set) ImportedBlocksLoaded() (bool, error) {
	return s.store.ImportedBlocksLoaded()
}

// ImportedBlocksLost reports whether the metadata store had drained the
// imported block counts before the set could load them.
func (s *Set) ImportedBlocksLost() (bool, error) {
	return s.store.ImportedBlocksLost()
}

// ImportedBlocks lists the imported block counts the set holds, by epoch.
func (s *Set) ImportedBlocks() (map[uint64]ledger.ImportedBlockEpoch, error) {
	out := map[uint64]ledger.ImportedBlockEpoch{}
	err := s.store.Rows(
		[]byte{badgerutxo.ImportedBlocksTag},
		func(k, v []byte) error {
			epoch, kind, pool, err := badgerutxo.DecodeImportedBlockKey(k)
			if err != nil {
				return err
			}
			n, slot, err := badgerutxo.DecodeImportedBlockValue(v)
			if err != nil {
				return err
			}
			e := out[epoch]
			if e.Pools == nil {
				e.Pools = map[string]uint64{}
			}
			e.CapturedSlot = slot
			if kind == badgerutxo.ImportedBlockTotal {
				e.Total = n
			} else {
				e.Pools[string(pool)] = n
			}
			out[epoch] = e
			return nil
		},
	)
	return out, err
}
