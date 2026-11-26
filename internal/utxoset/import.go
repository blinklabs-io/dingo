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
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// DirFor is the live UTxO set's directory under a node's data directory.
func DirFor(dataDir string) string {
	return badgerutxo.DirFor(dataDir)
}

// CheckpointDirFor is the directory a node's live UTxO set checkpoints are
// written to, beside the set.
func CheckpointDirFor(dataDir string) string {
	return badgerutxo.DirFor(dataDir) + "-checkpoints"
}

// LoadUtxoSet starts a ledger-state import: it replaces the set with the
// outputs feed puts, as of point, and leaves the set marked as a partial
// import until FinishImport. It implements ledgerstate.UtxoSetLoader. A set
// that a stopped import of the same point left with its outputs complete
// keeps them, and feed is not called.
func (s *Set) LoadUtxoSet(
	point ocommon.Point,
	feed func(put func(txID []byte, index uint32, cbor []byte) error) error,
) error {
	err := s.store.BootstrapImport(point,
		func(put func(badgerutxo.Output) error) error {
			return feed(func(txID []byte, index uint32, cbor []byte) error {
				return put(badgerutxo.Output{
					Ref:  badgerutxo.NewRef(txID, index),
					Cbor: cbor,
				})
			})
		},
	)
	if err != nil {
		return fmt.Errorf("utxoset: load: %w", err)
	}
	n, _ := s.store.Count()
	s.logger.Info(
		"loaded the live UTxO set",
		"live_outputs",
		n,
		"slot",
		point.Slot,
	)
	return nil
}

// LiveUtxoCbor implements types.LiveUtxoSource over the set's committed
// state, for a metadata store with no ledger staging changes over it.
func (s *Set) LiveUtxoCbor(
	_ types.Txn,
	txID []byte,
	index uint32,
) ([]byte, bool, error) {
	return s.Get(txID, index)
}

// LiveUtxoStake implements types.LiveUtxoSource over the set's committed
// state.
func (s *Set) LiveUtxoStake(
	_ types.Txn,
	tag uint8,
	hash []byte,
) (uint64, error) {
	return s.CredentialStake(tag, hash)
}

// EachLiveUtxoStake implements types.LiveUtxoSource over the set's committed
// state.
func (s *Set) EachLiveUtxoStake(
	_ types.Txn,
	fn func(tag uint8, hash []byte, lovelace uint64) error,
) error {
	return s.EachCredentialStake(func(tag uint8, hash []byte, n uint64) error {
		if n == 0 {
			return nil
		}
		return fn(tag, hash, n)
	})
}

// LivePointerStakes implements types.LiveUtxoSource over the set's committed
// state.
func (s *Set) LivePointerStakes(types.Txn) ([]types.PointerStake, error) {
	var ret []types.PointerStake
	err := s.store.EachSum(func(key []byte, n uint64) error {
		slot, txIndex, certIndex, ok := ledger.ParsePointerStakeKey(key)
		if !ok || n == 0 {
			return nil
		}
		ret = append(ret, types.PointerStake{
			Slot: slot, TxIndex: txIndex, CertIndex: certIndex, Lovelace: n,
		})
		return nil
	})
	return ret, err
}

// FinishImport removes the mark LoadUtxoSet left, once the import has written
// the whole ledger state at the snapshot's point.
func (s *Set) FinishImport() error {
	if err := s.store.EndImport(); err != nil {
		return fmt.Errorf("utxoset: finish import: %w", err)
	}
	return nil
}

// ImportPending returns the snapshot point of an import that began writing
// the set and did not finish; ok is false when there is none.
func (s *Set) ImportPending() (ocommon.Point, bool, error) {
	return s.store.ImportPending()
}

// LoadUtxoSetCatchUp starts a catch-up import of the ledger state at point,
// for a snapshot newer than the set: it marks the set as holding a partial
// import, replaces its outputs with the ones feed puts and keeps every other
// family, which holds the history older than the snapshot, for the import to
// write the snapshot's rows over. A set past point is rolled back to it
// through its journal first, and the load fails when the journal does not
// reach it. A set with no tip holds nothing the snapshot continues, so it is
// replaced whole. A set a stopped catch-up of the same point left with its
// outputs complete keeps them, and feed is not called.
func (s *Set) LoadUtxoSetCatchUp(
	point ocommon.Point,
	feed func(put func(txID []byte, index uint32, cbor []byte) error) error,
) error {
	incomplete, err := s.store.BootstrapIncomplete()
	if err != nil {
		return fmt.Errorf("utxoset: catch-up load: %w", err)
	}
	if !incomplete {
		tip, ok, err := s.store.Tip()
		if err != nil {
			return fmt.Errorf("utxoset: catch-up load: %w", err)
		}
		if !ok {
			return s.LoadUtxoSet(point, feed)
		}

		if tip.Slot > point.Slot {
			// The set holds the history of blocks past the snapshot, and its
			// families hold rows those blocks wrote. Rolling back to the
			// snapshot's point undoes both; replacing the set whole would
			// lose the history below it as well.
			if _, err := s.store.Rollback(point); err != nil {
				return fmt.Errorf(
					"utxoset: catch-up load: the set is at slot %d, past the snapshot's slot %d, and cannot be rolled back to it: %w",
					tip.Slot, point.Slot, err,
				)
			}
		}
	}
	err = s.store.BootstrapImportCatchUp(point,
		func(put func(badgerutxo.Output) error) error {
			return feed(func(txID []byte, index uint32, cbor []byte) error {
				return put(badgerutxo.Output{
					Ref:  badgerutxo.NewRef(txID, index),
					Cbor: cbor,
				})
			})
		},
	)
	if err != nil {
		return fmt.Errorf("utxoset: catch-up load: %w", err)
	}
	n, _ := s.store.Count()
	s.logger.Info(
		"loaded the live UTxO set for a catch-up, keeping its ledger-state families",
		"live_outputs", n,
		"slot", point.Slot,
	)
	return nil
}

// CatchUpReaches reports whether LoadUtxoSetCatchUp can load a snapshot at
// point without being refused: the set holds no tip, holds an import it has
// not finished, is not past point, or can be rolled back to exactly point
// from its journal. It changes nothing.
func (s *Set) CatchUpReaches(point ocommon.Point) (bool, error) {
	incomplete, err := s.store.BootstrapIncomplete()
	if err != nil {
		return false, fmt.Errorf("utxoset: catch-up reach: %w", err)
	}
	if incomplete {
		return true, nil
	}
	tip, ok, err := s.store.Tip()
	if err != nil {
		return false, fmt.Errorf("utxoset: catch-up reach: %w", err)
	}
	if !ok || tip.Slot <= point.Slot {
		return true, nil
	}
	// RollbackReaches also accepts a point inside the journal's range that
	// is no journaled tip, which the rollback the load runs then refuses.
	return s.store.HasJournalPoint(point)
}

// ReplaceImportedBlockCounts replaces the imported per-pool block counts of
// epoch, keyed by pool key hash, and their total, captured at slot.
func (s *Set) ReplaceImportedBlockCounts(
	epoch uint64,
	pools map[string]uint64,
	total, capturedSlot uint64,
) error {
	if err := s.store.ReplaceImportedBlocks(
		epoch, pools, total, capturedSlot,
	); err != nil {
		return fmt.Errorf("utxoset: replace imported block counts: %w", err)
	}
	return nil
}

// DeleteNetworkDonationsForEpoch deletes the donations recorded for epoch.
func (s *Set) DeleteNetworkDonationsForEpoch(epoch uint64) error {
	if err := s.store.DeleteDonationsForEpoch(epoch); err != nil {
		return fmt.Errorf("utxoset: delete donations: %w", err)
	}
	return nil
}

// HoldsLedgerState reports whether the set holds every ledger-state family,
// so a catch-up has a state to write the snapshot over.
func (s *Set) HoldsLedgerState() (bool, error) {
	for _, f := range nodesettings.Families() {
		loaded, err := ledger.FamilyLoaded(f, s)
		if err != nil || !loaded {
			return false, err
		}
	}
	return true, nil
}
