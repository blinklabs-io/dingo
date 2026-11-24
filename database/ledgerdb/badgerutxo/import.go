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
	"bytes"
	"errors"
	"fmt"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
)

// ErrImportPending reports a set that a ledger-state import began to write
// and did not finish. Its rows are part of a snapshot, so nothing may serve
// them; running the import again completes it.
var ErrImportPending = errors.New(
	"badgerutxo: a ledger-state import into the set did not finish",
)

// importFamilyPrefixes are the rows an import writes after the outputs.
var importFamilyPrefixes = func() [][]byte {
	var out [][]byte
	for _, p := range bootstrapPrefixes {
		kept := false
		for _, c := range catchUpPrefixes {
			kept = kept || bytes.Equal(p, c)
		}
		if !kept {
			out = append(out, p)
		}
	}
	return out
}()

func readImport(txn *badger.Txn) (point ocommon.Point, outputs, ok bool, err error) {
	item, err := txn.Get(importKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return ocommon.Point{}, false, false, nil
	}
	if err != nil {
		return ocommon.Point{}, false, false, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return ocommon.Point{}, false, false, err
	}
	if len(v) < 9 {
		return ocommon.Point{}, false, false, fmt.Errorf(
			"badgerutxo: import mark of %d bytes", len(v),
		)
	}
	return decodePoint(v[:len(v)-1]), v[len(v)-1] == 1, true, nil
}

func importValue(point ocommon.Point, outputs bool) []byte {
	v := encodePoint(point)
	if outputs {
		return append(v, 1)
	}
	return append(v, 0)
}

// ImportPending returns the point of the import the set is marked with; ok is
// false when none is in progress.
func (s *Store) ImportPending() (point ocommon.Point, ok bool, err error) {
	err = s.viewRaw(func(txn *badger.Txn) error {
		point, _, ok, err = readImport(txn)
		return err
	})
	return point, ok, err
}

// BootstrapImport starts an import of the ledger state at point: it marks the
// set, replaces its contents with the outputs feed supplies, and leaves every
// other family empty and loaded for the import to write. The mark stays until
// EndImport.
//
// A set already marked with point whose outputs are complete keeps them and
// feed is not called; the families a stopped import wrote are emptied, so the
// import that resumes writes them from the start.
func (s *Store) BootstrapImport(
	point ocommon.Point,
	feed func(put func(Output) error) error,
) error {
	resume, err := s.importOutputsHeld(point)
	if err != nil {
		return err
	}
	if resume {
		if err := s.updateRaw(func(txn *badger.Txn) error {
			// A failure mark a refused import write left belongs to the
			// rows dropped here.
			if err := txn.Delete(failedKey); err != nil {
				return err
			}
			for _, groups := range [][][]byte{loadedMarkers, valueMarkers} {
				for _, k := range groups {
					if err := txn.Delete(k); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("badgerutxo: clear markers for import: %w", err)
		}
		if err := s.db.DropPrefix(importFamilyPrefixes...); err != nil {
			return fmt.Errorf("badgerutxo: drop for import: %w", err)
		}
	} else {
		if err := s.bootstrap(point, feed, false, true); err != nil {
			return err
		}
	}
	return s.loadNoRows(false)
}

// BootstrapImportCatchUp starts an import of the ledger state at point into a
// set that holds an older state: it marks the set and replaces the live
// outputs, their running sums and the UTxO journal, keeping every other
// family's rows and loaded markers for the import to write the snapshot's
// rows over. A set already marked with point whose outputs are complete keeps
// them.
func (s *Store) BootstrapImportCatchUp(
	point ocommon.Point,
	feed func(put func(Output) error) error,
) error {
	resume, err := s.importOutputsHeld(point)
	if err != nil {
		return err
	}
	if resume {
		// The rows a refused import write left are written again.
		if err := s.updateRaw(func(txn *badger.Txn) error {
			return txn.Delete(failedKey)
		}); err != nil {
			return fmt.Errorf("badgerutxo: clear failure for import: %w", err)
		}
	} else if err := s.bootstrap(point, feed, true, true); err != nil {
		return err
	}
	// A family the set never loaded holds nothing the snapshot continues.
	return s.loadNoRows(true)
}

// EndImport removes the import mark. The import calls it once the set holds
// the whole ledger state at the snapshot's point.
func (s *Store) EndImport() error {
	return s.update(func(txn *badger.Txn) error {
		return txn.Delete(importKey)
	})
}

func (s *Store) importOutputsHeld(point ocommon.Point) (bool, error) {
	var held bool
	err := s.viewRaw(func(txn *badger.Txn) error {
		if err := checkBootstrapComplete(txn); err != nil {
			if errors.Is(err, ErrBootstrapIncomplete) {
				return nil
			}
			return err
		}
		at, outputs, ok, err := readImport(txn)
		if err != nil {
			return err
		}
		held = ok && outputs && at.Slot == point.Slot &&
			bytes.Equal(at.Hash, point.Hash)
		return nil
	})
	return held, err
}

// LoadMissingFamiliesEmpty marks every family the store has not loaded as
// loaded with no rows, for a set that starts empty.
func (s *Store) LoadMissingFamiliesEmpty() error {
	return s.loadNoRows(true)
}

// loadNoRows marks every family loaded with the rows it holds, for a set
// whose rows an import writes itself. With missingOnly it leaves a family
// already loaded as it is.
func (s *Store) loadNoRows(missingOnly bool) error {
	none := func(func(k, v []byte) error) error { return nil }
	noneSlots := func(
		func(k, v []byte) error, func(uint64, []byte) error,
	) error {
		return nil
	}
	for _, f := range []struct {
		marker []byte
		load   func() error
	}{
		{noncesKey, func() error { return s.LoadNonces(nil) }},
		{epochStateKey, func() error { return s.LoadEpochState(nil, nil, nil) }},
		{summaryKey, func() error { return s.LoadSummaries(nil) }},
		{networkStateKey, func() error { return s.LoadNetworkState(nil, nil) }},
		{pparamsKey, func() error { return s.LoadPParams(nil) }},
		{syncMarkersKey, func() error { return s.LoadMarkers(nil) }},
		{accountsKey, func() error {
			err := s.LoadAccounts(
				func(func(AccountRow) error) error { return nil },
			)
			if err != nil {
				return err
			}
			// The import writes each account with the inactivity
			// activation it carries. Accounts a catch-up keeps are left
			// for the load that reads their activation.
			return s.LoadAccountInactivityActivation(0, 0, nil)
		}},
		{inactivityKey, func() error { return s.LoadInactivity(noneSlots) }},
		{drepsKey, func() error {
			// A loaded DRep state carries its dormancy counter.
			return s.LoadDrepState(func(put func(k, v []byte) error) error {
				return put(DormancyKey, make([]byte, 8))
			})
		}},
		{drepHistoryLoadedKey, func() error { return s.LoadDRepHistory(false, none) }},
		{poolsKey, func() error { return s.LoadPools(none) }},
		{poolHistoryKey, func() error { return s.LoadPoolHistory(none) }},
		{registrationHistoryKey, func() error { return s.LoadRegistrationHistory(none) }},
		{importBaselinesKey, func() error { return s.LoadImportBaselines(none) }},
		{witnessSlotsKey, func() error { return s.LoadWitnessSlots(none) }},
		{closureUtxoKey, func() error { return s.LoadClosureUtxoEffects(none) }},
		{delegationsKey, func() error { return s.LoadDelegations(none) }},
		{txIndexKey, func() error { return s.LoadTxIndex(none) }},
		{genesisMIRKey, func() error { return s.LoadGenesisMIR(none) }},
		{rewardOutputsKey, func() error { return s.LoadRewardOutputs(none) }},
		{opCertsKey, func() error { return s.LoadOpCerts(none) }},
		{rewardBlocksKey, func() error { return s.LoadRewardBlocks(none) }},
		{importedBlocksKey, func() error { return s.LoadImportedBlocks(false, noneSlots) }},
		{rewardRecordsKey, func() error {
			return s.LoadRewardRecords(
				false, func(func(RewardRecord) error) error { return nil },
			)
		}},
		{rewardInputsKey, func() error { return s.LoadRewardInputs(noneSlots) }},
		{rewardDerivedKey, func() error { return s.LoadRewardDerived(noneSlots) }},
		{committeeKey, func() error { return s.LoadCommittee(none) }},
		{governanceKey, func() error { return s.LoadGovernance(none) }},
		{ratifyPendingKey, func() error { return s.LoadRatifyPending(noneSlots) }},
		{pparamUpdatesKey, func() error { return s.LoadPParamUpdates(none) }},
	} {
		if missingOnly {
			held, err := s.hasMarker(f.marker)
			if err != nil {
				return err
			}
			if held {
				continue
			}
		}
		if err := f.load(); err != nil {
			return err
		}
	}
	return nil
}
