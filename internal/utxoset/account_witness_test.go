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

package utxoset_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func openAccountSet(t *testing.T) *utxoset.Set {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	return set
}

func applyAt(
	t *testing.T,
	set *utxoset.Set,
	slot uint64,
	lc ledger.LedgerChanges,
) {
	t.Helper()
	tip := ocommon.NewPoint(slot, bytes.Repeat([]byte{byte(slot)}, 32))
	require.NoError(t, set.ApplyLedgerChanges(&tip, lc))
}

// TestWitnessSlotsRollBackToTheirTarget requires a rollback to drop the
// witnesses after its target and keep the rest, a read to take the greatest
// slot at or below its bound, and a credential to see only its own.
func TestWitnessSlotsRollBackToTheirTarget(t *testing.T) {
	t.Parallel()
	set := openAccountSet(t)
	a := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x11}, 28))
	b := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x12}, 28))
	scripts := models.NewStakeCredentialRef(1, a.Key)
	for _, w := range []struct {
		slot uint64
		ref  models.StakeCredentialRef
	}{{10, a}, {15, scripts}, {20, a}, {25, b}, {30, a}} {
		applyAt(t, set, w.slot, ledger.LedgerChanges{
			Witnesses: []ledger.WitnessChange{{Slot: w.slot, Ref: w.ref}},
		})
	}
	last := func(ref models.StakeCredentialRef, bound uint64) (uint64, bool) {
		slot, ok, err := set.LastWitnessSlot(ref, bound)
		require.NoError(t, err)
		return slot, ok
	}
	slot, ok := last(a, math.MaxUint64)
	require.True(t, ok)
	require.Equal(t, uint64(30), slot)
	slot, ok = last(a, 29)
	require.True(t, ok)
	require.Equal(t, uint64(20), slot)
	slot, ok = last(a, 20)
	require.True(t, ok)
	require.Equal(t, uint64(20), slot, "a bound is inclusive")
	_, ok = last(a, 9)
	require.False(t, ok)
	slot, _ = last(scripts, math.MaxUint64)
	require.Equal(t, uint64(15), slot, "a script credential is its own")

	_, err := set.Rollback(ocommon.NewPoint(20, bytes.Repeat([]byte{20}, 32)))
	require.NoError(t, err)
	slot, _ = last(a, math.MaxUint64)
	require.Equal(t, uint64(20), slot, "the witness at the target is kept")
	_, ok = last(b, math.MaxUint64)
	require.False(t, ok, "the witness after the target is gone")
	slot, _ = last(scripts, math.MaxUint64)
	require.Equal(t, uint64(15), slot)
}

// TestImportBaselinesRollBackToTheirTarget requires a baseline a later import
// replaced to come back with a rollback, and one the rollback undoes the
// first write of to be gone.
func TestImportBaselinesRollBackToTheirTarget(t *testing.T) {
	t.Parallel()
	set := openAccountSet(t)
	ref := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x21}, 28))
	deposit := uint64(2_000_000)
	applyAt(t, set, 5, ledger.LedgerChanges{})
	applyAt(t, set, 10, ledger.LedgerChanges{
		ImportBaselines: []ledger.ImportBaselineChange{
			{Slot: 10, Baseline: ledger.ImportBaseline{
				Ref: ref, Active: true, AddedSlot: 10, Deposit: &deposit,
			}},
		},
	})
	applyAt(t, set, 20, ledger.LedgerChanges{
		ImportBaselines: []ledger.ImportBaselineChange{
			{Slot: 20, Baseline: ledger.ImportBaseline{
				Ref: ref, Active: false, AddedSlot: 20,
			}},
		},
	})
	b, err := set.ImportBaseline(ref)
	require.NoError(t, err)
	require.Equal(t, &ledger.ImportBaseline{Ref: ref, AddedSlot: 20}, b)
	b, err = set.ImportBaselineAt(ref, 15)
	require.NoError(t, err)
	require.Equal(
		t,
		&ledger.ImportBaseline{
			Ref:       ref,
			Active:    true,
			AddedSlot: 10,
			Deposit:   &deposit,
		},
		b,
		"as a rollback to slot 15 would leave it",
	)

	_, err = set.Rollback(ocommon.NewPoint(10, bytes.Repeat([]byte{10}, 32)))
	require.NoError(t, err)
	b, err = set.ImportBaseline(ref)
	require.NoError(t, err)
	require.Equal(
		t,
		&ledger.ImportBaseline{
			Ref:       ref,
			Active:    true,
			AddedSlot: 10,
			Deposit:   &deposit,
		},
		b,
	)
	_, err = set.Rollback(ocommon.NewPoint(5, bytes.Repeat([]byte{5}, 32)))
	require.NoError(t, err)
	b, err = set.ImportBaseline(ref)
	require.NoError(t, err)
	require.Nil(t, b)
}

// TestLoadingUtxoSetClearsAccountWitnessFamilies requires a rebuild of the
// set for an import to drop the import baselines and witness slots and their
// markers, so they load again from the metadata store.
func TestLoadingUtxoSetClearsAccountWitnessFamilies(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	set := openAccountSet(t)
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	for _, loaded := range []func() (bool, error){
		set.ImportBaselinesLoaded, set.WitnessSlotsLoaded,
	} {
		ok, err := loaded()
		require.NoError(t, err)
		require.True(t, ok, "an empty metadata store loads empty families")
	}
	ref := models.NewStakeCredentialRef(0, bytes.Repeat([]byte{0x31}, 28))
	applyAt(t, set, 10, ledger.LedgerChanges{
		ImportBaselines: []ledger.ImportBaselineChange{
			{Slot: 10, Baseline: ledger.ImportBaseline{
				Ref: ref, Active: true, AddedSlot: 10,
			}},
		},
		Witnesses: []ledger.WitnessChange{{Slot: 10, Ref: ref}},
	})
	require.NoError(t, set.LoadUtxoSet(
		ocommon.NewPoint(40, bytes.Repeat([]byte{40}, 32)),
		func(func([]byte, uint32, []byte) error) error { return nil },
	))
	b, err := set.ImportBaseline(ref)
	require.NoError(t, err)
	require.Nil(t, b)
	_, ok, err := set.LastWitnessSlot(ref, math.MaxUint64)
	require.NoError(t, err)
	require.False(t, ok)
	// The import writes these families itself, so they are loaded empty.
	for _, loaded := range []func() (bool, error){
		set.ImportBaselinesLoaded, set.WitnessSlotsLoaded,
	} {
		ok, err := loaded()
		require.NoError(t, err)
		require.True(t, ok)
	}
}
