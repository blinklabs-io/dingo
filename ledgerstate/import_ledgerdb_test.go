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

package ledgerstate

import (
	"errors"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// openFixtureSetAt opens a LedgerDB set in dir, closed with the test.
func openFixtureSetAt(t *testing.T, dir string) *utxoset.Set {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{
		Dir: dir, Only: true, Authoritative: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	return set
}

// setRows closes set and returns every row of the store in dir, as
// "key=value" in key order.
func setRows(t *testing.T, set *utxoset.Set, dir string) []string {
	t.Helper()
	require.NoError(t, set.Close())
	st, err := badgerutxo.Open(dir)
	require.NoError(t, err)
	defer st.Close()
	var rows []string
	for tag := range 256 {
		require.NoError(t, st.Rows([]byte{byte(tag)}, func(k, v []byte) error {
			rows = append(rows, fmt.Sprintf("%x=%x", k, v))
			return nil
		}))
	}
	return rows
}

// TestImportWritesEveryFamilyToTheLedgerDB requires an import to leave the
// whole ledger state in the LedgerDB set, every family loaded and the import
// finished.
func TestImportWritesEveryFamilyToTheLedgerDB(t *testing.T) {
	t.Parallel()
	db := newFixtureDB(t)
	set := openFixtureSet(t)
	require.NoError(t, importLedgerStateForTest(
		t.Context(), richConfig(t, db, fixtureAnchorSlot, true, false, set),
	))
	_, pending, err := set.ImportPending()
	require.NoError(t, err)
	require.False(t, pending)
	for _, f := range nodesettings.Families() {
		loaded, err := ledger.FamilyLoaded(f, set)
		require.NoError(t, err, f.String())
		require.True(t, loaded, "family %s", f)
	}
	ldb := withImportLedgerDB(ImportConfig{Database: db, UtxoSet: set}).LedgerDB
	account, err := ldb.GetAccountByCredential(
		t.Context(), 0, fixtureAnchorKey, true, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.True(t, account.Active)
	require.Equal(t, richPool1, account.Pool)
	for _, pool := range [][]byte{richPool1, richPool2} {
		p, err := ldb.GetPool(t.Context(), [28]byte(pool), false, nil)
		require.NoError(t, err)
		require.NotNil(t, p, "pool %x", pool)
	}
	drep, err := ldb.GetDrepByCredential(t.Context(), 0, richDrep1, true, nil)
	require.NoError(t, err)
	require.NotNil(t, drep)
	dormant, err := ldb.GetDormantDRepEpochs(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, richAnchorDormant, dormant)
	state, err := ldb.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, fixtureAnchorSlot, state.Slot)
	nonce, ok, err := ldb.GetLatestBlockNonce(nil)
	require.NoError(t, err)
	require.True(t, ok, "the snapshot tip's nonce")
	require.Equal(t, fixtureAnchorSlot, nonce.Slot)
	count, err := set.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
}

var errStopped = errors.New("stopped")

// TestImportResumesAfterAStopAtEachPhase stops an import after each phase, as
// a crash there would, and requires the set to refuse to serve the partial
// state and the same import run again to leave the set row for row as an
// import that never stopped does, for a fresh import and for a catch-up.
func TestImportResumesAfterAStopAtEachPhase(t *testing.T) {
	t.Parallel()
	phases := []string{
		models.ImportPhaseUTxO,
		models.ImportPhaseCertState,
		models.ImportPhaseSnapshots,
		models.ImportPhasePParams,
		models.ImportPhaseGovState,
		models.ImportPhaseTip,
	}
	for _, catchUp := range []bool{false, true} {
		run := func(t *testing.T, stopAt string) []string {
			t.Helper()
			db := newFixtureDB(t)
			dir := t.TempDir()
			set := openFixtureSetAt(t, dir)
			slot := fixtureAnchorSlot
			if catchUp {
				require.NoError(t, importLedgerStateForTest(
					t.Context(),
					richConfig(t, db, fixtureAnchorSlot, true, false, set),
				))
				slot = fixtureCatchUpSlot
			}
			cfg := richConfig(t, db, slot, !catchUp, catchUp, set)
			if stopAt != "" {
				stopped := cfg
				stopped.afterPhase = func(phase string) error {
					if phase == stopAt {
						return errStopped
					}
					return nil
				}
				err := importLedgerStateForTest(t.Context(), stopped)
				require.ErrorIs(t, err, errStopped)
				_, pending, err := set.ImportPending()
				require.NoError(t, err)
				require.True(t, pending, "a stopped import stays marked")
				err = set.Reconcile(t.Context(), withImportLedgerDB(ImportConfig{Database: db, UtxoSet: set}).LedgerDB, nil)
				require.ErrorIs(t, err, badgerutxo.ErrImportPending,
					"a set holding a partial import must not be served")
			}
			require.NoError(t, importLedgerStateForTest(t.Context(), cfg))
			_, pending, err := set.ImportPending()
			require.NoError(t, err)
			require.False(t, pending)
			return setRows(t, set, dir)
		}
		name := "fresh"
		if catchUp {
			name = "catch-up"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			want := run(t, "")
			for _, phase := range phases {
				t.Run(phase, func(t *testing.T) {
					t.Parallel()
					require.Equal(t, want, run(t, phase))
				})
			}
		})
	}
}

// TestCatchUpImportKeepsAccountActivationFloor requires a catch-up import to
// keep the inactivity activation membership and floor of an account the set
// already holds. A snapshot carries neither, and the reward guard reads the
// floor from the account.
func TestCatchUpImportKeepsAccountActivationFloor(t *testing.T) {
	t.Parallel()
	const activationEpoch = uint64(150)
	db := newFixtureDB(t)
	set := openFixtureSet(t)
	require.NoError(t, importLedgerStateForTest(
		t.Context(),
		snapshotConfig(t, db, fixtureCatchUpSlot, false, set),
	))
	held, err := set.Account(0, fixtureStakingKey)
	require.NoError(t, err)
	require.NotNil(t, held)
	stamped := *held
	stamped.InactivityActivation = true
	stamped.InactivityActivationEpoch = activationEpoch
	require.NoError(t, set.ApplyLedgerChanges(nil, ledger.LedgerChanges{
		Accounts: []ledger.AccountChange{{
			Slot: fixtureCatchUpSlot, Account: stamped,
		}},
	}))

	const laterSlot = uint64(300_000)
	require.NoError(t, importLedgerStateForTest(
		t.Context(),
		snapshotConfig(t, db, laterSlot, true, set),
	))
	kept, err := set.Account(0, fixtureStakingKey)
	require.NoError(t, err)
	require.NotNil(t, kept)
	require.Equal(
		t, uint64(4_000_000), uint64(kept.Reward),
		"the catch-up must write the snapshot's account",
	)
	require.True(
		t, kept.InactivityActivation,
		"a re-imported account stays an activation member",
	)
	require.Equal(t, activationEpoch, kept.InactivityActivationEpoch)
}
