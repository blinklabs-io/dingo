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
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/drainfix"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// drainedFixture is a database whose ledger state lives only in its set.
type drainedFixture struct {
	db  *database.Database
	set *utxoset.Set
}

const (
	drainFixtureAnchorSlot  = uint64(100_000)
	drainFixtureCatchUpSlot = uint64(200_000)
)

var (
	drainFixtureStakingKey = bytes.Repeat([]byte{0xbb}, 28)
	// drainFixtureAnchorKey is the one account of the anchor snapshot, which
	// the later snapshots no longer list.
	drainFixtureAnchorKey = bytes.Repeat([]byte{0xcc}, 28)
)

// drainFixtureConfig is the import of the snapshot at tipSlot. The anchor
// snapshot has one output and a small reward; the later one has two outputs
// and a larger reward, so a catch-up that kept the anchor's state differs.
func drainFixtureConfig(
	t *testing.T,
	db *database.Database,
	tipSlot uint64,
	reconcile bool,
	set UtxoSetLoader,
) ImportConfig {
	t.Helper()
	addr := buildShelleyAddr(
		0, 1,
		bytes.Repeat([]byte{0x83}, 28),
		bytes.Repeat([]byte{0x84}, 28),
	)
	amounts := []uint64{5_000_000}
	reward := uint64(4_000_000)
	if tipSlot == drainFixtureCatchUpSlot {
		amounts = []uint64{5_000_000, 7_000_000}
		reward = 9_000_000
	}
	nonce := bytes.Repeat([]byte{byte(tipSlot / 100_000)}, 32)
	stakingKey := drainFixtureStakingKey
	if tipSlot == drainFixtureAnchorSlot {
		stakingKey = drainFixtureAnchorKey
	}
	return ImportConfig{
		Database:  db,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Reconcile: reconcile,
		UtxoSet:   set,
		State: &RawLedgerState{
			UTxOData:      inlineUTxOMap(t, addr, amounts),
			CertStateData: accountCertStateData(t, stakingKey, reward, 2_000_000),
			GovStateData: testGovStateData(
				t, bytes.Repeat([]byte{0x95}, 32), tipSlot/1_000,
			),
			Epoch:               tipSlot / 1_000,
			EraIndex:            EraConway,
			EraBounds:           make([]EraBound, EraConway+1),
			EpochNonce:          nonce,
			EvolvingNonce:       nonce,
			CandidateNonce:      nonce,
			LastEpochBlockNonce: nonce,
			Tip: &SnapshotTip{
				Slot:      tipSlot,
				BlockHash: bytes.Repeat([]byte{byte(tipSlot / 100_000)}, 32),
			},
		},
		EpochLength: func(uint) (uint, uint, error) {
			return 1, importTestEpochLength, nil
		},
	}
}

func openDrainFixtureSet(t *testing.T) *utxoset.Set {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{
		Dir: t.TempDir(), Only: true, Authoritative: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	return set
}

func newDrainFixtureDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	return db
}

// newDrainedFixture imports the anchor snapshot into a set-only database,
// loads the set's families and drains every family the release graduates.
func newDrainedFixture(t *testing.T) *drainedFixture {
	t.Helper()
	ctx := t.Context()
	db := newDrainFixtureDB(t)
	set := openDrainFixtureSet(t)
	require.NoError(t, ImportLedgerState(
		ctx, drainFixtureConfig(t, db, drainFixtureAnchorSlot, false, set),
	))
	drainfix.Drain(t, db, set)
	return &drainedFixture{db: db, set: set}
}

// requireSetMatchesSQL compares every graduated family in set with the SQL
// rows an undrained database holds, and the live outputs with its utxo rows.
func requireSetMatchesSQL(
	t *testing.T,
	undrained *database.Database,
	set *utxoset.Set,
) {
	t.Helper()
	ctx := t.Context()
	ldb := dbadapter.New(undrained)
	checked := 0
	for _, f := range nodesettings.Families() {
		if len(statedrain.Tables(f)) == 0 && f != nodesettings.FamilyEraSwitch {
			continue
		}
		if f == nodesettings.FamilyGovernance {
			// A proposal's surrogate id comes from the SQL sequence, which
			// keeps counting past the rows a drain deleted, and from the set's
			// own sequence afterwards; the rows are compared without it.
			requireSameProposals(t, ldb, set)
			checked++
			continue
		}
		require.NoError(
			t, ledger.VerifyFamily(ctx, f, ldb, set),
			"family %s differs from the undrained database", f,
		)
		checked++
	}
	require.Positive(t, checked)
	want := map[string][]byte{}
	require.NoError(t, undrained.IterateLiveUtxos(ctx, nil, func(u *models.Utxo) error {
		want[string(u.TxId)+string(rune(u.OutputIdx))] = u.Cbor
		return nil
	}))
	got := map[string][]byte{}
	require.NoError(t, set.EachUtxo(func(txID []byte, idx uint32, cbor []byte) error {
		got[string(txID)+string(rune(idx))] = cbor
		return nil
	}))
	require.Equal(t, want, got, "live outputs differ from the undrained database")
}

type proposalKey struct {
	TxHash                      string
	ActionIndex                 uint32
	ProposedEpoch, ExpiresEpoch uint64
	AddedSlot                   uint64
	Deposit                     uint64
}

func requireSameProposals(
	t *testing.T,
	ldb ledgerdb.LedgerDB,
	set *utxoset.Set,
) {
	t.Helper()
	txn := ldb.Begin(t.Context(), false)
	defer txn.Release()
	stored, err := ldb.GetAllGovernanceProposals(t.Context(), txn)
	require.NoError(t, err)
	held, err := set.ProposalsAt(nil)
	require.NoError(t, err)
	require.NotEmpty(t, stored)
	key := func(p *models.GovernanceProposal) proposalKey {
		return proposalKey{
			string(p.TxHash), p.ActionIndex, p.ProposedEpoch,
			p.ExpiresEpoch, p.AddedSlot, p.Deposit,
		}
	}
	var want, got []proposalKey
	for _, p := range stored {
		want = append(want, key(p))
	}
	for i := range held {
		got = append(got, key(&held[i]))
	}
	require.ElementsMatch(t, want, got)
}

// newUndrainedCatchUp imports the anchor snapshot into a database that drained
// nothing and then catches it up to the snapshot at slot. Its SQL rows keep the
// history older than slot, which a drained database's set has to keep too.
func newUndrainedCatchUp(t *testing.T, slot uint64) *database.Database {
	t.Helper()
	db := newDrainFixtureDB(t)
	require.NoError(t, ImportLedgerState(
		t.Context(), drainFixtureConfig(t, db, drainFixtureAnchorSlot, false, nil),
	))
	require.NoError(t, ImportLedgerState(
		t.Context(), drainFixtureConfig(t, db, slot, true, nil),
	))
	return db
}

func newUndrainedImport(t *testing.T) *database.Database {
	t.Helper()
	return newUndrainedCatchUp(t, drainFixtureCatchUpSlot)
}

func requireImportFinished(t *testing.T, f *drainedFixture, wantMask nodesettings.FamilyMask) {
	t.Helper()
	ctx := t.Context()
	_, pending, err := f.db.DrainedImportPending(ctx)
	require.NoError(t, err)
	require.False(t, pending, "the pending marker must be cleared")
	require.False(t, f.db.DrainedImportActive())
	mask, err := f.db.DrainedFamilies(ctx)
	require.NoError(t, err)
	require.Equal(t, wantMask, mask, "the latch must be kept")
	markers, err := f.db.ListSyncStateKeysByPrefix(statedrain.MarkerPrefix, nil)
	require.NoError(t, err)
	require.Empty(t, markers, "every drain marker must be cleared")
	tip, ok, err := f.set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, drainFixtureCatchUpSlot, tip.Slot)
	raw, err := dbtest.RawSQLiteMetadata(t, f.db)
	require.NoError(t, err)
	for _, table := range []string{"account", "block_nonce", "epoch", "pparams", "governance_proposal"} {
		var n int
		require.NoError(t, raw.QueryRow("SELECT count(*) FROM "+table).Scan(&n))
		require.Zero(t, n, "%s rows must be deleted again", table)
	}
}

// TestCatchUpOnADrainedDatabaseMatchesAnUndrainedCatchUp requires a catch-up
// import into a drained database to leave the live UTxO set holding what the
// same catch-up leaves in the SQL tables of an undrained database, with the
// latch kept and the SQL rows deleted again.
func TestCatchUpOnADrainedDatabaseMatchesAnUndrainedCatchUp(t *testing.T) {
	t.Parallel()
	undrained := newUndrainedImport(t)
	f := newDrainedFixture(t)
	latch, err := f.db.DrainedFamilies(t.Context())
	require.NoError(t, err)

	require.NoError(t, ImportLedgerState(
		t.Context(),
		drainFixtureConfig(t, f.db, drainFixtureCatchUpSlot, true, f.set),
	))

	requireImportFinished(t, f, latch)
	requireSetMatchesSQL(t, undrained, f.set)
}

// failingLoader fails the first LoadLedgerState, as a crash between the SQL
// import and the set's load would.
type failingLoader struct {
	*utxoset.Set
	failed bool
}

func (l *failingLoader) LoadLedgerState(
	ctx context.Context, ldb ledgerdb.LedgerDB,
) error {
	if !l.failed {
		l.failed = true
		return errors.New("injected crash before the set loaded")
	}
	return l.Set.LoadLedgerState(ctx, ldb)
}

// TestCatchUpOnADrainedDatabaseResumesAfterACrash requires an import that
// stopped part way to leave the latch and a pending marker, and the next import
// to finish with the same result as one that never stopped.
func TestCatchUpOnADrainedDatabaseResumesAfterACrash(t *testing.T) {
	t.Parallel()
	undrained := newUndrainedImport(t)
	for _, tc := range []struct {
		name  string
		crash func(t *testing.T, f *drainedFixture)
	}{
		{"during the SQL phases", func(t *testing.T, f *drainedFixture) {
			ctx, cancel := context.WithCancel(t.Context())
			cfg := drainFixtureConfig(t, f.db, drainFixtureCatchUpSlot, true, f.set)
			cfg.OnProgress = func(p ImportProgress) {
				if p.Stage == "accounts" {
					cancel()
				}
			}
			err := ImportLedgerState(ctx, cfg)
			require.ErrorIs(t, err, context.Canceled)
		}},
		{"before the set loaded", func(t *testing.T, f *drainedFixture) {
			err := ImportLedgerState(
				t.Context(),
				drainFixtureConfig(t, f.db, drainFixtureCatchUpSlot, true, &failingLoader{Set: f.set}),
			)
			require.ErrorContains(t, err, "injected crash")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newDrainedFixture(t)
			latch, err := f.db.DrainedFamilies(t.Context())
			require.NoError(t, err)

			tc.crash(t, f)

			_, pending, err := f.db.DrainedImportPending(t.Context())
			require.NoError(t, err)
			require.True(t, pending, "an interrupted import stays pending")
			require.False(t, f.db.DrainedImportActive())
			mask, err := f.db.DrainedFamilies(t.Context())
			require.NoError(t, err)
			require.Equal(t, latch, mask, "the latch must be kept")

			require.NoError(t, ImportLedgerState(
				t.Context(),
				drainFixtureConfig(t, f.db, drainFixtureCatchUpSlot, true, f.set),
			))
			requireImportFinished(t, f, latch)
			requireSetMatchesSQL(t, undrained, f.set)
		})
	}
}

const drainFixtureRetrySlot = uint64(300_000)

// TestRetriedDrainedImportOverADifferentSnapshotDropsTheFirstOnesRows requires
// an import that stopped part way to leave no row behind when the next import
// is of another snapshot: the set ends equal to a fresh import of the second.
func TestRetriedDrainedImportOverADifferentSnapshotDropsTheFirstOnesRows(t *testing.T) {
	t.Parallel()
	baseline := newUndrainedCatchUp(t, drainFixtureRetrySlot)
	f := newDrainedFixture(t)

	ctx, cancel := context.WithCancel(t.Context())
	cfg := drainFixtureConfig(t, f.db, drainFixtureCatchUpSlot, true, f.set)
	cfg.OnProgress = func(p ImportProgress) {
		if p.Stage == "accounts" {
			cancel()
		}
	}
	require.ErrorIs(t, ImportLedgerState(ctx, cfg), context.Canceled)

	require.NoError(t, ImportLedgerState(
		t.Context(),
		drainFixtureConfig(t, f.db, drainFixtureRetrySlot, true, f.set),
	))
	_, pending, err := f.db.DrainedImportPending(t.Context())
	require.NoError(t, err)
	require.False(t, pending)
	requireSetMatchesSQL(t, baseline, f.set)
}

// TestDrainedCatchUpKeepsTheHistoryOlderThanTheSnapshot requires a catch-up
// into a drained database to keep what a catch-up into SQL rows keeps: the
// account the snapshot no longer lists stays, inactive, with the state it had
// before the snapshot readable as of the earlier slot, and the block nonce
// and epoch rows older than the snapshot stay.
func TestDrainedCatchUpKeepsTheHistoryOlderThanTheSnapshot(t *testing.T) {
	t.Parallel()
	f := newDrainedFixture(t)
	require.NoError(t, ImportLedgerState(
		t.Context(),
		drainFixtureConfig(t, f.db, drainFixtureCatchUpSlot, true, f.set),
	))

	gone, err := f.set.Account(0, drainFixtureAnchorKey)
	require.NoError(t, err)
	require.NotNil(t, gone, "the account only the older snapshot listed must stay")
	require.False(t, gone.Active, "the newer snapshot no longer lists it")
	before, err := f.set.AccountAt(0, drainFixtureAnchorKey, drainFixtureAnchorSlot)
	require.NoError(t, err)
	require.NotNil(t, before)
	require.True(t, before.Active, "as of the older snapshot the account was active")
	kept, err := f.set.Account(0, drainFixtureStakingKey)
	require.NoError(t, err)
	require.NotNil(t, kept)
	require.True(t, kept.Active)

	nonces, err := f.set.BlockNoncesInSlotRange(0, drainFixtureCatchUpSlot+1)
	require.NoError(t, err)
	var slots []uint64
	for _, n := range nonces {
		slots = append(slots, n.Slot)
	}
	require.Contains(t, slots, drainFixtureAnchorSlot, "the older snapshot's nonce must stay")
	require.Contains(t, slots, drainFixtureCatchUpSlot)
}
