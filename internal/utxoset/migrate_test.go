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
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// sqlFixture is a metadata store holding a UTxO set in its utxo rows.
type sqlFixture struct {
	ledgerdb.LedgerDB
	mu     sync.Mutex
	tip    ocommon.Point
	rows   []*models.Utxo
	sync   map[string]string
	marked bool
	// flaky makes every scan after the first return different CBOR.
	flaky bool
	scans int
}

// sqlStore is the fixture as a utxoset.MigrationStore; its method names
// clash with the LedgerDB interface the fixture also stands in for.
type sqlStore struct{ *sqlFixture }

type noTxn struct{ ledgerdb.Txn }

func (noTxn) Release() {}

func (f *sqlFixture) Begin(context.Context, bool) ledgerdb.Txn { return noTxn{} }

func (f *sqlFixture) GetTip(ledgerdb.Txn) (ochainsync.Tip, error) {
	return ochainsync.Tip{Point: f.tip}, nil
}

func (f *sqlFixture) IterateLiveUtxos(
	_ context.Context,
	_ ledgerdb.Txn,
	fn func(*models.Utxo) error,
) error {
	f.mu.Lock()
	rows := append([]*models.Utxo(nil), f.rows...)
	f.scans++
	flaky := f.flaky && f.scans > 1
	f.mu.Unlock()
	for _, r := range rows {
		u := *r
		if flaky {
			u.Cbor = []byte{0x81, 0x00}
		}
		if err := fn(&u); err != nil {
			return err
		}
	}
	return nil
}

func (f sqlStore) SQLTip(context.Context) (ocommon.Point, error) {
	return f.tip, nil
}

func (f sqlStore) GetSyncState(_ context.Context, k string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sync[k], nil
}

func (f sqlStore) SetSyncState(_ context.Context, k, v string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sync[k] = v
	return nil
}

func (f sqlStore) DeleteSyncState(_ context.Context, k string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.sync, k)
	return nil
}

func (f sqlStore) DeleteUtxoRows(_ context.Context, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := min(limit, len(f.rows))
	f.rows = f.rows[n:]
	return int64(n), nil
}

func (f *sqlFixture) remaining() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows)
}

func newSQLFixture(outputs int) *sqlFixture {
	f := &sqlFixture{
		tip:  slotPoint(50),
		sync: map[string]string{},
	}
	for i := range outputs {
		f.rows = append(f.rows, &models.Utxo{
			TxId:      bytes.Repeat([]byte{byte(i + 1)}, 32),
			OutputIdx: uint32(i % 3), //nolint:gosec // small test index
			Cbor:      []byte{0x82, byte(i), 0x01},
		})
	}
	return f
}

func (f *sqlFixture) env(hook func(string) error) utxoset.MigrationEnv {
	return utxoset.MigrationEnv{
		Store: sqlStore{f},
		Mark: func(context.Context) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.marked = true
			return nil
		},
		Marked: func(context.Context) (bool, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.marked, nil
		},
		ChunkRows: 10,
		Hook:      hook,
	}
}

func openOnly(t *testing.T, dir string) *utxoset.Set {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: dir, Only: true})
	require.NoError(t, err)
	return set
}

func requireMigrated(
	t *testing.T, set *utxoset.Set, f *sqlFixture, outputs int,
) {
	t.Helper()
	n, err := set.Count()
	require.NoError(t, err)
	require.EqualValues(t, outputs, n)
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, f.tip, tip)
	require.True(t, f.marked, "the set-only mark")
	require.Zero(t, f.remaining(), "utxo rows left")
	require.NotContains(t, f.sync, utxoset.MigrationSyncKey)
}

// A database that holds its UTxO set in the metadata store moves it into the
// live UTxO set, with the set-only mark written before any row is deleted.
func TestMigrateFromSQLMovesTheUtxoSet(t *testing.T) {
	t.Parallel()
	const outputs = 25
	f := newSQLFixture(outputs)
	set := openOnly(t, t.TempDir())
	t.Cleanup(func() { _ = set.Close() })
	var sawRows []int
	err := set.MigrateFromSQL(t.Context(), f, f.env(func(step string) error {
		if step == "marked" {
			require.True(t, f.marked)
			sawRows = append(sawRows, f.remaining())
		}
		return nil
	}))
	require.NoError(t, err)
	require.Equal(t, []int{outputs}, sawRows, "rows deleted before the mark")
	requireMigrated(t, set, f, outputs)
}

func TestMigrateFromSQLInlineCleanupUsesTheCompletedMark(t *testing.T) {
	t.Parallel()
	const outputs = 25
	f := newSQLFixture(outputs)
	set := openOnly(t, t.TempDir())
	t.Cleanup(func() { _ = set.Close() })
	env := f.env(nil)
	env.Marked = nil

	require.NoError(t, set.MigrateFromSQL(t.Context(), f, env))
	requireMigrated(t, set, f, outputs)
}

// A node can finish its cutover before deleting historical SQL rows. The
// durable commit and set-only mark make the cleanup safe, and its progress
// record remains until a managed worker completes every batch.
func TestMigrateFromSQLDefersRowCleanupAfterCutover(t *testing.T) {
	t.Parallel()
	const outputs = 25
	f := newSQLFixture(outputs)
	set := openOnly(t, t.TempDir())
	t.Cleanup(func() { _ = set.Close() })
	env := f.env(nil)
	env.DeferCleanup = true

	require.NoError(t, set.MigrateFromSQL(t.Context(), f, env))
	require.True(t, f.marked)
	require.Equal(t, outputs, f.remaining())
	require.Equal(
		t,
		"committed",
		f.sync[utxoset.MigrationSyncKey],
	)

	require.NoError(t, set.CleanupMigratedSQLRows(t.Context(), env))
	requireMigrated(t, set, f, outputs)
}

// A migration whose progress is committed and whose set-only mark is durable
// resumes over drained ledger-state families, because it never reads the SQL
// rows again; an uncommitted one must refuse, since the rows are no longer a
// faithful source.
func TestMigrateFromSQLResumesCommittedMigrationOverDrainedState(t *testing.T) {
	t.Parallel()
	const outputs = 25
	for _, deferCleanup := range []bool{false, true} {
		t.Run(fmt.Sprintf("defer=%t", deferCleanup), func(t *testing.T) {
			t.Parallel()
			f := newSQLFixture(outputs)
			set := openOnly(t, t.TempDir())
			t.Cleanup(func() { _ = set.Close() })
			env := f.env(nil)
			require.NoError(t, set.MigrateFromSQL(t.Context(), f, env))
			requireMigrated(t, set, f, outputs)

			// Rebuild a crash after the mark: rows and progress remain.
			f.mu.Lock()
			f.sync[utxoset.MigrationSyncKey] = "committed"
			f.rows = newSQLFixture(outputs).rows
			f.mu.Unlock()
			set.SetDrainedFamilies(nodesettings.AllFamilies())
			env.DeferCleanup = deferCleanup

			require.NoError(t, set.MigrateFromSQL(t.Context(), f, env))
			if deferCleanup {
				require.Equal(t, outputs, f.remaining())
				require.Equal(t, "committed", f.sync[utxoset.MigrationSyncKey])
				require.NoError(t, set.CleanupMigratedSQLRows(t.Context(), env))
			}
			requireMigrated(t, set, f, outputs)
		})
	}
}

func TestMigrateFromSQLRefusesUnmarkedMigrationOverDrainedState(t *testing.T) {
	t.Parallel()
	f := newSQLFixture(5)
	set := openOnly(t, t.TempDir())
	t.Cleanup(func() { _ = set.Close() })
	set.SetDrainedFamilies(nodesettings.AllFamilies())

	err := set.MigrateFromSQL(t.Context(), f, f.env(nil))

	require.ErrorContains(t, err, "drained ledger-state families")
	require.Equal(t, 5, f.remaining())
	require.False(t, f.marked)
}

// A stop between any two steps, as a crash would cause, leaves the metadata
// store intact until the mark, and the next run finishes the migration.
func TestMigrateFromSQLResumesAfterACrashAtEveryStep(t *testing.T) {
	t.Parallel()
	const outputs = 25
	errCrash := errors.New("crash")
	for _, step := range []string{
		"scanned", "bootstrapped", "verified", "committed", "marked",
		"drain:1", "drain:2",
	} {
		t.Run(step, func(t *testing.T) {
			t.Parallel()
			f := newSQLFixture(outputs)
			dir := t.TempDir()
			set := openOnly(t, dir)
			err := set.MigrateFromSQL(t.Context(), f, f.env(
				func(s string) error {
					if s == step {
						return errCrash
					}
					return nil
				},
			))
			require.ErrorIs(t, err, errCrash)
			switch step {
			case "scanned", "bootstrapped", "verified", "committed":
				require.False(t, f.marked, "marked before the crash point")
				require.Equal(t, outputs, f.remaining())
			case "marked":
				require.Equal(t, outputs, f.remaining())
			default:
				require.Less(t, f.remaining(), outputs)
				require.True(t, f.marked)
			}
			require.NoError(t, set.Close())

			set = openOnly(t, dir)
			t.Cleanup(func() { _ = set.Close() })
			require.NoError(t, set.MigrateFromSQL(t.Context(), f, f.env(nil)))
			requireMigrated(t, set, f, outputs)
		})
	}
}

// A set kept from an interrupted run that no longer matches the metadata
// store is rebuilt before anything is marked or deleted.
func TestMigrateFromSQLRebuildsAKeptSetThatDiffers(t *testing.T) {
	t.Parallel()
	const outputs = 12
	errCrash := errors.New("crash")
	f := newSQLFixture(outputs)
	dir := t.TempDir()
	set := openOnly(t, dir)
	require.ErrorIs(t, set.MigrateFromSQL(t.Context(), f, f.env(
		func(s string) error {
			if s == "bootstrapped" {
				return errCrash
			}
			return nil
		},
	)), errCrash)
	// An extra output at the same tip: the kept set no longer matches.
	tip := slotPoint(50)
	require.NoError(t, set.ApplyUtxoChanges(&tip, []ledger.UtxoEvent{{
		Point: tip,
		Added: []ledger.UtxoOutput{{
			Ref:  models.UtxoId{Hash: bytes.Repeat([]byte{0xfe}, 32)},
			Cbor: []byte{0x80},
		}},
	}}))
	require.NoError(t, set.Close())
	set = openOnly(t, dir)
	t.Cleanup(func() { _ = set.Close() })
	require.NoError(t, set.MigrateFromSQL(t.Context(), f, f.env(nil)))
	requireMigrated(t, set, f, outputs)
}

// A set that does not match the metadata store after a rebuild is never
// marked and no row is deleted.
func TestMigrateFromSQLRefusesToMarkAMismatch(t *testing.T) {
	t.Parallel()
	f := newSQLFixture(8)
	f.flaky = true
	set := openOnly(t, t.TempDir())
	t.Cleanup(func() { _ = set.Close() })
	err := set.MigrateFromSQL(t.Context(), f, f.env(nil))
	require.ErrorContains(t, err, "does not match")
	require.False(t, f.marked)
	require.Equal(t, 8, f.remaining())
	require.NotEqual(t, "committed", f.sync[utxoset.MigrationSyncKey])
}

// The migration stops before writing anything when the disk cannot hold the
// set.
func TestMigrateFromSQLChecksFreeSpace(t *testing.T) {
	t.Parallel()
	f := newSQLFixture(8)
	set := openOnly(t, t.TempDir())
	t.Cleanup(func() { _ = set.Close() })
	env := f.env(nil)
	env.FreeBytes = func() (uint64, error) { return 1, nil }
	err := set.MigrateFromSQL(t.Context(), f, env)
	require.ErrorIs(t, err, utxoset.ErrMigrationInsufficientSpace)
	_, ok, err := set.Tip()
	require.NoError(t, err)
	require.False(t, ok, "the set was built despite the space check")
	require.False(t, f.marked)
	require.Equal(t, 8, f.remaining())
}

// applyFixtureRows applies the fixture's rows to the set one per slot ending
// at the fixture's tip, so the set matches the metadata store and holds a
// journal behind its tip.
func applyFixtureRows(t *testing.T, set *utxoset.Set, f *sqlFixture) {
	t.Helper()
	first := f.tip.Slot - uint64(len(f.rows)) + 1
	for i, row := range f.rows {
		tip := slotPoint(first + uint64(i))
		require.NoError(t, set.ApplyUtxoChanges(&tip, []ledger.UtxoEvent{{
			Point: tip,
			Added: []ledger.UtxoOutput{{
				Ref:  models.UtxoId{Hash: row.TxId, Idx: row.OutputIdx},
				Cbor: row.Cbor,
			}},
		}}))
	}
}

// A set that already equals the metadata store at its tip keeps the rollback
// journal it holds.
func TestMigrateFromSQLKeepsAMatchingSetAndItsJournal(t *testing.T) {
	t.Parallel()
	const outputs = 6
	f := newSQLFixture(outputs)
	set := openOnly(t, t.TempDir())
	t.Cleanup(func() { _ = set.Close() })
	applyFixtureRows(t, set, f)
	require.NoError(t, set.MigrateFromSQL(t.Context(), f, f.env(nil)))
	requireMigrated(t, set, f, outputs)
	_, err := set.Rollback(slotPoint(f.tip.Slot - 2))
	require.NoError(t, err, "the migration dropped the set's rollback journal")
	n, err := set.Count()
	require.NoError(t, err)
	require.EqualValues(t, outputs-2, n)
}

// A rebuild of a set that already holds data needs only the space the data does
// not already occupy.
func TestMigrateFromSQLCountsTheBuiltSetInItsSpaceCheck(t *testing.T) {
	t.Parallel()
	const outputs = 400
	f := newSQLFixture(outputs)
	dir := t.TempDir()
	set := openOnly(t, dir)
	applyFixtureRows(t, set, f)
	// One extra output makes the kept set differ, so it is rebuilt.
	tip := f.tip
	require.NoError(t, set.ApplyUtxoChanges(&tip, []ledger.UtxoEvent{{
		Point: tip,
		Added: []ledger.UtxoOutput{{
			Ref:  models.UtxoId{Hash: bytes.Repeat([]byte{0xfe}, 32)},
			Cbor: []byte{0x80},
		}},
	}}))
	// Reopening flushes the data to table files, which the size counts.
	require.NoError(t, set.Close())
	set = openOnly(t, dir)
	t.Cleanup(func() { _ = set.Close() })
	env := f.env(nil)
	env.FreeBytes = func() (uint64, error) { return 0, nil }
	err := set.MigrateFromSQL(t.Context(), f, env)
	require.NotErrorIs(t, err, utxoset.ErrMigrationInsufficientSpace)
	require.NoError(t, err)
}

// A migration interrupted after it recorded its commit and before the set-only
// mark ran while the node may have written to the metadata store in between,
// so the set is checked and rebuilt again before it is marked and the rows go.
func TestMigrateFromSQLChecksAgainWhenResumedFromCommitBeforeTheMark(t *testing.T) {
	t.Parallel()
	const outputs = 12
	errCrash := errors.New("crash")
	f := newSQLFixture(outputs)
	dir := t.TempDir()
	set := openOnly(t, dir)
	require.ErrorIs(t, set.MigrateFromSQL(t.Context(), f, f.env(
		func(s string) error {
			if s == "committed" {
				return errCrash
			}
			return nil
		},
	)), errCrash)
	require.False(t, f.marked)
	tip := slotPoint(50)
	require.NoError(t, set.ApplyUtxoChanges(&tip, []ledger.UtxoEvent{{
		Point: tip,
		Added: []ledger.UtxoOutput{{
			Ref:  models.UtxoId{Hash: bytes.Repeat([]byte{0xfe}, 32)},
			Cbor: []byte{0x80},
		}},
	}}))
	require.NoError(t, set.Close())

	set = openOnly(t, dir)
	t.Cleanup(func() { _ = set.Close() })
	require.NoError(t, set.MigrateFromSQL(t.Context(), f, f.env(nil)))
	requireMigrated(t, set, f, outputs)
}
