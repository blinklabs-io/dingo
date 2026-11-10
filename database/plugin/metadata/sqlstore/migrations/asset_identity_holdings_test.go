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

package migrations_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

const assetSplitVersion = 59

// legacyAssetRow is one row of the pre-split asset table. A nil field is
// stored as NULL.
type legacyAssetRow struct {
	utxo   any
	policy any
	name   any
	amount any
}

func assetSplitPolicy(b byte) []byte {
	out := make([]byte, 28)
	for i := range out {
		out[i] = b
	}
	return out
}

// assetSplitDB returns a database migrated through the version before the
// split, holding utxo rows 1..utxos and the given legacy asset rows (ids
// follow the slice order), and the path it lives at.
func assetSplitDB(
	t *testing.T,
	utxos int,
	assets []legacyAssetRow,
) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, assetSplitVersion-1)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	for id := 1; id <= utxos; id++ {
		txID := make([]byte, 32)
		txID[0] = byte(id)
		_, err := db.Exec(`
INSERT INTO utxo (id, tx_id, output_idx, added_slot, deleted_slot, amount)
VALUES (?, ?, 0, 1, 0, '1')`, id, txID)
		require.NoError(t, err)
	}
	for _, row := range assets {
		_, err := db.Exec(`
INSERT INTO asset (name, policy_id, fingerprint, utxo_id, amount)
VALUES (?, ?, ?, ?, ?)`, row.name, row.policy, []byte("fp"), row.utxo,
			row.amount)
		require.NoError(t, err)
	}
	return db, path
}

func assetSplitRunner(
	t *testing.T,
	db *sql.DB,
	path string,
	batchSize int,
	wrap func(migrations.Backfill) migrations.Backfill,
) migrations.Runner {
	t.Helper()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	migration := &registry[assetSplitVersion-1]
	require.Equal(t, "asset-identity-holdings", migration.Name)
	require.NotNil(t, migration.Backfill)
	if batchSize > 0 {
		migration.BatchSize = batchSize
	}
	if wrap != nil {
		migration.Backfill = wrap(migration.Backfill)
	}
	return migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
	}
}

func runAssetSplit(t *testing.T, db *sql.DB, path string) {
	t.Helper()
	runner := assetSplitRunner(t, db, path, 0, nil)
	require.NoError(t, runner.Run(context.Background()))
}

// assetHoldings renders every holding as policy/name/utxo/amount text, with
// the amount's storage class, in a stable order.
func assetHoldings(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`
SELECT lower(hex(i.policy_id)), lower(hex(i.name)), h.utxo_id, h.amount, typeof(h.amount)
FROM asset_holding h JOIN asset_identity i ON i.id = h.asset_id
ORDER BY h.utxo_id, hex(i.policy_id), hex(i.name)`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var out []string
	for rows.Next() {
		var policy, name, class string
		var utxo int64
		var amount sql.NullString
		require.NoError(t, rows.Scan(&policy, &name, &utxo, &amount, &class))
		out = append(out, fmt.Sprintf(
			"%s/%s/%d/%q/%s", policy, name, utxo, amount.String, class,
		))
	}
	require.NoError(t, rows.Err())
	return out
}

func sqliteObjectNames(t *testing.T, db *sql.DB, kind string) []string {
	t.Helper()
	rows, err := db.Query(
		`SELECT name FROM sqlite_master
WHERE type = ? AND name IN ('asset', 'asset_holding', 'asset_identity')
   OR (type = ? AND name LIKE 'idx_asset%')
ORDER BY name`, kind, kind,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var out []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		out = append(out, name)
	}
	require.NoError(t, rows.Err())
	return out
}

// TestAssetIdentityHoldingsCopiesHoldings upgrades a database holding one
// asset on several UTxOs, the empty name, and NULL names and policies, and
// checks one identity per distinct pair, one holding per row, the legacy table
// and its indexes gone, and the holdings table stored without a rowid.
func TestAssetIdentityHoldingsCopiesHoldings(t *testing.T) {
	t.Parallel()
	shared, other := assetSplitPolicy(0x01), assetSplitPolicy(0x02)
	db, path := assetSplitDB(t, 4, []legacyAssetRow{
		{1, shared, []byte("tok"), "5"},
		{2, shared, []byte("tok"), "6"},
		{3, shared, []byte("tok"), "7"},
		{1, shared, []byte{}, "1"},
		{2, other, []byte("x"), "2"},
		{3, other, nil, "3"},
		{4, nil, []byte("tok"), "4"},
	})
	runAssetSplit(t, db, path)

	require.Equal(t, []string{
		fmt.Sprintf("%x/%x/1/\"1\"/text", shared, ""),
		fmt.Sprintf("%x/%x/1/\"5\"/text", shared, "tok"),
		fmt.Sprintf("%x/%x/2/\"6\"/text", shared, "tok"),
		fmt.Sprintf("%x/%x/2/\"2\"/text", other, "x"),
		fmt.Sprintf("%x/%x/3/\"7\"/text", shared, "tok"),
		fmt.Sprintf("%x/%x/3/\"3\"/text", other, ""),
		fmt.Sprintf("%x/%x/4/\"4\"/text", "", "tok"),
	}, assetHoldings(t, db))

	var identities, distinct int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*), COUNT(DISTINCT hex(policy_id) || '/' || hex(name))
FROM asset_identity`,
	).Scan(&identities, &distinct))
	require.Equal(t, 5, identities, "one identity per distinct pair")
	require.Equal(t, identities, distinct)

	require.Equal(t, []string{"asset_holding", "asset_identity"},
		sqliteObjectNames(t, db, "table"),
		"the legacy asset table is dropped, and its indexes with it")
	for _, name := range sqliteObjectNames(t, db, "index") {
		require.NotContains(t, []string{
			"idx_asset_unique", "idx_asset_policy_id", "idx_asset_utxo_id",
		}, name)
	}
	var definition string
	require.NoError(t, db.QueryRow(
		`SELECT sql FROM sqlite_master WHERE name = 'asset_holding'`,
	).Scan(&definition))
	require.Contains(t, strings.ToUpper(definition), "WITHOUT ROWID")
	var indexSQL string
	require.NoError(t, db.QueryRow(
		`SELECT sql FROM sqlite_master WHERE name = 'idx_asset_holding_asset_id'`,
	).Scan(&indexSQL))
	require.Contains(t, indexSQL, "asset_holding")
}

// TestAssetIdentityHoldingsCopiesAmountsVerbatim keeps the amount text as
// stored, including NULL and text that is not a canonical decimal.
func TestAssetIdentityHoldingsCopiesAmountsVerbatim(t *testing.T) {
	t.Parallel()
	policy := assetSplitPolicy(0x03)
	db, path := assetSplitDB(t, 6, []legacyAssetRow{
		{1, policy, []byte("a"), nil},
		{2, policy, []byte("a"), "007"},
		{3, policy, []byte("a"), " 5"},
		{4, policy, []byte("a"), "18446744073709551615"},
		{5, policy, []byte("a"), "9223372036854775808"},
		{6, policy, []byte("a"), ""},
	})
	runAssetSplit(t, db, path)

	hexPolicy := fmt.Sprintf("%x", policy)
	require.Equal(t, []string{
		hexPolicy + "/61/1/\"\"/null",
		hexPolicy + "/61/2/\"007\"/text",
		hexPolicy + "/61/3/\" 5\"/text",
		hexPolicy + "/61/4/\"18446744073709551615\"/text",
		hexPolicy + "/61/5/\"9223372036854775808\"/text",
		hexPolicy + "/61/6/\"\"/text",
	}, assetHoldings(t, db))
}

// TestAssetIdentityHoldingsDropsOrphans drops asset rows whose UTxO is gone or
// unset, as the foreign key would have, and keeps the rest.
func TestAssetIdentityHoldingsDropsOrphans(t *testing.T) {
	t.Parallel()
	policy := assetSplitPolicy(0x04)
	db, path := assetSplitDB(t, 2, []legacyAssetRow{
		{1, policy, []byte("a"), "1"},
		{99, policy, []byte("a"), "2"},
		{nil, policy, []byte("a"), "3"},
		{2, policy, []byte("b"), "4"},
	})
	runAssetSplit(t, db, path)

	hexPolicy := fmt.Sprintf("%x", policy)
	require.Equal(t, []string{
		hexPolicy + "/61/1/\"1\"/text",
		hexPolicy + "/62/2/\"4\"/text",
	}, assetHoldings(t, db))
	var cursor string
	require.NoError(t, db.QueryRow(
		`SELECT cursor FROM schema_migrations WHERE version = ?`,
		assetSplitVersion,
	).Scan(&cursor))
	require.Contains(t, cursor, `"orphans":2`)
	require.Contains(t, cursor, `"scanned":4`)
	require.Contains(t, cursor, `"copied":2`)
}

// TestAssetIdentityHoldingsMergesIdenticalDuplicates merges rows that land on
// one (utxo, policy, name), which happens when a NULL name and the empty name
// held the same asset, and counts the coalesced rows.
func TestAssetIdentityHoldingsMergesIdenticalDuplicates(t *testing.T) {
	t.Parallel()
	policy := assetSplitPolicy(0x05)
	db, path := assetSplitDB(t, 1, []legacyAssetRow{
		{1, policy, []byte{}, "9"},
		{1, policy, nil, "9"},
	})
	runAssetSplit(t, db, path)

	require.Equal(t, []string{
		fmt.Sprintf("%x//1/\"9\"/text", policy),
	}, assetHoldings(t, db))
	var cursor string
	require.NoError(t, db.QueryRow(
		`SELECT cursor FROM schema_migrations WHERE version = ?`,
		assetSplitVersion,
	).Scan(&cursor))
	require.Contains(t, cursor, `"merged":1`)
	require.Contains(t, cursor, `"coalesced":1`)
}

// TestAssetIdentityHoldingsRefusesConflictingDuplicates fails the migration
// when two rows for one (utxo, policy, name) disagree on the amount, and
// leaves the legacy table in place.
func TestAssetIdentityHoldingsRefusesConflictingDuplicates(t *testing.T) {
	t.Parallel()
	policy := assetSplitPolicy(0x06)
	db, path := assetSplitDB(t, 1, []legacyAssetRow{
		{1, policy, []byte{}, "9"},
		{1, policy, nil, "10"},
	})
	runner := assetSplitRunner(t, db, path, 0, nil)
	err := runner.Run(context.Background())
	require.ErrorContains(t, err, "different amounts")

	var legacy int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM asset`).Scan(&legacy))
	require.Equal(t, 2, legacy, "the source must survive a failed copy")
}

// TestAssetIdentityHoldingsResumesAfterInterruption cancels the backfill
// partway, resumes, and requires the holdings to equal those of an
// uninterrupted run.
func TestAssetIdentityHoldingsResumesAfterInterruption(t *testing.T) {
	t.Parallel()
	assets, utxos := manyLegacyAssets()

	wantDB, wantPath := assetSplitDB(t, utxos, assets)
	runAssetSplit(t, wantDB, wantPath)
	want := assetHoldings(t, wantDB)
	require.Len(t, want, len(assets))

	db, path := assetSplitDB(t, utxos, assets)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	interrupted := assetSplitRunner(t, db, path, 7,
		func(inner migrations.Backfill) migrations.Backfill {
			return func(
				ctx context.Context,
				batch migrations.Batch,
			) (migrations.BatchResult, error) {
				calls++
				if calls == 4 {
					cancel()
				}
				return inner(ctx, batch)
			}
		})
	require.Error(t, interrupted.Run(ctx))
	var phase, cursor string
	require.NoError(t, db.QueryRow(
		`SELECT phase, cursor FROM schema_migrations WHERE version = ?`,
		assetSplitVersion,
	).Scan(&phase, &cursor))
	require.Equal(t, "backfill", phase)
	require.Contains(t, cursor, `"last_id":21`,
		"three committed batches of seven rows")
	var partial int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM asset_holding`,
	).Scan(&partial))
	require.Equal(t, 21, partial)

	resume := assetSplitRunner(t, db, path, 7, nil)
	require.NoError(t, resume.Run(context.Background()))
	require.Equal(t, want, assetHoldings(t, db))
}

// TestAssetIdentityHoldingsRefusesSkippedRange moves the cursor past rows the
// copy never handled and requires the migration to refuse rather than drop
// them with the legacy table.
func TestAssetIdentityHoldingsRefusesSkippedRange(t *testing.T) {
	t.Parallel()
	assets, utxos := manyLegacyAssets()
	db, path := assetSplitDB(t, utxos, assets)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	interrupted := assetSplitRunner(t, db, path, 7,
		func(inner migrations.Backfill) migrations.Backfill {
			return func(
				ctx context.Context,
				batch migrations.Batch,
			) (migrations.BatchResult, error) {
				calls++
				if calls == 3 {
					cancel()
				}
				return inner(ctx, batch)
			}
		})
	require.Error(t, interrupted.Run(ctx))

	// Skip rows 15..28: the cursor claims them handled.
	_, err := db.Exec(`
UPDATE schema_migrations
SET cursor = replace(cursor, '"last_id":14', '"last_id":28')
WHERE version = ?`, assetSplitVersion)
	require.NoError(t, err)

	resume := assetSplitRunner(t, db, path, 7, nil)
	err = resume.Run(context.Background())
	require.ErrorContains(t, err, "asset split is incomplete")
	var legacy int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM asset`).Scan(&legacy))
	require.Equal(t, len(assets), legacy, "the source must not be dropped")
}

func manyLegacyAssets() ([]legacyAssetRow, int) {
	const utxos = 10
	var assets []legacyAssetRow
	for i := range 40 {
		assets = append(assets, legacyAssetRow{
			utxo:   i%utxos + 1,
			policy: assetSplitPolicy(byte(i % 3)),
			name:   []byte{byte('a' + i/utxos)},
			amount: fmt.Sprint(i + 1),
		})
	}
	return assets, utxos
}

// TestAssetIdentityHoldingsExpandReplays reruns the expand phase of a
// finished migration, as an interrupted upgrade does, and requires the data
// and schema to survive.
func TestAssetIdentityHoldingsExpandReplays(t *testing.T) {
	t.Parallel()
	policy := assetSplitPolicy(0x07)
	db, path := assetSplitDB(t, 2, []legacyAssetRow{
		{1, policy, []byte("a"), "1"},
		{2, policy, []byte("a"), "2"},
	})
	runAssetSplit(t, db, path)
	want := assetHoldings(t, db)

	_, err := db.Exec(`
UPDATE schema_migrations
SET phase = 'expand', dirty = 1, completed_at = NULL WHERE version = ?`,
		assetSplitVersion)
	require.NoError(t, err)
	runAssetSplit(t, db, path)
	require.Equal(t, want, assetHoldings(t, db))
}

// TestAssetIdentityHoldingsBuildsIndexesWithDeferredMarkerPending migrates a
// database whose deferred-index marker is pending, as an interrupted bulk load
// leaves it, and requires the new holdings index to exist at once: the
// migration builds it in its own expand phase and never leaves it for the
// deferred rebuild.
func TestAssetIdentityHoldingsBuildsIndexesWithDeferredMarkerPending(
	t *testing.T,
) {
	t.Parallel()
	policy := assetSplitPolicy(0x08)
	db, path := assetSplitDB(t, 1, []legacyAssetRow{
		{1, policy, []byte("a"), "1"},
	})
	_, err := db.Exec(`DROP INDEX idx_asset_policy_id`)
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO sync_state (sync_key, value) VALUES (?, 'true')`,
		deferred.SyncStateKey,
	)
	require.NoError(t, err)

	runAssetSplit(t, db, path)

	var count int
	require.NoError(t, db.QueryRow(`
SELECT COUNT(*) FROM sqlite_master
WHERE type = 'index' AND name = 'idx_asset_holding_asset_id'`).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM sync_state WHERE sync_key = ?`,
		deferred.SyncStateKey,
	).Scan(&count))
	require.Equal(t, 1, count, "the migration leaves the marker to its owner")
	require.Len(t, assetHoldings(t, db), 1)
}
