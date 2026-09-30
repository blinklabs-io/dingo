//go:build dingo_db_integration

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

package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestPostgresRestorePoolStateAtSlotScopesAndReverts is the real-PostgreSQL
// counterpart to the SQLite-based scope tests in pool_restore_scope_test.go.
// See testRestorePoolStateAtSlotScopesAndReverts's doc comment for why scope
// is asserted through RowsAffected rather than an "UPDATE OF column-list"
// trigger here.
func TestPostgresRestorePoolStateAtSlotScopesAndReverts(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("DINGO_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	schema := fmt.Sprintf("sqlstore_restore_pool_%d", time.Now().UnixNano())
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})
	testRestorePoolStateAtSlotScopesAndReverts(
		t,
		"pgx",
		postgresDSNWithSearchPath(t, dsn, schema),
		"postgres",
		schema,
	)
}

// TestMySQLRestorePoolStateAtSlotScopesAndReverts is MySQL's counterpart to
// TestPostgresRestorePoolStateAtSlotScopesAndReverts. It matters
// independently of the PostgreSQL run: MySQL is the one dialect with no
// UPDATE ... FROM syntax at all, and the one whose SET targets must be
// table-qualified (dialect.go's mysqlUpdateFromJoinSQL) because latest's
// projected columns share names with pool's own.
func TestMySQLRestorePoolStateAtSlotScopesAndReverts(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("DINGO_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
	}
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	database := fmt.Sprintf("sqlstore_restore_pool_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + database + "`")
		_ = admin.Close()
	})
	testRestorePoolStateAtSlotScopesAndReverts(
		t,
		"mysql",
		mysqlDSNWithClientFoundRows(t, mysqlDSNWithDatabase(t, dsn, database)),
		"mysql",
		database,
	)
}

// mysqlDSNWithClientFoundRows sets the go-sql-driver/mysql ClientFoundRows
// option, which makes sql.Result.RowsAffected() report rows the statement's
// WHERE/JOIN matched rather than the driver's non-standard default of rows
// whose value actually changed. Without this, a JOIN that touches an
// unaffected pool but happens to reassign it its own existing values (true
// for exactly the "never re-registered" pools RestorePoolStateAtSlot must
// leave untouched) would silently not count toward RowsAffected on MySQL,
// making a scope regression invisible to this test on this one dialect even
// though PostgreSQL's and SQLite's drivers report it correctly. This affects
// only this test's own connection, not the production DSN
// (database/plugin/metadata/mysql/provider.go), which is free to keep
// MySQL's default semantics.
func mysqlDSNWithClientFoundRows(t *testing.T, dsn string) string {
	t.Helper()
	parsed, err := mysqldriver.ParseDSN(dsn)
	require.NoError(t, err)
	parsed.ClientFoundRows = true
	return parsed.FormatDSN()
}

// testRestorePoolStateAtSlotScopesAndReverts exercises
// restorePoolDenormalizedFields and restorePoolLatestOpCertSequence -- the
// literal statements RestorePoolStateAtSlot issues, not a reimplementation of
// them -- against a real backend, asserting scope from the RowsAffected each
// dialect's driver reports for the ExecContext call rather than from an
// "UPDATE OF column-list" trigger: SQLite's OF-clause semantics (fire only
// when the statement's SET list names the column) are not a guarantee this
// package can rely on being portable to MySQL's row-trigger firing rules
// without separately validating them, and RowsAffected is unambiguous on
// every driver here since every assignment in this fixture is a genuine
// value change. It sequences the statements exactly as
// RestorePoolStateAtSlot's own body does (query affected op-cert
// pool_key_hashes before deleting the rows that would otherwise make them
// unrecoverable), so this reaches the same translated SQL text
// dialectQueryer.translate produces for a live call -- MySQL's
// "transaction" reserved-identifier rewrite and CTE-in-UPDATE handling,
// PostgreSQL's ? -> $N rebind -- not a hand-simplified stand-in for it.
func testRestorePoolStateAtSlotScopesAndReverts(
	t *testing.T,
	driver, dsn, dialectName, lockNamespace string,
) {
	t.Helper()
	store := newIntegrationSQLStore(t, driver, dsn, dialectName, lockNamespace)
	ctx := context.Background()

	const targetSlot = 1500

	seedUnaffected := func(marker byte) {
		hash := bytes.Repeat([]byte{marker}, 28)
		pool := &models.Pool{
			PoolKeyHash:   hash,
			Pledge:        100,
			Cost:          200,
			Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
			VrfKeyHash:    bytes.Repeat([]byte{0xa0}, 32),
			RewardAccount: bytes.Repeat([]byte{0xb0}, 28),
		}
		reg := &models.PoolRegistration{
			PoolKeyHash:   hash,
			AddedSlot:     1000,
			Pledge:        pool.Pledge,
			Cost:          pool.Cost,
			Margin:        pool.Margin,
			VrfKeyHash:    pool.VrfKeyHash,
			RewardAccount: pool.RewardAccount,
		}
		require.NoError(t, store.ImportPool(pool, reg, nil))
	}
	for _, marker := range []byte{0x10, 0x11, 0x12} {
		seedUnaffected(marker)
	}

	affectedHash := bytes.Repeat([]byte{0x99}, 28)
	before := &models.Pool{
		PoolKeyHash:                affectedHash,
		Pledge:                     100,
		Cost:                       200,
		Margin:                     &types.Rat{Rat: big.NewRat(1, 100)},
		VrfKeyHash:                 bytes.Repeat([]byte{0xa1}, 32),
		RewardAccount:              bytes.Repeat([]byte{0xb1}, 28),
		RewardAccountCredentialTag: 0,
		LeiosKeyPublic:             bytes.Repeat([]byte{0xc1}, 96),
		LeiosKeyPossessionProof:    bytes.Repeat([]byte{0xd1}, 48),
	}
	beforeReg := &models.PoolRegistration{
		PoolKeyHash:                affectedHash,
		AddedSlot:                  1000,
		Pledge:                     before.Pledge,
		Cost:                       before.Cost,
		Margin:                     before.Margin,
		VrfKeyHash:                 before.VrfKeyHash,
		RewardAccount:              before.RewardAccount,
		RewardAccountCredentialTag: before.RewardAccountCredentialTag,
		LeiosKeyPublic:             before.LeiosKeyPublic,
		LeiosKeyPossessionProof:    before.LeiosKeyPossessionProof,
	}
	require.NoError(t, store.ImportPool(before, beforeReg, nil))

	after := &models.Pool{
		PoolKeyHash:                affectedHash,
		Pledge:                     999,
		Cost:                       888,
		Margin:                     &types.Rat{Rat: big.NewRat(2, 100)},
		VrfKeyHash:                 bytes.Repeat([]byte{0xa2}, 32),
		RewardAccount:              bytes.Repeat([]byte{0xb2}, 28),
		RewardAccountCredentialTag: 1,
		LeiosKeyPublic:             bytes.Repeat([]byte{0xc2}, 96),
		LeiosKeyPossessionProof:    bytes.Repeat([]byte{0xd2}, 48),
	}
	afterReg := &models.PoolRegistration{
		PoolKeyHash:                affectedHash,
		AddedSlot:                  2000,
		Pledge:                     after.Pledge,
		Cost:                       after.Cost,
		Margin:                     after.Margin,
		VrfKeyHash:                 after.VrfKeyHash,
		RewardAccount:              after.RewardAccount,
		RewardAccountCredentialTag: after.RewardAccountCredentialTag,
		LeiosKeyPublic:             after.LeiosKeyPublic,
		LeiosKeyPossessionProof:    after.LeiosKeyPossessionProof,
	}
	require.NoError(t, store.ImportPool(after, afterReg, nil))

	// One op-cert row below the target survives; the one above it is
	// discarded, so latest_op_cert_sequence must revert to the surviving
	// value (1), not merely stay at whatever UpdatePoolOpCertSequence left it
	// (5).
	require.NoError(t, store.UpdatePoolOpCertSequence(
		lcommon.PoolKeyHash(affectedHash), 1, 500, nil,
	))
	require.NoError(t, store.UpdatePoolOpCertSequence(
		lcommon.PoolKeyHash(affectedHash), 5, 2000, nil,
	))

	db := store.instrumentedQueryer(store.writeDB)

	// Mirrors RestorePoolStateAtSlot's own statement order: the affected
	// pool_key_hash set must be captured before the delete removes the rows
	// it is computed from.
	affectedOpCertHashes, err := queryPoolKeyHashesWithOpCertAfterSlot(
		ctx, db, targetSlot,
	)
	require.NoError(t, err)
	_, err = db.ExecContext(
		ctx, "DELETE FROM pool_opcert_sequence WHERE slot > ?", targetSlot,
	)
	require.NoError(t, err)

	denormRows, err := store.restorePoolDenormalizedFields(ctx, db, targetSlot)
	require.NoError(t, err)
	require.Equal(
		t,
		int64(1),
		denormRows,
		"exactly the one pool with a discarded post-target registration",
	)

	opCertRows, err := store.restorePoolLatestOpCertSequence(
		ctx, db, affectedOpCertHashes,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), opCertRows)

	restored, err := store.GetPool(lcommon.PoolKeyHash(affectedHash), true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), uint64(restored.Pledge))
	require.Equal(t, uint64(200), uint64(restored.Cost))
	require.Equal(t, big.NewRat(1, 100).String(), restored.Margin.String())
	require.Equal(t, before.VrfKeyHash, restored.VrfKeyHash)
	require.Equal(t, before.RewardAccount, restored.RewardAccount)
	require.Equal(
		t,
		before.RewardAccountCredentialTag,
		restored.RewardAccountCredentialTag,
	)
	require.Equal(t, before.LeiosKeyPublic, restored.LeiosKeyPublic)
	require.Equal(
		t,
		before.LeiosKeyPossessionProof,
		restored.LeiosKeyPossessionProof,
	)
	require.Equal(t, uint64(1), restored.LatestOpCertSequence)

	// Exercise the real public entry point too, not only the two statements
	// above: this is what confirms withWriteTransaction's transaction
	// wiring and dialectQueryer's translation work end-to-end against a
	// live backend, which calling restorePoolDenormalizedFields/
	// restorePoolLatestOpCertSequence directly does not. Everything above
	// already put the database into the fully-restored state, so this call
	// has nothing left to revert -- it exists to prove RestorePoolStateAtSlot
	// itself runs cleanly on this dialect end-to-end, not to add a further
	// scope or value assertion.
	require.NoError(t, store.RestorePoolStateAtSlot(targetSlot, nil))

	restoredAgain, err := store.GetPool(
		lcommon.PoolKeyHash(affectedHash), true, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(100), uint64(restoredAgain.Pledge))
	require.Equal(t, uint64(1), restoredAgain.LatestOpCertSequence)
}
