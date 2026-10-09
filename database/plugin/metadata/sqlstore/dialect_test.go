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
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestSQLiteRestoreModeKeepsRaisedWALAutocheckpoint guards against
// RestoreNormalMode silently undoing the checkpoint-threshold fix in
// database/plugin/metadata/sqlite/shared_sqlstore.go's sqliteCommonPragmas:
// a bulk load (SetBulkMode) must leave a connection at the same
// wal_autocheckpoint every ordinary connection already runs with, not
// SQLite's much smaller compiled-in default, once RestoreNormalMode runs
// after it.
func TestSQLiteRestoreModeKeepsRaisedWALAutocheckpoint(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	dialect := SQLiteDialect()
	ctx := context.Background()
	require.NoError(t, dialect.SetBulkMode(ctx, db))
	require.NoError(t, dialect.RestoreNormalMode(ctx, db))

	var pages int
	require.NoError(t, db.QueryRow("PRAGMA wal_autocheckpoint").Scan(&pages))
	require.Equal(t, 10000, pages)
}

func TestPostgresRebind(t *testing.T) {
	t.Parallel()
	query := `SELECT ?, '?', "?", value -- ?
FROM things WHERE a = ? AND note = 'it''s ?' /* ? */ AND b = ?`
	require.Equal(
		t,
		`SELECT $1, '?', "?", value -- ?
FROM things WHERE a = $2 AND note = 'it''s ?' /* ? */ AND b = $3`,
		PostgresDialect().Rebind(query),
	)
}

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()
	require.Equal(t, `"a""b"`, SQLiteDialect().QuoteIdentifier(`a"b`))
	require.Equal(t, "`a``b`", MySQLDialect().QuoteIdentifier("a`b"))
}

// TestTranslateMySQLUpsertRewritesBigintCast covers the "AS BIGINT" cast
// added for sumCredentialUtxoStake: SQLite and PostgreSQL both accept BIGINT
// directly, but MySQL's CAST() has no BIGINT spelling, so
// dialectQueryer.translate (which calls translateMySQLUpsert for every mysql
// query, not only upserts) must rewrite it to the 64-bit "AS SIGNED" form the
// same way it already does for "AS INTEGER".
func TestTranslateMySQLUpsertRewritesBigintCast(t *testing.T) {
	t.Parallel()
	query := `SELECT SUM(CAST(amount AS BIGINT)) FROM utxo WHERE credential_tag = ? AND staking_key = ? AND deleted_slot = 0`
	require.Equal(
		t,
		`SELECT SUM(CAST(amount AS SIGNED)) FROM utxo WHERE credential_tag = ? AND staking_key = ? AND deleted_slot = 0`,
		translateMySQLUpsert(query),
	)
}

func TestTranslateMySQLSyncStateUpsert(t *testing.T) {
	t.Parallel()
	query := `INSERT INTO sync_state (sync_key, value) VALUES (?, ?)
ON CONFLICT (sync_key) DO UPDATE SET value = excluded.value`
	require.Equal(
		t,
		`INSERT INTO sync_state (sync_key, value) VALUES (?, ?)
ON DUPLICATE KEY UPDATE value = VALUES(value)`,
		translateMySQLUpsert(query),
	)
}

func TestTranslateMySQLReservedIdentifiers(t *testing.T) {
	t.Parallel()
	query := `SELECT "transaction"."hash", "index" FROM "transaction"`
	require.Equal(
		t,
		"SELECT `transaction`.`hash`, `index` FROM `transaction`",
		translateMySQLReservedIdentifiers(query),
	)
}

func TestTranslateMySQLReservedIdentifiersPreservesLiteralsAndComments(
	t *testing.T,
) {
	t.Parallel()
	query := `SELECT '"not an identifier"', "transaction" -- "comment"
FROM "transaction" /* "comment" */`
	require.Equal(
		t,
		"SELECT '\"not an identifier\"', `transaction` -- \"comment\"\nFROM `transaction` /* \"comment\" */",
		translateMySQLReservedIdentifiers(query),
	)
}

func TestMySQLDeferredIndexDDLUsesPrefixes(t *testing.T) {
	t.Parallel()
	dialect := MySQLDialect()
	require.Equal(
		t,
		"CREATE INDEX `idx_addr_tx_stake_position` ON `address_transaction` (`credential_tag`, `staking_key`(255), `slot`, `tx_index`, `payment_key`(255))",
		dialect.CreateIndexSQL(
			"idx_addr_tx_stake_position",
			"address_transaction",
			[]string{
				"credential_tag", "staking_key", "slot", "tx_index", "payment_key",
			},
		),
	)
	require.Equal(
		t,
		"CREATE INDEX `idx_utxo_deleted_payment_script` ON `utxo` (`deleted_slot`, `payment_script`, `amount`(255))",
		dialect.CreateIndexSQL(
			"idx_utxo_deleted_payment_script",
			"utxo",
			[]string{"deleted_slot", "payment_script", "amount"},
		),
	)
	require.Equal(t,
		"DROP INDEX `idx_utxo_deleted_payment_script` ON `utxo`",
		dialect.DropIndexSQL("idx_utxo_deleted_payment_script", "utxo"),
	)
	require.Equal(
		t,
		"CREATE INDEX `idx_asset_mint_burn_lookup` ON `asset_mint_burn` (`policy_id`(255), `name`(255), `slot`)",
		dialect.CreateIndexSQL(
			"idx_asset_mint_burn_lookup",
			"asset_mint_burn",
			[]string{"policy_id", "name", "slot"},
		),
	)
	require.Equal(
		t,
		"CREATE INDEX `idx_asset_mint_burn_fingerprint` ON `asset_mint_burn` (`fingerprint`(255))",
		dialect.CreateIndexSQL(
			"idx_asset_mint_burn_fingerprint",
			"asset_mint_burn",
			[]string{"fingerprint"},
		),
	)
	require.False(t, dialect.CanDropIndex("idx_utxo_spent_at_tx_id", "utxo"))
	require.True(t, dialect.CanDropIndex("idx_utxo_payment_key", "utxo"))
}

func TestMySQLDoNothingUsesAnInsertedColumn(t *testing.T) {
	t.Parallel()
	query := `INSERT INTO sync_state (sync_key, value) VALUES (?, ?)
ON CONFLICT (sync_key) DO NOTHING`
	got := translateMySQLUpsert(query)
	require.Contains(t, got, "ON DUPLICATE KEY UPDATE sync_key = sync_key")
	require.NotContains(t, got, "id = id")
}

func TestMySQLReturningTranslationQuotesReservedIdentifiers(t *testing.T) {
	t.Parallel()
	query := `INSERT INTO "transaction" (hash) VALUES (?) RETURNING id`
	base, _ := translateMySQLReturning(query)
	base = translateMySQLReservedIdentifiers(base)
	require.Contains(t, base, "INSERT INTO `transaction`")
}

func TestMySQLForeignKeyIndexErrorDetection(t *testing.T) {
	t.Parallel()
	require.True(t, isMySQLForeignKeyIndexError(
		fmt.Errorf(
			"Error 1553 (HY000): Cannot drop index: needed in a foreign key constraint",
		),
	))
	require.False(t, isMySQLForeignKeyIndexError(
		fmt.Errorf("Error 1553 (HY000): unrelated DDL failure"),
	))
}

// TestNewDialectQueryerIdempotentUnderCountingQueryer proves
// newDialectQueryer's idempotence guard still recognizes an
// already-dialect-wrapped handle when countingQueryer sits on top of it, as
// Store.instrumentedQueryer produces whenever Config.PromRegistry is set.
// Without unwrapDialectQueryer, db's concrete type at this call is
// countingQueryer, not dialectQueryer, so the guard would miss it and wrap
// a second dialectQueryer around the countingQueryer -- translating every
// operational query's SQL text twice on every call, and (via
// transactionBatchAccumulator.insertTransaction's identical assertion) also
// leaving MySQL's own dialect check unable to see through the wrapper.
func TestNewDialectQueryerIdempotentUnderCountingQueryer(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	inner := dialectQueryer{queryer: db, dialect: "postgres"}
	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "test_unwrap_dialect_queryer_total"},
		[]string{"op"},
	)
	wrapped := countingQueryer{queryer: inner, counter: counter}

	got := newDialectQueryer(wrapped, "postgres")
	gotWrapped, ok := got.(countingQueryer)
	require.True(
		t,
		ok,
		"expected newDialectQueryer to return the countingQueryer unchanged, not re-wrap it in another dialectQueryer",
	)
	_, isDialect := gotWrapped.queryer.(dialectQueryer)
	require.True(t, isDialect)
}

// TestUnwrapDialectQueryerFindsDialectUnderCountingQueryer is the direct
// unit test for the helper transaction_write.go's insertTransaction and
// newDialectQueryer's idempotence guard both rely on to see past
// countingQueryer.
func TestUnwrapDialectQueryerFindsDialectUnderCountingQueryer(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	inner := dialectQueryer{queryer: db, dialect: "mysql"}
	wrapped := countingQueryer{queryer: inner, counter: nil}

	got, ok := unwrapDialectQueryer(wrapped)
	require.True(t, ok)
	require.Equal(t, "mysql", got.dialect)
}

// TestUpdateFromJoinSQLStandardFormUnqualifiesAssignmentTargets covers the
// SQLite/PostgreSQL shape: both accept UPDATE ... SET ... FROM ... WHERE, and
// both reject a table-qualified assignment target ("UPDATE t SET t.col = ..."
// is a syntax error on both), unlike MySQL's JOIN form below.
func TestUpdateFromJoinSQLStandardFormUnqualifiesAssignmentTargets(
	t *testing.T,
) {
	t.Parallel()
	for _, dialect := range []Dialect{SQLiteDialect(), PostgresDialect()} {
		got := dialect.UpdateFromJoinSQL(
			"pool", "latest", "latest.pool_id = pool.id",
			[]JoinAssignment{
				{Column: "pledge", Expr: "latest.pledge"},
				{Column: "cost", Expr: "latest.cost"},
			},
		)
		require.Equal(
			t,
			"UPDATE pool SET pledge = latest.pledge, cost = latest.cost "+
				"FROM latest WHERE latest.pool_id = pool.id",
			got,
			dialect.Name(),
		)
	}
}

// TestUpdateFromJoinSQLMySQLQualifiesAssignmentTargets covers MySQL's lack of
// UPDATE ... FROM syntax: it needs UPDATE ... JOIN ... ON ... SET ..., and
// every assignment target must be qualified with the target table, since the
// joined source here projects columns with the same names as the target's
// own and an unqualified SET is ambiguous (MySQL error 1052).
func TestUpdateFromJoinSQLMySQLQualifiesAssignmentTargets(t *testing.T) {
	t.Parallel()
	got := MySQLDialect().UpdateFromJoinSQL(
		"pool", "latest", "latest.pool_id = pool.id",
		[]JoinAssignment{
			{Column: "pledge", Expr: "latest.pledge"},
			{Column: "cost", Expr: "latest.cost"},
		},
	)
	require.Equal(
		t,
		"UPDATE pool JOIN latest ON latest.pool_id = pool.id "+
			"SET pool.pledge = latest.pledge, pool.cost = latest.cost",
		got,
	)
}

// TestRestorePoolStateAtSlotQueryClassifiesAsOtherNamedInsteadOfUnknown
// guards the instrumentation fix for RestorePoolStateAtSlot's CTE-based
// UPDATE: classifySQLStatement deliberately calls any WITH-leading statement
// "other" rather than guessing a verb (see that function's doc comment), but
// before the query carried a "-- name:" comment it fell into the generic
// "unknown" bucket in dingo_database_sql_query_duration_seconds, aggregating
// it with every other unnamed hand-written query in the store. This must hold
// for all three dialects' assembled statement text, not only SQLite's.
func TestRestorePoolStateAtSlotQueryClassifiesAsOtherNamedInsteadOfUnknown(
	t *testing.T,
) {
	t.Parallel()
	for _, dialect := range []Dialect{
		SQLiteDialect(), PostgresDialect(), MySQLDialect(),
	} {
		op, name := classifySQLStatement(
			restorePoolDenormalizedFieldsQuery(dialect),
		)
		require.Equal(t, "other", op, dialect.Name())
		require.Equal(t, "RestorePoolStateAtSlot", name, dialect.Name())
	}
}

// TestApplyMIRCertificatePersistsProjectedDeltas proves the persistence path
// writes exactly what the certificate's reward projection returns, for every
// credential, rather than a value it re-derives from the underlying field.
// RewardsAmount is *big.Int on every gouroboros release, so this is the path a
// signed delta travels once the underlying field is widened to delta_coin.
func TestApplyMIRCertificatePersistsProjectedDeltas(t *testing.T) {
	t.Parallel()
	store := newMigratedTestStore(t)

	first := mirTestCredential(0x21)
	second := mirTestCredential(0x22)
	cert := decodeMIRDistributionCertificate(
		t,
		uint(lcommon.MirSourceReserves),
		map[*lcommon.Credential]uint64{
			first:  1_200,
			second: 450,
		},
	)
	_, err := applyMIRCertificate(
		context.Background(),
		newDialectQueryer(store.writeDB, store.dialect.Name()),
		cert,
		0,
		400,
	)
	require.NoError(t, err)

	want := map[string]string{}
	for credential, amount := range cert.Reward.RewardsAmount() {
		want[string(credential.Credential[:])] = amount.String()
	}
	require.Len(t, want, 2)

	effects, err := store.GetMIRCertsInSlotRange(0, 1_000, nil)
	require.NoError(t, err)
	require.Len(t, effects, 1)
	got := map[string]string{}
	for _, reward := range effects[0].Rewards {
		require.NotNil(t, reward.Amount)
		got[string(reward.Credential)] = reward.Amount.String()
	}
	assert.Equal(t, want, got)
}

// decodeMIRDistributionCertificate builds a distribution MIR certificate
// through the CBOR decoder, so the test does not depend on the Go type of the
// reward map.
func decodeMIRDistributionCertificate(
	t *testing.T,
	source uint,
	rewards map[*lcommon.Credential]uint64,
) *lcommon.MoveInstantaneousRewardsCertificate {
	t.Helper()
	encoded, err := cbor.Encode(struct {
		cbor.StructAsArray
		Source  uint
		Rewards map[*lcommon.Credential]uint64
	}{
		Source:  source,
		Rewards: rewards,
	})
	require.NoError(t, err)
	cert := &lcommon.MoveInstantaneousRewardsCertificate{
		CertType: uint(lcommon.CertificateTypeMoveInstantaneousRewards),
	}
	require.NoError(t, cert.Reward.UnmarshalCBOR(encoded))
	return cert
}
