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

package migrations

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestBackfillCheckpointIdentityMigrationTranslation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		dialect     string
		integerType string
		blobType    string
	}{
		{dialect: "sqlite", integerType: "INTEGER", blobType: "BLOB"},
		{dialect: "postgres", integerType: "BIGINT", blobType: "BYTEA"},
		{dialect: "mysql", integerType: "BIGINT", blobType: "BLOB"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.dialect, func(t *testing.T) {
			t.Parallel()
			registry, err := cachedRegistryForDialect(testCase.dialect)
			require.NoError(t, err)
			require.Len(t, registry, 57)
			migration := registry[51]
			require.Equal(t, backfillCheckpointIdentitySchemaRelease, migration.Name)
			require.Contains(t, migration.SQL[testCase.dialect].Expand,
				"ALTER TABLE backfill_checkpoint ADD COLUMN last_index_id "+
					testCase.integerType)
			require.Contains(t, migration.SQL[testCase.dialect].Expand,
				"ALTER TABLE backfill_checkpoint ADD COLUMN last_block_hash "+
					testCase.blobType)
			require.Contains(t, migration.SQL[testCase.dialect].Expand[2],
				"completed IS NOT TRUE")
		})
	}
}

func TestDrepDormancySeedUsesPortableIdempotentInsert(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		registry func() ([]Migration, error)
		dialect  string
	}{
		{name: "sqlite", registry: SQLiteRegistry, dialect: "sqlite"},
		{name: "postgres", registry: cachedPostgresRegistry, dialect: "postgres"},
		{name: "mysql", registry: cachedMySQLRegistry, dialect: "mysql"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry, err := tc.registry()
			require.NoError(t, err)
			require.NoError(t, validateRegistry(registry, tc.dialect))
			require.Len(t, registry, 57)
			migration := registry[39]
			require.Equal(t, drepDormancyStateSchemaRelease, migration.Name)
			seed := strings.Join(migration.SQL[tc.dialect].Expand, "\n")
			require.Contains(t, seed, "WHERE NOT EXISTS")
			require.NotContains(t, strings.ToUpper(seed), "ON CONFLICT")
		})
	}
}

func TestAccountDRepClearBackfillRequiresCompletePV10History(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec(`
CREATE TABLE pparams (id integer, cbor blob, added_slot integer, era_id integer);
CREATE TABLE sync_state (sync_key text PRIMARY KEY, value text);
CREATE TABLE account (
    credential_tag integer, staking_key blob, created_slot integer,
    active numeric
);
CREATE TABLE account_import_baseline (
    credential_tag integer, staking_key blob, added_slot integer
);
CREATE TABLE stake_registration (credential_tag integer, staking_key blob, added_slot integer);
CREATE TABLE stake_registration_delegation (credential_tag integer, staking_key blob, added_slot integer);
CREATE TABLE stake_vote_registration_delegation (credential_tag integer, staking_key blob, added_slot integer);
CREATE TABLE vote_registration_delegation (credential_tag integer, staking_key blob, added_slot integer);
CREATE TABLE registration (credential_tag integer, staking_key blob, added_slot integer);
CREATE TABLE stake_deregistration (credential_tag integer, staking_key blob, added_slot integer);
CREATE TABLE deregistration (credential_tag integer, staking_key blob, added_slot integer);`)
	require.NoError(t, err)
	fields := make([]any, 13)
	fields[12] = common.ProtocolParametersProtocolVersion{Major: 11}
	data, err := cbor.Encode(fields)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO pparams VALUES (1, ?, 50, ?)`,
		data, conway.EraIdConway)
	require.NoError(t, err)
	tx, err := db.Begin()
	require.NoError(t, err)
	result, err := accountDRepClearBackfill(t.Context(), Batch{
		Tx: tx, Rebind: func(query string) string { return query },
	})
	require.NoError(t, err)
	require.True(t, result.Done)
	require.Equal(t, "checked", result.Cursor)
	require.NoError(t, tx.Rollback())

	fields[12] = common.ProtocolParametersProtocolVersion{Major: 10}
	data, err = cbor.Encode(fields)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO pparams VALUES (1, ?, 100, ?)`,
		data, conway.EraIdConway)
	require.NoError(t, err)

	tx, err = db.Begin()
	require.NoError(t, err)
	_, err = accountDRepClearBackfill(t.Context(), Batch{
		Tx: tx, Rebind: func(query string) string { return query },
	})
	require.ErrorContains(t, err, "resync is required")
	require.NoError(t, tx.Rollback())

	_, err = db.Exec(`INSERT INTO sync_state VALUES ('mithril_ledger_slot', '200')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO account VALUES (0, X'AA', 50, TRUE)`)
	require.NoError(t, err)
	tx, err = db.Begin()
	require.NoError(t, err)
	_, err = accountDRepClearBackfill(t.Context(), Batch{
		Tx: tx, Rebind: func(query string) string { return query },
	})
	require.ErrorContains(t, err, "baseline is incomplete")
	require.NoError(t, tx.Rollback())

	_, err = db.Exec(`INSERT INTO account_import_baseline VALUES (0, X'AA', 200)`)
	require.NoError(t, err)
	tx, err = db.Begin()
	require.NoError(t, err)
	result, err = accountDRepClearBackfill(t.Context(), Batch{
		Tx: tx, Rebind: func(query string) string { return query },
	})
	require.NoError(t, err)
	require.True(t, result.Done)
	require.Equal(t, "checked", result.Cursor)
	require.NoError(t, tx.Rollback())
}

func TestAccountDRepClearSchemaTranslatesAllDialects(t *testing.T) {
	t.Parallel()
	for _, dialect := range []string{"sqlite", "postgres", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			t.Parallel()
			registry, err := cachedRegistryForDialect(dialect)
			require.NoError(t, err)
			migration := registry[34]
			require.Equal(t, accountDRepClearSchemaRelease, migration.Name)
			sql := strings.Join(migration.SQL[dialect].Expand, "\n")
			require.Contains(t, sql, "account_drep_clear")
			require.Contains(t, sql, "idx_account_drep_clear_added_slot")
		})
	}
}

// TestPointerStakeMigrationTranslatesForProviders pins the postgres and mysql
// renderings of the utxo_pointer table. The pointer position is joined against
// certs and "transaction" when stake is computed, so an integer column that
// translated to something else -- or a foreign key that did not survive the
// rewrite, leaving rows behind after their utxo is rolled back -- would change
// stake on those backends only.
func TestPointerStakeMigrationTranslatesForProviders(t *testing.T) {
	t.Parallel()

	postgres, err := cachedPostgresRegistry()
	require.NoError(t, err)
	postgresSQL := strings.Join(postgres[12].SQL["postgres"].Expand, "\n")
	require.Contains(t, postgresSQL, `"utxo_id" BIGINT NOT NULL`)
	require.Contains(t, postgresSQL, `"ptr_cert_index" BIGINT NOT NULL`)
	require.Contains(
		t,
		postgresSQL,
		`FOREIGN KEY ("utxo_id") REFERENCES "utxo"("id") ON DELETE CASCADE`,
	)
	require.NotContains(t, postgresSQL, "`")

	mysql, err := cachedMySQLRegistry()
	require.NoError(t, err)
	mysqlSQL := strings.Join(mysql[12].SQL["mysql"].Expand, "\n")
	require.Contains(t, mysqlSQL, "`utxo_id` BIGINT NOT NULL")
	require.Contains(
		t,
		mysqlSQL,
		"FOREIGN KEY (`utxo_id`) REFERENCES `utxo`(`id`) ON DELETE CASCADE",
	)
	require.NotContains(t, mysqlSQL, "CREATE INDEX IF NOT EXISTS")
}

func TestCommitteeCredentialMigrationTranslatesForProviders(t *testing.T) {
	t.Parallel()

	postgres, err := cachedPostgresRegistry()
	require.NoError(t, err)
	postgresSQL := strings.Join(postgres[7].SQL["postgres"].Expand, "\n")
	require.Contains(t, postgresSQL, `"cold_credential_tag" BIGINT`)
	require.Contains(t, postgresSQL, "DROP INDEX IF EXISTS")

	mysql, err := cachedMySQLRegistry()
	require.NoError(t, err)
	mysqlSQL := strings.Join(mysql[7].SQL["mysql"].Expand, "\n")
	require.Contains(
		t,
		mysqlSQL,
		"DROP INDEX `idx_committee_member_cold_cred_hash` ON `committee_member`",
	)
	require.NotContains(t, mysqlSQL, "DROP INDEX IF EXISTS")

	postgresPresence := strings.Join(postgres[8].SQL["postgres"].Expand, "\n")
	require.Contains(t, postgresPresence, `"term_start_slot_set" boolean`)
	require.Len(t, postgres[8].SQL["postgres"].Expand, 1)
	require.NotNil(t, postgres[8].Backfill)
	mysqlPresence := strings.Join(mysql[8].SQL["mysql"].Expand, "\n")
	require.Contains(t, mysqlPresence, "`term_start_slot_set` boolean")
	require.Len(t, mysql[8].SQL["mysql"].Expand, 1)
	require.NotNil(t, mysql[8].Backfill)
}

// TestMySQLRegistryPrefixesAccountBaselinePrimaryKey guards the v4 migration's
// dialect hazard: `staking_key` is a blob in the composite primary key, and
// MySQL rejects a key over a VARBINARY column without a prefix length. The
// backfill statement in the same migration must not be rewritten at all.
func TestMySQLRegistryPrefixesAccountBaselinePrimaryKey(t *testing.T) {
	t.Parallel()
	registry, err := cachedMySQLRegistry()
	require.NoError(t, err)
	expand := registry[3].SQL["mysql"].Expand
	require.Len(t, expand, 2)
	require.Contains(t, expand[0], "`staking_key` blob NOT NULL")
	require.Contains(t, expand[0], "`staking_key`(255)")
	require.NotContains(t, expand[1], "ON DUPLICATE KEY")
	require.Contains(t, expand[1], "LEFT JOIN `account_import_baseline`")
	// The certificate-history filters must survive as plain subqueries: they
	// read tables other than the INSERT's target, which is what keeps MySQL
	// from raising error 1093, and no key prefix belongs in a predicate.
	require.Equal(t, 10, strings.Count(expand[1], "NOT EXISTS (SELECT 1 FROM"))
	require.NotContains(t, expand[1], "cert.`staking_key`(255)")
}

// TestPostgresRegistryTypesAccountBaseline checks the v4 table picks up the
// PostgreSQL type translation, and that the backfill's identifiers are
// requoted rather than left as MySQL backticks.
func TestPostgresRegistryTypesAccountBaseline(t *testing.T) {
	t.Parallel()
	registry, err := cachedPostgresRegistry()
	require.NoError(t, err)
	expand := registry[3].SQL["postgres"].Expand
	require.Len(t, expand, 2)
	require.Contains(t, expand[0], `"staking_key" BYTEA NOT NULL`)
	require.Contains(t, expand[0], `"active" BOOLEAN NOT NULL DEFAULT true`)
	require.NotContains(t, expand[0], "`")
	require.NotContains(t, expand[1], "`")
	require.Contains(t, expand[1], `LEFT JOIN "account_import_baseline"`)
	require.Equal(t, 10, strings.Count(expand[1], "NOT EXISTS (SELECT 1 FROM"))
	require.Contains(t, expand[1], `FROM "stake_vote_registration_delegation"`)
}

// TestMySQLRegistryPrefixesTokenRegistrySubjectIndex guards the token registry
// migration's one dialect hazard: `subject` is a TEXT column, and MySQL
// rejects a unique index on TEXT without a prefix length. The translation
// derives that prefix from the CREATE TABLE in the same migration, so this
// breaks if v3 ever stops carrying its own table definition.
func TestMySQLRegistryPrefixesTokenRegistrySubjectIndex(t *testing.T) {
	t.Parallel()
	registry, err := cachedMySQLRegistry()
	require.NoError(t, err)
	require.Contains(
		t,
		registry[2].SQL["mysql"].Expand,
		"CREATE UNIQUE INDEX `idx_token_registry_entry_subject`"+
			" ON `token_registry_entry`(`subject`(255))",
	)
}

func TestMySQLRegistryPrefixesPoolOpCertSequenceIndex(t *testing.T) {
	t.Parallel()
	registry, err := cachedMySQLRegistry()
	require.NoError(t, err)
	require.NoError(t, validateRegistry(registry, "mysql"))
	require.Len(t, registry, 57)

	require.Contains(
		t,
		registry[0].SQL["mysql"].Expand,
		"CREATE INDEX `idx_pool_opcert_sequence_pool_sequence` ON `pool_opcert_sequence`(`pool_key_hash`(255),`sequence`)",
	)
}

func TestUtxoAddressIdentityMigrationTranslatesForProviders(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		registry      func() ([]Migration, error)
		dialect       string
		idDefinition  string
		addressType   string
		foreignKey    string
		indexFragment string
	}{
		{
			name: "sqlite", registry: SQLiteRegistry, dialect: "sqlite",
			idDefinition:  "`utxo_id` INTEGER PRIMARY KEY",
			addressType:   "`address` BLOB NOT NULL",
			foreignKey:    "FOREIGN KEY (`utxo_id`)",
			indexFragment: "ON `utxo_address` (`address`)",
		},
		{
			name: "postgres", registry: cachedPostgresRegistry, dialect: "postgres",
			idDefinition:  "\"utxo_id\" BIGINT PRIMARY KEY",
			addressType:   "\"address\" BYTEA NOT NULL",
			foreignKey:    "FOREIGN KEY (\"utxo_id\")",
			indexFragment: "ON \"utxo_address\" (\"address\")",
		},
		{
			name: "mysql", registry: cachedMySQLRegistry, dialect: "mysql",
			idDefinition:  "`utxo_id` BIGINT PRIMARY KEY",
			addressType:   "`address` BLOB NOT NULL",
			foreignKey:    "FOREIGN KEY (`utxo_id`)",
			indexFragment: "ON `utxo_address` (`address`(255))",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			registry, err := tc.registry()
			require.NoError(t, err)
			expand := registry[41].SQL[tc.dialect].Expand
			require.Len(t, expand, 2)
			require.Contains(t, expand[0], tc.idDefinition)
			require.Contains(t, expand[0], tc.addressType)
			require.Contains(t, expand[0], tc.foreignKey)
			require.Contains(t, expand[0], "ON DELETE CASCADE")
			require.Contains(t, expand[1], tc.indexFragment)
		})
	}
}

func TestRewardCreditRoundMigrationTranslatesForProviders(t *testing.T) {
	t.Parallel()

	postgres, err := cachedPostgresRegistry()
	require.NoError(t, err)
	postgresSQL := strings.Join(postgres[30].SQL["postgres"].Expand, "\n")
	require.Contains(t, postgresSQL, `CREATE INDEX IF NOT EXISTS "idx_reward_account_output_pending_round"`)
	require.Contains(t, postgresSQL, `ON "reward_account_output"("spendable", "guarded", "folded", "epoch")`)
	require.NotContains(t, postgresSQL, "`")

	mysql, err := cachedMySQLRegistry()
	require.NoError(t, err)
	mysqlSQL := strings.Join(mysql[30].SQL["mysql"].Expand, "\n")
	require.Contains(t, mysqlSQL, "CREATE INDEX `idx_reward_account_output_pending_round`")
	require.Contains(t, mysqlSQL, "ON `reward_account_output`(`spendable`, `guarded`, `folded`, `epoch`)")
	require.NotContains(t, mysqlSQL, "CREATE INDEX IF NOT EXISTS")
}

func TestLeiosKeyLookupIndexesTranslateForProviders(t *testing.T) {
	t.Parallel()

	postgres, err := cachedPostgresRegistry()
	require.NoError(t, err)
	postgresSQL := strings.Join(postgres[50].SQL["postgres"].Expand, "\n")
	require.Contains(t, postgresSQL,
		`ON "pool_registration"("pool_id", "certificate_id", "registration_index", "added_slot", "id")`)
	require.Contains(t, postgresSQL,
		`ON "pool_retirement"("pool_id", "added_slot")`)

	mysql, err := cachedMySQLRegistry()
	require.NoError(t, err)
	mysqlSQL := strings.Join(mysql[50].SQL["mysql"].Expand, "\n")
	require.Contains(t, mysqlSQL,
		"ON `pool_registration`(`pool_id`, `certificate_id`, `registration_index`, `added_slot`, `id`)")
	require.Contains(t, mysqlSQL,
		"ON `pool_retirement`(`pool_id`, `added_slot`)")
	require.NotContains(t, mysqlSQL, "CREATE INDEX IF NOT EXISTS")
}

// TestRewardOutputCredentialPendingIndexSkipsMySQL pins the covering index to
// the providers that can cover with it. MySQL indexes the blob staking_key and
// text amount by prefix, and a prefix index never covers a column, so the
// index would cost every reward output write and save no table fetch.
func TestRewardOutputCredentialPendingIndexSkipsMySQL(t *testing.T) {
	t.Parallel()

	postgres, err := PostgresRegistry()
	require.NoError(t, err)
	require.Equal(t, rewardOutputCredentialPendingIndexSchemaRelease,
		postgres[53].Name)
	require.Contains(t,
		strings.Join(postgres[53].SQL["postgres"].Expand, "\n"),
		`ON "reward_account_output"`+"\n"+
			`       ("credential_tag", "staking_key", "spendable", "guarded", "folded",`)

	mysql, err := MySQLRegistry()
	require.NoError(t, err)
	require.NoError(t, validateRegistry(mysql, "mysql"))
	require.Equal(t, rewardOutputCredentialPendingIndexSchemaRelease,
		mysql[53].Name)
	require.Empty(t, mysql[53].SQL["mysql"].Expand)
	require.Empty(t, mysql[53].SQL["mysql"].Contract)
}

func TestRatificationHistoryMigrationTranslatesForProviders(t *testing.T) {
	t.Parallel()

	postgres, err := cachedPostgresRegistry()
	require.NoError(t, err)
	postgresSQL := strings.Join(postgres[5].SQL["postgres"].Expand, "\n")
	require.Contains(t, postgresSQL, `"id" BIGSERIAL PRIMARY KEY`)
	require.Contains(t, postgresSQL, `"proposal_id" BIGINT NOT NULL`)
	require.NotContains(t, postgresSQL, "`")

	mysql, err := cachedMySQLRegistry()
	require.NoError(t, err)
	mysqlSQL := strings.Join(mysql[5].SQL["mysql"].Expand, "\n")
	require.Contains(t, mysqlSQL, "`id` BIGINT AUTO_INCREMENT PRIMARY KEY")
	require.Contains(
		t,
		mysqlSQL,
		"FOREIGN KEY (`proposal_id`) REFERENCES `governance_proposal`(`id`)",
	)
	require.NotContains(t, mysqlSQL, "CREATE INDEX IF NOT EXISTS")
}

func TestMySQLSchemaTranslationPrefixesBlobIndexes(t *testing.T) {
	expand, err := loadSQL("v1/sqlite/expand.sql")
	require.NoError(t, err)
	translated := translateSchemaSQLInSchema(expand, "mysql", expand)
	for _, statement := range translated {
		if strings.Contains(
			statement,
			"idx_account_inactivity_activation_staking_key",
		) {
			require.Contains(t, statement, "`staking_key`(255)")
		}
		if strings.Contains(statement, "idx_transaction_block_hash") {
			require.Contains(t, statement, "`block_hash`(255)")
		}
		if strings.Contains(statement, "idx_utxo_collateral_by_tx_id") {
			require.NotContains(t, statement, "`collateral_by_tx_id`(255)")
		}
	}
}

func TestPostgresSchemaTranslationUsesCompatibleIntegerTypes(t *testing.T) {
	t.Parallel()
	expand, err := loadSQL("v1/sqlite/expand.sql")
	require.NoError(t, err)
	translated := translateSchemaSQLInSchema(expand, "postgres", expand)
	joined := strings.Join(translated, "\n")
	require.Contains(t, joined, "BIGSERIAL PRIMARY KEY")
	// SQLite INTEGER foreign-key columns must be widened alongside the
	// BIGSERIAL IDs they reference; PostgreSQL rejects an integer→bigint FK.
	require.NotRegexp(t, `(?i)\binteger\b`, joined)
	require.Contains(t, joined, "BIGINT")
}

func TestSplitSQL(t *testing.T) {
	t.Parallel()
	statements, err := splitSQL(`
		-- comment with ;
		CREATE/* preserve token boundary */TABLE thing (value TEXT DEFAULT ';');
		/* another ; */
		INSERT INTO thing VALUES ('it''s;fine');
	`)
	require.NoError(t, err)
	require.Equal(t, []string{
		"CREATE TABLE thing (value TEXT DEFAULT ';')",
		"INSERT INTO thing VALUES ('it''s;fine')",
	}, statements)
}

func TestSplitSQLPreservesCommentTokenBoundaries(t *testing.T) {
	t.Parallel()
	statements, err := splitSQL(
		"CREATE/*inline*/TABLE thing (id INTEGER); SELECT/*x*/1;",
	)
	require.NoError(t, err)
	require.Equal(t, []string{
		"CREATE TABLE thing (id INTEGER)",
		"SELECT 1",
	}, statements)
}

func TestPoolRegistrationIdentityMigrationTranslatesIndexDrop(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		dialect string
		dropSQL string
	}{
		{"sqlite", "DROP INDEX IF EXISTS `idx_pool_reg_pool_slot`"},
		{"postgres", `DROP INDEX IF EXISTS "idx_pool_reg_pool_slot"`},
		{"mysql", "DROP INDEX `idx_pool_reg_pool_slot` ON `pool_registration`"},
	} {
		t.Run(tc.dialect, func(t *testing.T) {
			t.Parallel()
			registry, err := cachedRegistryForDialect(tc.dialect)
			require.NoError(t, err)
			migration := registry[42]
			require.Equal(t, poolRegistrationIdentitySchemaRelease, migration.Name)
			expand := migration.SQL[tc.dialect].Expand
			require.Contains(t, expand, tc.dropSQL)
			if tc.dialect == "mysql" {
				require.Contains(t, expand[2],
					"CREATE UNIQUE INDEX `idx_pool_reg_pool_slot_cert_index`")
				require.Equal(t, tc.dropSQL, expand[3])
			}
		})
	}
}
