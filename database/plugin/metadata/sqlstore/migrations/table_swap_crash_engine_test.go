//go:build dingo_extra_plugins

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
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// swapStatement matches the statements that move a rebuilt table into place:
// the drops and renames of whole tables and the foreign key and counter
// changes that go with them. MySQL commits each one implicitly.
var swapStatement = regexp.MustCompile(
	"(?is)^\\s*(DROP TABLE|RENAME TABLE|ALTER TABLE\\s+\\S+\\s+RENAME TO|" +
		"ALTER TABLE\\s+\\S+\\s+(DROP FOREIGN KEY|ADD CONSTRAINT|" +
		"AUTO_INCREMENT))",
)

var errInjectedCrash = errors.New("injected crash")

// crashInjector fails the nth swap statement it sees without running it, as a
// process killed between two implicitly committed statements would.
type crashInjector struct {
	mu     sync.Mutex
	target int
	seen   int
	fired  bool
}

func (c *crashInjector) check(query string) error {
	if !swapStatement.MatchString(query) {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen++
	if c.target > 0 && c.seen == c.target && !c.fired {
		c.fired = true
		return errInjectedCrash
	}
	return nil
}

func (c *crashInjector) statements() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seen
}

type crashConnector struct {
	inner driver.Connector
	inj   *crashInjector
}

func (c crashConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.inner.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &crashConn{Conn: conn, inj: c.inj}, nil
}

func (c crashConnector) Driver() driver.Driver { return c.inner.Driver() }

// crashConn forwards to the MySQL connection, which implements every optional
// interface the migration runner's queries reach.
type crashConn struct {
	driver.Conn
	inj *crashInjector
}

func (c *crashConn) ExecContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Result, error) {
	if err := c.inj.check(query); err != nil {
		return nil, err
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *crashConn) QueryContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *crashConn) PrepareContext(
	ctx context.Context,
	query string,
) (driver.Stmt, error) {
	return c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
}

func (c *crashConn) BeginTx(
	ctx context.Context,
	opts driver.TxOptions,
) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c *crashConn) CheckNamedValue(nv *driver.NamedValue) error {
	return c.Conn.(driver.NamedValueChecker).CheckNamedValue(nv)
}

func (c *crashConn) ResetSession(ctx context.Context) error {
	return c.Conn.(driver.SessionResetter).ResetSession(ctx)
}

func (c *crashConn) IsValid() bool {
	return c.Conn.(driver.Validator).IsValid()
}

// openCrashDB opens the engine's schema through a connection that fails the
// nth swap statement. A target of zero only counts them.
func openCrashDB(
	t *testing.T,
	e *assetSplitEngine,
	target int,
) (*sql.DB, *crashInjector) {
	t.Helper()
	cfg, err := mysqldriver.ParseDSN(e.dsn)
	require.NoError(t, err)
	connector, err := mysqldriver.NewConnector(cfg)
	require.NoError(t, err)
	inj := &crashInjector{target: target}
	db := sql.OpenDB(crashConnector{inner: connector, inj: inj})
	t.Cleanup(func() { _ = db.Close() })
	return db, inj
}

// schemaFingerprint lists the tables, columns, indexes and foreign keys of
// the engine's schema, so two databases can be compared by structure.
func schemaFingerprint(t *testing.T, e *assetSplitEngine) []string {
	t.Helper()
	var out []string
	collect := func(label, query string) {
		rows, err := e.db.Query(query)
		require.NoError(t, err)
		defer func() { require.NoError(t, rows.Close()) }()
		cols, err := rows.Columns()
		require.NoError(t, err)
		for rows.Next() {
			values := make([]sql.NullString, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			parts := make([]string, len(cols))
			for i, v := range values {
				parts[i] = v.String
			}
			out = append(out, label+" "+strings.Join(parts, "|"))
		}
		require.NoError(t, rows.Err())
	}
	collect("table", `
SELECT table_name FROM information_schema.tables
WHERE table_schema = DATABASE() ORDER BY table_name`)
	collect("column", `
SELECT table_name, column_name, column_type, is_nullable, column_key, extra
FROM information_schema.columns WHERE table_schema = DATABASE()
ORDER BY table_name, ordinal_position`)
	collect("index", `
SELECT table_name, index_name, seq_in_index, column_name, non_unique
FROM information_schema.statistics WHERE table_schema = DATABASE()
ORDER BY table_name, index_name, seq_in_index`)
	collect("fk", `
SELECT k.table_name, k.constraint_name, k.column_name,
       k.referenced_table_name, k.referenced_column_name, r.delete_rule
FROM information_schema.key_column_usage k
JOIN information_schema.referential_constraints r
  ON r.constraint_schema = k.constraint_schema
 AND r.constraint_name = k.constraint_name AND r.table_name = k.table_name
WHERE k.table_schema = DATABASE() AND k.referenced_table_name IS NOT NULL
ORDER BY k.table_name, k.constraint_name, k.ordinal_position`)
	return out
}

// queryContents dumps the rows a query returns.
func queryContents(t *testing.T, e *assetSplitEngine, query string) []string {
	t.Helper()
	rows, err := e.db.Query(query)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var out []string
	for rows.Next() {
		values := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		out = append(out, fmt.Sprintf("%q", values))
	}
	require.NoError(t, rows.Err())
	return out
}

// swapCrashCase is one migration whose swap runs on MySQL.
type swapCrashCase struct {
	name    string
	version int
	// prepare migrates to the version before and may insert legacy rows.
	prepare func(t *testing.T, e *assetSplitEngine)
	// contents are queries over the rows that must survive every
	// interruption. Dictionary ids are not compared: a rolled-back batch
	// still consumes auto-increment values, so rows name their credentials
	// by hash.
	contents []string
	// keepsUtxoKeys requires the foreign keys that reference utxo to be the
	// ones the template held, so a key missing from the migration's fixed
	// list cannot be lost on every run alike.
	keepsUtxoKeys bool
}

func tableQuery(tables ...string) []string {
	queries := make([]string, len(tables))
	for i, table := range tables {
		queries[i] = "SELECT * FROM `" + table + "` ORDER BY 1, 2"
	}
	return queries
}

func prepareEmpty(version int) func(*testing.T, *assetSplitEngine) {
	return func(t *testing.T, e *assetSplitEngine) {
		t.Helper()
		e.run(t, e.registry[:version-1])
	}
}

var swapCrashCases = []swapCrashCase{
	{
		name: "utxo_edges", version: edgeClusteredVersion,
		prepare:  prepareEmpty(edgeClusteredVersion),
		contents: tableQuery("utxo_reference_edge", "utxo_collateral_edge"),
	},
	{
		name: "asset_mint_burn", version: mintBurnIDsVersion,
		prepare:  prepareEmpty(mintBurnIDsVersion),
		contents: tableQuery("asset_mint_burn"),
	},
	{
		name: "utxo_address", version: utxoAddressDictionaryVersion,
		prepare:  prepareEmpty(utxoAddressDictionaryVersion),
		contents: tableQuery("utxo_address"),
	},
	{
		name: "block_nonce", version: blockNonceClusteredVersion,
		prepare:  prepareEmpty(blockNonceClusteredVersion),
		contents: tableQuery("block_nonce"),
	},
	{
		name: "credentials", version: utxoCredentialDictionaryVersion,
		prepare:       seedCredentialDictionary,
		keepsUtxoKeys: true,
		contents: append(tableQuery("utxo_address", "utxo_reference_edge"),
			`SELECT u.id, u.tx_id, u.output_idx, pc.hash, sc.hash, d.hash,
       u.credential_tag, u.amount, u.payment_script, u.added_slot,
       u.deleted_slot
FROM utxo u
LEFT JOIN credential pc ON pc.id = u.payment_key_id
LEFT JOIN credential sc ON sc.id = u.staking_key_id
LEFT JOIN datum d ON d.id = u.datum_id ORDER BY u.id`,
			`SELECT a.id, pc.hash, sc.hash, a.credential_tag, a.transaction_id,
       a.slot, a.tx_index, a.payment_script
FROM address_transaction a
LEFT JOIN credential pc ON pc.id = a.payment_key_id
LEFT JOIN credential sc ON sc.id = a.staking_key_id ORDER BY a.id`),
	},
	{
		name: "pool_opcert", version: poolOpCertClusteredVersion,
		prepare: func(t *testing.T, e *assetSplitEngine) {
			t.Helper()
			e.run(t, e.registry[:poolOpCertClusteredVersion-1])
			for _, row := range poolOpCertRows {
				_, err := e.db.Exec(e.query(`
INSERT INTO pool_opcert_sequence (pool_key_hash, slot, sequence)
VALUES (?, ?, ?)`), row.Pool, *row.Slot, row.Sequence)
				require.NoError(t, err)
			}
		},
		contents: []string{`SELECT c.hash, o.slot, o.sequence
FROM pool_opcert_sequence o
JOIN credential c ON c.id = o.pool_key_id ORDER BY o.slot`},
	},
}

type swapOutcome struct {
	schema   []string
	contents [][]string
}

func (c swapCrashCase) outcome(t *testing.T, e *assetSplitEngine) swapOutcome {
	t.Helper()
	out := swapOutcome{
		schema: schemaFingerprint(t, e),
	}
	for _, query := range c.contents {
		out.contents = append(out.contents, queryContents(t, e, query))
	}
	return out
}

// cloneSchema copies every table of the template schema, with its rows, into
// the engine's empty schema, so a case migrates and seeds once and each
// interruption point starts from the same state without replaying the
// history.
func cloneSchema(t *testing.T, to, template *assetSplitEngine) {
	t.Helper()
	ctx := context.Background()
	conn, err := to.db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	_, err = conn.ExecContext(ctx, "SET foreign_key_checks = 0")
	require.NoError(t, err)
	rows, err := template.db.Query("SHOW TABLES")
	require.NoError(t, err)
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	for _, name := range names {
		var ignored, ddl string
		require.NoError(t, template.db.QueryRow(
			"SHOW CREATE TABLE `"+name+"`").Scan(&ignored, &ddl))
		_, err := conn.ExecContext(ctx, ddl)
		require.NoError(t, err, name)
		_, err = conn.ExecContext(ctx, "INSERT INTO `"+name+
			"` SELECT * FROM `"+template.database+"`.`"+name+"`")
		require.NoError(t, err, name)
	}
}

// TestMySQLSwapResumesAfterCrash interrupts each rebuild's table swap before
// every one of its statements, as a crash between two implicitly committed
// DDL statements would, then reruns the migration and requires the schema,
// foreign keys and rows of an uninterrupted run.
func TestMySQLSwapResumesAfterCrash(t *testing.T) {
	t.Parallel()
	for _, tc := range swapCrashCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			template := newMySQLAssetSplitEngine(t, "dingo_swap_tpl_"+tc.name)
			tc.prepare(t, template)

			ref := newMySQLAssetSplitEngine(t, "dingo_swap_ref_"+tc.name)
			cloneSchema(t, ref, template)
			refDB, counter := openCrashDB(t, ref, 0)
			require.NoError(t, runWith(refDB, ref, tc.version))
			total := counter.statements()
			require.Positive(t, total, "the swap issued no statements")
			want := tc.outcome(t, ref)
			if tc.keepsUtxoKeys {
				require.NotEmpty(t, utxoKeys(schemaFingerprint(t, template)))
				require.Equal(t,
					utxoKeys(schemaFingerprint(t, template)),
					utxoKeys(want.schema),
				)
			}

			for n := 1; n <= total; n++ {
				t.Run(fmt.Sprintf("crash_before_statement_%d", n),
					func(t *testing.T) {
						e := newMySQLAssetSplitEngine(t,
							fmt.Sprintf("dingo_swap_%s_%d", tc.name, n))
						cloneSchema(t, e, template)
						db, inj := openCrashDB(t, e, n)
						err := runWith(db, e, tc.version)
						require.ErrorIs(t, err, errInjectedCrash)
						require.True(t, inj.fired)

						require.NoError(t, runWith(e.db, e, tc.version))
						require.Equal(t, want.schema, schemaFingerprint(t, e))
						for i, query := range tc.contents {
							require.Equal(t, want.contents[i],
								queryContents(t, e, query), query)
						}
					})
			}
		})
	}
}

// utxoKeys picks the foreign keys that reference utxo out of a fingerprint.
func utxoKeys(fingerprint []string) []string {
	var keys []string
	for _, line := range fingerprint {
		if strings.HasPrefix(line, "fk ") && strings.Contains(line, "|utxo|id|") {
			keys = append(keys, line)
		}
	}
	return keys
}

func runWith(db *sql.DB, e *assetSplitEngine, version int) error {
	runner := migrations.Runner{
		DB:       db,
		Dialect:  e.dialect,
		Registry: e.registry[:version],
		Locker:   migrations.NewProcessLocker(),
	}
	return runner.Run(context.Background())
}
