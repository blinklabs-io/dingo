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
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

const spentStakingIndex = "idx_utxo_deleted_staking_amount"

// planSpendSequence selects a distinct live seeded output for each spend, so
// a scenario can run once per planner-statistics stage.
var planSpendSequence atomic.Uint64

// utxoPlanScenario drives one store method and names what the statements it
// issues against utxo must do.
type utxoPlanScenario struct {
	name string
	run  func(t *testing.T, store *Store)
	// wholeSet marks a read that returns every live row: no index can answer
	// it better than a pass over the table.
	wholeSet bool
	// cached names statements the store prepares once at Start, which the
	// recorder therefore never sees run.
	cached []string
	// built returns statements the store assembles for callers outside this
	// package's reach, to be planned alongside the recorded ones.
	built func(t *testing.T, store *Store) []string
	// indexes must each appear in some statement's plan.
	indexes []string
}

func utxoPlanScenarios() []utxoPlanScenario {
	const maxRows = 1000
	payment := churnHash('p', 1, 0, 28)
	staking := churnHash('s', 1, 0, 28)
	yes, no := true, false
	slot := uint64(1_000_000)
	spentFrom := uint64(1_500)
	return []utxoPlanScenario{
		{
			name: "rollback sweep",
			run: func(t *testing.T, store *Store) {
				require.NoError(t, store.DeleteUtxosAfterSlot(slot, nil))
			},
			indexes: []string{"idx_utxo_added_slot"},
		},
		{
			name: "rollback un-spend",
			run: func(t *testing.T, store *Store) {
				require.NoError(t, store.SetUtxosNotDeletedAfterSlot(slot, nil))
			},
			indexes: []string{
				// The stake-reference sweep reads only the index; the
				// un-spend UPDATE seeks it and fetches the row.
				"USING COVERING INDEX idx_utxo_deleted_staking_amount (deleted_slot>?)",
				"USING INDEX idx_utxo_deleted_staking_amount (deleted_slot>?)",
			},
		},
		{
			name: "restored by rollback",
			run: func(t *testing.T, store *Store) {
				_, err := store.GetUtxosRestoredByRollback(slot, nil)
				require.NoError(t, err)
			},
			indexes: []string{"idx_utxo_deleted_staking_amount"},
		},
		{
			name: "spent prune window",
			run: func(t *testing.T, store *Store) {
				_, err := store.GetUtxosDeletedBeforeSlot(spentFrom, 100, nil)
				require.NoError(t, err)
			},
			indexes: []string{"idx_utxo_deleted_staking_amount"},
		},
		{
			name: "spent rows missing an address",
			run: func(t *testing.T, store *Store) {
				_, err := store.GetSpentUtxosMissingAddressAfterSlot(
					slot, 0, 100, nil,
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "live stake of a credential",
			run:     func(*testing.T, *Store) {},
			cached:  []string{sumCredentialUtxoStakeQuery},
			indexes: []string{"idx_utxo_staking_deleted_amount"},
		},
		{
			name: "account totals",
			run: func(t *testing.T, store *Store) {
				_, err := store.GetAccountAddressTotals(
					0, staking, models.AccountAggregateLimits{
						MaxUtxos: maxRows, MaxAssetRows: maxRows,
						MaxAssets: maxRows,
					}, nil,
				)
				require.NoError(t, err)
			},
		},
		{
			name: "address by payment credential",
			run: func(t *testing.T, store *Store) {
				for _, script := range []*bool{&yes, &no} {
					_, err := store.GetUtxosByAddress(
						[]models.UtxoAddressPattern{{
							PaymentPart: payment, PaymentScript: script,
						}}, maxRows, nil,
					)
					require.NoError(t, err)
				}
			},
		},
		{
			name: "address by delegation credential",
			run: func(t *testing.T, store *Store) {
				_, err := store.GetUtxosByAddress(
					[]models.UtxoAddressPattern{{DelegationPart: staking}},
					maxRows, nil,
				)
				require.NoError(t, err)
			},
		},
		{
			name: "address by payment and delegation credential",
			run: func(t *testing.T, store *Store) {
				_, err := store.GetUtxosByAddress(
					[]models.UtxoAddressPattern{{
						PaymentPart: payment, DelegationPart: staking,
						PaymentScript: &no,
					}}, maxRows, nil,
				)
				require.NoError(t, err)
			},
		},
		{
			name: "address as of a slot",
			run: func(t *testing.T, store *Store) {
				_, err := store.GetUtxosByAddressAsOf(
					[]models.UtxoAddressPattern{{
						PaymentPart: payment, PaymentScript: &no,
					}}, slot, maxRows, nil,
				)
				require.NoError(t, err)
			},
		},
		{
			name: "history of unspent outputs by credential",
			run: func(t *testing.T, store *Store) {
				_, err := store.GetUtxosWithHistory(&models.UtxoHistoryQuery{
					AddressPatterns: []models.UtxoAddressPattern{{
						DelegationPart: staking,
					}},
					Status: models.UtxoHistoryStatusUnspent,
					Limit:  100,
				}, nil)
				require.NoError(t, err)
			},
		},
		{
			name: "history of spent outputs by credential",
			run: func(t *testing.T, store *Store) {
				_, err := store.GetUtxosWithHistory(&models.UtxoHistoryQuery{
					AddressPatterns: []models.UtxoAddressPattern{{
						DelegationPart: staking,
					}},
					Status: models.UtxoHistoryStatusSpent,
					Limit:  100,
				}, nil)
				require.NoError(t, err)
			},
		},
		{
			name: "history of outputs spent in a slot range",
			run: func(t *testing.T, store *Store) {
				before := spentFrom + 100
				_, err := store.GetUtxosWithHistory(&models.UtxoHistoryQuery{
					MatchAllAddresses: true,
					SpentAfter:        &spentFrom,
					SpentBefore:       &before,
					Limit:             100,
				}, nil)
				require.NoError(t, err)
			},
			indexes: []string{"idx_utxo_deleted_staking_amount"},
		},
		{
			name: "spend one output",
			run: func(t *testing.T, store *Store) {
				input, err := mockledger.NewTransactionInputBuilder().
					WithTxId(churnHash(
						't', 2*planSpendSequence.Add(1)+1, 0, 32,
					)).WithIndex(0).Build()
				require.NoError(t, err)
				spender := churnHash('x', 1, 0, 32)
				testutil.TransactionID(t, store.writeDB, spender)
				require.NoError(t, store.SetUtxoDeletedAtSlot(
					input, slot, spender, nil,
				))
			},
		},
		{
			name: "pointer stake as of a slot",
			run:  func(*testing.T, *Store) {},
			built: func(t *testing.T, store *Store) []string {
				query, _, err := pointerStakeInputsSQL(slot, 0)
				require.NoError(t, err)
				return []string{query}
			},
		},
		{
			name: "delegated stake as of a slot",
			run:  func(*testing.T, *Store) {},
			built: func(t *testing.T, store *Store) []string {
				query, _, err := store.historicalStakeCTE(
					t.Context(), store.writeDB, slot, 0, nil, 0, 0,
					"1 = 1", nil,
				)
				require.NoError(t, err)
				return []string{query + "\nSELECT utxo_amount " +
					"FROM active_delegator_stake"}
			},
		},
		{
			name: "live lovelace at the era boundary",
			// The store sums with dingo_bigint_sum, which the recording
			// driver does not register; the plain SUM has the same access
			// path.
			run: func(*testing.T, *Store) {},
			cached: []string{
				"SELECT SUM(CAST(amount AS BIGINT)) FROM utxo " +
					"WHERE deleted_slot = 0",
			},
			wholeSet: true,
		},
		{
			name: "iterate every live output",
			run: func(t *testing.T, store *Store) {
				require.NoError(t, store.IterateLiveUtxos(
					nil, func(*models.Utxo) error { return nil },
				))
			},
			wholeSet: true,
		},
	}
}

var utxoTableScan = regexp.MustCompile(`(?m)^SCAN utxo( AS \w+)?$`)

// TestSpentStakingIndexReadersKeepAnIndex runs each reader of utxo that
// idx_utxo_deleted_staking_amount could serve and checks the plan of every
// statement it issues: none may pass over the utxo table, and the readers that
// range over spent rows must choose the partial index. The scenarios run with
// no planner statistics and again after ANALYZE.
//
// idx_utxo_deleted_payment_script also leads with deleted_slot and holds every
// row, so while it exists the planner may choose it over the partial index for
// a spent-row range. The "supply index dropped" variant removes it so that the
// partial index is the only deleted_slot candidate and the choice rests on the
// statement text alone; the other variant keeps the production schema.
func TestSpentStakingIndexReadersKeepAnIndex(t *testing.T) {
	t.Parallel()
	skipPlanTestUnderRace(t)
	for _, dropSupplyIndex := range []bool{false, true} {
		name := "supply index present"
		if dropSupplyIndex {
			name = "supply index dropped"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			skipPlanTestUnderRace(t)
			checkSpentStakingIndexReaders(t, dropSupplyIndex)
		})
	}
}

func checkSpentStakingIndexReaders(t *testing.T, dropSupplyIndex bool) {
	t.Helper()
	store, recorder := newRecordingSQLiteStore(t)
	seedChurnUtxos(t, store, "shelley", 20_000)
	// A handful of pointer outputs, as on mainnet. With none, ANALYZE leaves
	// utxo_pointer without statistics and the planner drives the pointer-stake
	// statement from utxo instead of from the pointer table.
	_, err := store.writeDB.Exec(
		"INSERT INTO utxo_pointer (utxo_id, ptr_slot, ptr_tx_index, " +
			"ptr_cert_index) SELECT id, id, 0, 0 FROM utxo " +
			"WHERE deleted_slot = 0 LIMIT 50",
	)
	require.NoError(t, err)
	var definition string
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT sql FROM sqlite_master WHERE name = ?", spentStakingIndex,
	).Scan(&definition))
	require.Contains(t, definition, "WHERE deleted_slot > 0",
		"the readers below are only meaningful against the partial index")
	if dropSupplyIndex {
		_, err = store.writeDB.Exec("DROP INDEX idx_utxo_deleted_payment_script")
		require.NoError(t, err)
		// Idle read connections keep the schema they loaded at Start.
		require.NoError(t, store.refreshReadPool(t.Context()))
	}

	for _, stage := range []string{"no statistics", "analyzed"} {
		if stage == "analyzed" {
			_, err := store.writeDB.Exec("ANALYZE")
			require.NoError(t, err)
		}
		for _, scenario := range utxoPlanScenarios() {
			mark := len(recorder.snapshot())
			scenario.run(t, store)
			var utxoStatements []string
			for _, statement := range recorder.snapshot()[mark:] {
				if !regexp.MustCompile(`\butxo\b`).MatchString(statement) {
					continue
				}
				if strings.HasPrefix(strings.TrimSpace(statement), "PRAGMA") ||
					strings.HasPrefix(strings.TrimSpace(statement), "BEGIN") {
					continue
				}
				utxoStatements = append(utxoStatements, statement)
			}
			utxoStatements = append(utxoStatements, scenario.cached...)
			if scenario.built != nil {
				utxoStatements = append(utxoStatements, scenario.built(t, store)...)
			}
			require.NotEmpty(t, utxoStatements,
				"%s/%s issued no statement against utxo", stage, scenario.name)
			var plans []string
			for _, statement := range utxoStatements {
				// The recorder keeps statement text only. A bound above every
				// slot in the fixture is the realistic rollback and lookup
				// point; a NULL parameter plans as an empty range instead.
				args := make([]any, strings.Count(statement, "?"))
				for i := range args {
					args[i] = int64(5_000_000)
				}
				plan := queryPlan(t, store.writeDB, statement, args...)
				plans = append(plans, plan)
				if !scenario.wholeSet {
					require.NotRegexp(t, utxoTableScan, plan,
						"%s/%s scans utxo.\nstatement: %s\nplan: %s",
						stage, scenario.name, statement, plan)
				}
			}
			joined := strings.Join(plans, "\n")
			if !dropSupplyIndex {
				continue
			}
			if scenario.wholeSet {
				require.Regexp(t, utxoTableScan, joined,
					"%s/%s is the pass over every live row", stage, scenario.name)
			}
			for _, index := range scenario.indexes {
				require.Contains(t, joined, index,
					"%s/%s must use %s.\nplans: %s",
					stage, scenario.name, index, joined)
			}
		}
	}
}

// TestDeferredRebuildKeepsSpentStakingIndexPartial drops the manifest and
// rebuilds it: the bulk-load rebuild must restore the index with the predicate
// the upgrade gives it, or a node that bulk-loads would carry the full index
// again.
func TestDeferredRebuildKeepsSpentStakingIndexPartial(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	check := func() {
		t.Helper()
		var definition string
		require.NoError(t, store.readDB.QueryRow(
			"SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
			spentStakingIndex,
		).Scan(&definition))
		require.Contains(t, definition, "WHERE deleted_slot > 0")
	}
	check()
	require.NoError(t, store.DropDeferredIndexes())
	require.NoError(t, store.BuildDeferredIndexes())
	check()
}

// TestSpentStakingIndexDeferredCreateSQLPerDialect pins the statement the
// bulk-load rebuild issues for each engine: the partial index on SQLite and
// PostgreSQL, and on MySQL, which has no partial index, an index on
// deleted_slot alone rather than one that holds every live row by credential.
func TestSpentStakingIndexDeferredCreateSQLPerDialect(t *testing.T) {
	t.Parallel()
	var entry deferred.Index
	for _, index := range deferred.Manifest {
		if index.Name == spentStakingIndex {
			entry = index
		}
	}
	require.Equal(t, spentStakingIndex, entry.Name)

	for _, dialect := range []Dialect{SQLiteDialect(), PostgresDialect()} {
		statement := deferredIndexCreateSQL(dialect, entry)
		require.Contains(t, statement,
			`("deleted_slot", "credential_tag", "staking_key", "amount")`,
			dialect.Name())
		require.True(t, strings.HasSuffix(statement, " WHERE deleted_slot > 0"),
			"%s: %s", dialect.Name(), statement)
	}
	require.Equal(t,
		"CREATE INDEX `"+spentStakingIndex+"` ON `utxo` (`deleted_slot`)",
		deferredIndexCreateSQL(MySQLDialect(), entry))
}

// useFullSpentStakingIndex replaces the partial index with the full definition
// databases carried before it became partial. A test that shows a statement
// shape can be steered onto a deleted_slot index needs an index that holds
// every live row to reproduce the steering.
func useFullSpentStakingIndex(tb testing.TB, store *Store) {
	tb.Helper()
	for _, statement := range []string{
		"DROP INDEX " + spentStakingIndex,
		"CREATE INDEX " + spentStakingIndex + " ON utxo " +
			"(deleted_slot, credential_tag, staking_key, amount)",
	} {
		_, err := store.writeDB.Exec(statement)
		require.NoError(tb, err, statement)
	}
	// Idle read connections keep the schema they loaded at Start.
	require.NoError(tb, store.refreshReadPool(tb.Context()))
}
