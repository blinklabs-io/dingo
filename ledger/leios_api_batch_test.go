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

package ledger

import (
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/safedecode"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func newLeiosAPIApplyTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database, *sql.DB, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	db, err := dbtest.NewDatabase(
		t,
		&database.Config{
			DataDir:      t.TempDir(),
			StorageMode:  types.StorageModeAPI,
			PromRegistry: reg,
		},
	)
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	initializeLeiosApplyTestState(ls)
	return ls, db, raw, reg
}

func TestLeiosAPIBatchAppliesDependentTransactionsAndDeduplicates(
	t *testing.T,
) {
	t.Parallel()
	ls, db, rawDB, reg := newLeiosAPIApplyTestLedger(t)
	enableLeiosApplyInputValidation(ls)
	input := leiosApplyTestSeedKeyUtxo(t, db, 0x71, 5_000_000)
	firstRaw, first := leiosApplyTestSignedSpendingTx(t, 1, input, 0, 5_000_000)
	secondRaw, second := leiosApplyTestSignedSpendingTx(
		t,
		2,
		first.Hash().Bytes(),
		0,
		4_799_999,
	)
	point := leiosApplyTestRankingPoint(0x72)
	contextSlot := uint64(990)
	_, err := rawDB.Exec(
		`CREATE TRIGGER require_complete_closure_witnesses BEFORE INSERT ON key_witness WHEN (SELECT COUNT(*) FROM "transaction") < 2 BEGIN SELECT RAISE(ABORT, 'API rows flushed before closure completed'); END`,
	)
	require.NoError(t, err)
	_, err = rawDB.Exec(
		`CREATE TRIGGER require_closure_parents_before_spends BEFORE UPDATE OF spent_by_tx_id ON utxo WHEN NEW.spent_by_tx_id IS NOT NULL AND (SELECT COUNT(*) FROM "transaction") < 2 BEGIN SELECT RAISE(ABORT, 'UTxO spend was not closure-batched'); END`,
	)
	require.NoError(t, err)
	var applied int
	err = db.Transaction(t.Context(), true).Do(func(txn *database.Txn) error {
		var err error
		applied, _, err = ls.applyEndorserBlockInContext(
			t.Context(),
			txn,
			point,
			1,
			990,
			leiosApplyTestEbHash(0x73),
			[]cbor.RawMessage{firstRaw, firstRaw, secondRaw},
			&contextSlot,
		)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, 2, applied)
	families, err := reg.Gather()
	require.NoError(t, err)
	queryCounts := make(map[string]uint64)
	for _, family := range families {
		if family.GetName() != "dingo_database_sql_query_duration_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "query" {
					queryCounts[label.GetValue()] += metric.GetHistogram().
						GetSampleCount()
				}
			}
		}
	}
	require.EqualValues(t, 1, queryCounts["BatchOverlayInsertUtxos"])
	require.EqualValues(t, 1, queryCounts["BatchOverlayMarkSpent"])
	requireLeiosApplyTestTxCount(t, rawDB, 2)
	var count int
	require.NoError(
		t,
		rawDB.QueryRow(`SELECT COUNT(*) FROM key_witness`).Scan(&count),
	)
	require.Equal(t, 2, count)
	require.NoError(
		t,
		rawDB.QueryRow(`SELECT COUNT(*) FROM leios_transaction_context WHERE slot = ?`, contextSlot).
			Scan(&count),
	)
	require.Equal(t, 2, count)
	spender, _ := leiosApplyTestUtxoState(t, rawDB, first.Hash().Bytes(), 0)
	require.Equal(t, second.Hash().Bytes(), spender)
	applied, err = leiosApplyTestApplyEndorserBlock(
		t,
		ls,
		db,
		leiosApplyTestRankingPoint(0x74),
		2,
		991,
		leiosApplyTestEbHash(0x75),
		firstRaw,
		secondRaw,
	)
	require.NoError(t, err)
	require.Zero(t, applied)
	requireLeiosApplyTestTxCount(t, rawDB, 2)
}

func TestLeiosAPIBatchFinalFlushFailureRollsBackAllEffects(t *testing.T) {
	t.Parallel()
	ls, db, rawDB, _ := newLeiosAPIApplyTestLedger(t)
	enableLeiosApplyInputValidation(ls)
	input := leiosApplyTestSeedKeyUtxo(t, db, 0x76, 5_000_000)
	firstRaw, first := leiosApplyTestSignedSpendingTx(t, 1, input, 0, 5_000_000)
	secondRaw, _ := leiosApplyTestSignedSpendingTx(
		t,
		2,
		first.Hash().Bytes(),
		0,
		4_799_999,
	)
	_, err := rawDB.Exec(
		`CREATE TRIGGER fail_closure_api BEFORE INSERT ON address_transaction BEGIN SELECT RAISE(ABORT, 'injected API detail flush failure'); END`,
	)
	require.NoError(t, err)
	_, err = leiosApplyTestApplyEndorserBlock(
		t,
		ls,
		db,
		leiosApplyTestRankingPoint(0x77),
		1,
		992,
		leiosApplyTestEbHash(0x78),
		firstRaw,
		secondRaw,
	)
	require.ErrorContains(t, err, "injected API detail flush failure")
	var storageErr *leiosEndorserBlockStorageError
	require.ErrorAs(t, err, &storageErr)
	requireLeiosApplyTestTxCount(t, rawDB, 0)
	spender, deleted := leiosApplyTestUtxoState(t, rawDB, input, 0)
	require.Empty(t, spender)
	require.Zero(t, deleted)
	_, err = rawDB.Exec(`DROP TRIGGER fail_closure_api`)
	require.NoError(t, err)
	_, err = leiosApplyTestApplyEndorserBlock(
		t,
		ls,
		db,
		leiosApplyTestRankingPoint(0x77),
		1,
		992,
		leiosApplyTestEbHash(0x78),
		firstRaw,
		secondRaw,
	)
	require.NoError(t, err)
	requireLeiosApplyTestTxCount(t, rawDB, 2)
}

func TestLeiosAPIBatchRetainsSameClosureAndRankingConflictChecks(t *testing.T) {
	t.Parallel()
	ls, db, rawDB, _ := newLeiosAPIApplyTestLedger(t)
	enableLeiosApplyInputValidation(ls)
	producerRaw, producer := leiosApplyTestProducerTx(t, 0x79)
	firstRaw, _ := leiosApplyTestSpendingTx(t, 0x7a, producer.Hash().Bytes(), 0)
	secondRaw, _ := leiosApplyTestSpendingTx(
		t,
		0x7b,
		producer.Hash().Bytes(),
		0,
	)
	_, err := leiosApplyTestApplyEndorserBlock(
		t,
		ls,
		db,
		leiosApplyTestRankingPoint(0x79),
		1,
		100,
		leiosApplyTestEbHash(0x79),
		producerRaw,
	)
	require.NoError(t, err)
	_, err = leiosApplyTestApplyEndorserBlock(
		t,
		ls,
		db,
		leiosApplyTestRankingPoint(0x7a),
		2,
		101,
		leiosApplyTestEbHash(0x7a),
		firstRaw,
		secondRaw,
	)
	require.ErrorIs(t, err, ErrUtxoAlreadyConsumed)
	requireLeiosApplyTestTxCount(t, rawDB, 1)
	_, err = leiosApplyTestApplyEndorserBlock(
		t,
		ls,
		db,
		leiosApplyTestRankingPoint(0x7b),
		2,
		102,
		leiosApplyTestEbHash(0x7b),
		firstRaw,
	)
	require.NoError(t, err)
	_, err = leiosApplyTestApplyEndorserBlock(
		t,
		ls,
		db,
		leiosApplyTestRankingPoint(0x7c),
		3,
		103,
		leiosApplyTestEbHash(0x7c),
		secondRaw,
	)
	require.NoError(t, err)
	requireLeiosApplyTestTxCount(t, rawDB, 3)
	rankingRaw, ranking := leiosApplyTestSpendingTx(
		t,
		0x7d,
		producer.Hash().Bytes(),
		0,
	)
	err = leiosApplyTestApplyRankingDelta(
		t,
		ls,
		db,
		leiosApplyTestRankingPoint(0x7d),
		4,
		rankingRaw,
		ranking,
	)
	require.ErrorIs(t, err, types.ErrUtxoConflict)
	requireLeiosApplyTestTxCount(t, rawDB, 3)
}

func TestLeiosAPIBatchStagesPaymentRunsAroundUnsupportedTransaction(
	t *testing.T,
) {
	t.Parallel()
	for _, name := range []string{"commit", "prefix-flush-failure", "suffix-failure"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			failPrefix := name == "prefix-flush-failure"
			failSuffix := name == "suffix-failure"
			ls, db, raw, reg := newLeiosAPIApplyTestLedger(t)
			enableLeiosApplyInputValidation(ls)
			input := leiosApplyTestSeedKeyUtxo(t, db, 0xa1, 50_000_000)
			previous, amount := input, uint64(50_000_000)
			payloads := make([]cbor.RawMessage, 0, 65)
			var barrier, prefixLast, last lcommon.Transaction
			for i := range 65 {
				rawTx, tx := leiosApplyTestSignedSpendingTx(
					t,
					byte(i+1),
					previous,
					0,
					amount,
				)
				if i == 31 {
					prefixLast = tx
				}
				if i == 32 {
					var envelope []cbor.RawMessage
					_, err := cbor.Decode(rawTx, &envelope)
					require.NoError(t, err)
					var witnesses map[uint]cbor.RawMessage
					_, err = cbor.Decode(envelope[1], &witnesses)
					require.NoError(t, err)
					witnesses[1], err = cbor.Encode(
						cbor.Tag{
							Number:  258,
							Content: []any{[]any{uint(0), pathSignerHash()}},
						},
					)
					require.NoError(t, err)
					envelope[1], err = cbor.Encode(witnesses)
					require.NoError(t, err)
					rawTx, err = cbor.Encode(envelope)
					require.NoError(t, err)
					tx, err = safedecode.DijkstraBlockTransaction(rawTx)
					require.NoError(t, err)
					require.Len(t, tx.Witnesses().NativeScripts(), 1)
					barrier = tx
				}
				payloads = append(payloads, rawTx)
				previous, amount, last = tx.Hash().
					Bytes(),
					amount-200_000-uint64(
						i+1,
					), tx
			}
			overlaySpends := func() uint64 {
				families, err := reg.Gather()
				require.NoError(t, err)
				var count uint64
				for _, family := range families {
					if family.GetName() != "dingo_database_sql_query_duration_seconds" {
						continue
					}
					for _, metric := range family.Metric {
						for _, label := range metric.Label {
							if label.GetName() == "query" &&
								label.GetValue() == "BatchOverlayMarkSpent" {
								count += metric.GetHistogram().GetSampleCount()
							}
						}
					}
				}
				return count
			}
			validate := ls.currentEra.ValidateTxFunc
			barrierValidated := false
			ls.currentEra.ValidateTxFunc = func(tx lcommon.Transaction, slot uint64, state lcommon.LedgerState, pp lcommon.ProtocolParameters) error {
				if tx.Hash() == barrier.Hash() {
					barrierValidated = true
					if overlaySpends() == 0 {
						return fmt.Errorf(
							"payment prefix was not materialized before unsupported transaction validation",
						)
					}
				}
				return validate(tx, slot, state, pp)
			}
			ls.activeEras[0].ValidateTxFunc = ls.currentEra.ValidateTxFunc
			ls.publishSnapshotsLocked()
			injectedFailure := "injected late-window failure"
			if failPrefix || failSuffix {
				failureHash := last.Hash().Bytes()
				if failPrefix {
					failureHash = prefixLast.Hash().Bytes()
					injectedFailure = "injected prefix-window failure"
				}
				_, err := raw.Exec(
					fmt.Sprintf(
						`CREATE TRIGGER fail_window BEFORE INSERT ON address_transaction WHEN (SELECT hash FROM "transaction" WHERE id=NEW.transaction_id)=X'%x' BEGIN SELECT RAISE(ABORT, '%s'); END`,
						failureHash,
						injectedFailure,
					),
				)
				require.NoError(t, err)
			}
			applied, err := leiosApplyTestApplyEndorserBlock(
				t,
				ls,
				db,
				leiosApplyTestRankingPoint(0xa2),
				1,
				1000,
				leiosApplyTestEbHash(0xa3),
				payloads...)
			require.Equal(
				t,
				!failPrefix,
				barrierValidated,
				"prefix flush must succeed before unsupported validation",
			)
			if failPrefix || failSuffix {
				require.ErrorContains(t, err, injectedFailure)
				var storageErr *leiosEndorserBlockStorageError
				require.ErrorAs(t, err, &storageErr)
				requireLeiosApplyTestTxCount(t, raw, 0)
				var count int
				require.NoError(
					t,
					raw.QueryRow(`SELECT COUNT(*) FROM utxo`).Scan(&count),
				)
				require.Equal(
					t,
					1,
					count,
					"already-materialized windows must roll back",
				)
				spender, deleted := leiosApplyTestUtxoState(t, raw, input, 0)
				require.Empty(t, spender)
				require.Zero(t, deleted)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 65, applied)
			requireLeiosApplyTestTxCount(t, raw, 65)
			require.GreaterOrEqual(
				t,
				overlaySpends(),
				uint64(2),
				"both payment runs must use retained staging",
			)
			var live int
			require.NoError(
				t,
				raw.QueryRow(`SELECT COUNT(*) FROM utxo WHERE deleted_slot=0`).
					Scan(&live),
			)
			require.Equal(t, 1, live)
		})
	}
}
