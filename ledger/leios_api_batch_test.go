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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

func newLeiosAPIApplyTestLedger(t *testing.T) (*LedgerState, *database.Database, *sql.DB) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir(), StorageMode: types.StorageModeAPI})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	ls := &LedgerState{db: db, config: LedgerStateConfig{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	initializeLeiosApplyTestState(ls)
	return ls, db, raw
}

func TestLeiosAPIBatchAppliesDependentTransactionsAndDeduplicates(t *testing.T) {
	t.Parallel()
	ls, db, rawDB := newLeiosAPIApplyTestLedger(t)
	enableLeiosApplyInputValidation(ls)
	input := leiosApplyTestSeedKeyUtxo(t, db, 0x71, 5_000_000)
	firstRaw, first := leiosApplyTestSignedSpendingTx(t, 1, input, 0, 5_000_000)
	secondRaw, second := leiosApplyTestSignedSpendingTx(t, 2, first.Hash().Bytes(), 0, 4_799_999)
	point := leiosApplyTestRankingPoint(0x72)
	contextSlot := uint64(990)
	_, err := rawDB.Exec(`CREATE TRIGGER require_complete_closure_witnesses BEFORE INSERT ON key_witness WHEN (SELECT COUNT(*) FROM "transaction") < 2 BEGIN SELECT RAISE(ABORT, 'API rows flushed before closure completed'); END`)
	require.NoError(t, err)
	var applied int
	err = db.Transaction(t.Context(), true).Do(func(txn *database.Txn) error {
		var err error
		applied, _, err = ls.applyEndorserBlockInContext(t.Context(), txn, point, 1, 990, leiosApplyTestEbHash(0x73), []cbor.RawMessage{firstRaw, firstRaw, secondRaw}, &contextSlot)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, 2, applied)
	requireLeiosApplyTestTxCount(t, rawDB, 2)
	var count int
	require.NoError(t, rawDB.QueryRow(`SELECT COUNT(*) FROM key_witness`).Scan(&count))
	require.Equal(t, 2, count)
	require.NoError(t, rawDB.QueryRow(`SELECT COUNT(*) FROM leios_transaction_context WHERE slot = ?`, contextSlot).Scan(&count))
	require.Equal(t, 2, count)
	spender, _ := leiosApplyTestUtxoState(t, rawDB, first.Hash().Bytes(), 0)
	require.Equal(t, second.Hash().Bytes(), spender)
	applied, err = leiosApplyTestApplyEndorserBlock(t, ls, db, leiosApplyTestRankingPoint(0x74), 2, 991, leiosApplyTestEbHash(0x75), firstRaw, secondRaw)
	require.NoError(t, err)
	require.Zero(t, applied)
	requireLeiosApplyTestTxCount(t, rawDB, 2)
}

func TestLeiosAPIBatchFinalFlushFailureRollsBackAllEffects(t *testing.T) {
	t.Parallel()
	ls, db, rawDB := newLeiosAPIApplyTestLedger(t)
	enableLeiosApplyInputValidation(ls)
	input := leiosApplyTestSeedKeyUtxo(t, db, 0x76, 5_000_000)
	firstRaw, first := leiosApplyTestSignedSpendingTx(t, 1, input, 0, 5_000_000)
	secondRaw, _ := leiosApplyTestSignedSpendingTx(t, 2, first.Hash().Bytes(), 0, 4_799_999)
	_, err := rawDB.Exec(`CREATE TRIGGER fail_closure_api BEFORE INSERT ON address_transaction BEGIN SELECT RAISE(ABORT, 'injected API detail flush failure'); END`)
	require.NoError(t, err)
	_, err = leiosApplyTestApplyEndorserBlock(t, ls, db, leiosApplyTestRankingPoint(0x77), 1, 992, leiosApplyTestEbHash(0x78), firstRaw, secondRaw)
	require.ErrorContains(t, err, "injected API detail flush failure")
	var storageErr *leiosEndorserBlockStorageError
	require.ErrorAs(t, err, &storageErr)
	requireLeiosApplyTestTxCount(t, rawDB, 0)
	spender, deleted := leiosApplyTestUtxoState(t, rawDB, input, 0)
	require.Empty(t, spender)
	require.Zero(t, deleted)
	_, err = rawDB.Exec(`DROP TRIGGER fail_closure_api`)
	require.NoError(t, err)
	_, err = leiosApplyTestApplyEndorserBlock(t, ls, db, leiosApplyTestRankingPoint(0x77), 1, 992, leiosApplyTestEbHash(0x78), firstRaw, secondRaw)
	require.NoError(t, err)
	requireLeiosApplyTestTxCount(t, rawDB, 2)
}

func TestLeiosAPIBatchRetainsSameClosureAndRankingConflictChecks(t *testing.T) {
	t.Parallel()
	ls, db, rawDB := newLeiosAPIApplyTestLedger(t)
	enableLeiosApplyInputValidation(ls)
	producerRaw, producer := leiosApplyTestProducerTx(t, 0x79)
	firstRaw, _ := leiosApplyTestSpendingTx(t, 0x7a, producer.Hash().Bytes(), 0)
	secondRaw, _ := leiosApplyTestSpendingTx(t, 0x7b, producer.Hash().Bytes(), 0)
	_, err := leiosApplyTestApplyEndorserBlock(t, ls, db, leiosApplyTestRankingPoint(0x79), 1, 100, leiosApplyTestEbHash(0x79), producerRaw)
	require.NoError(t, err)
	_, err = leiosApplyTestApplyEndorserBlock(t, ls, db, leiosApplyTestRankingPoint(0x7a), 2, 101, leiosApplyTestEbHash(0x7a), firstRaw, secondRaw)
	require.ErrorIs(t, err, ErrUtxoAlreadyConsumed)
	requireLeiosApplyTestTxCount(t, rawDB, 1)
	_, err = leiosApplyTestApplyEndorserBlock(t, ls, db, leiosApplyTestRankingPoint(0x7b), 2, 102, leiosApplyTestEbHash(0x7b), firstRaw)
	require.NoError(t, err)
	_, err = leiosApplyTestApplyEndorserBlock(t, ls, db, leiosApplyTestRankingPoint(0x7c), 3, 103, leiosApplyTestEbHash(0x7c), secondRaw)
	require.NoError(t, err)
	requireLeiosApplyTestTxCount(t, rawDB, 3)
	rankingRaw, ranking := leiosApplyTestSpendingTx(t, 0x7d, producer.Hash().Bytes(), 0)
	err = leiosApplyTestApplyRankingDelta(t, ls, db, leiosApplyTestRankingPoint(0x7d), 4, rankingRaw, ranking)
	require.ErrorIs(t, err, types.ErrUtxoConflict)
	requireLeiosApplyTestTxCount(t, rawDB, 3)
}
