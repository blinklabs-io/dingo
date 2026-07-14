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

package sqlite

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/internal/collateralfee"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"math/big"
)

func collateralFeeTestHash(fill byte) lcommon.Blake2b256 {
	var h [32]byte
	for i := range h {
		h[i] = fill
	}
	return lcommon.NewBlake2b256(h[:])
}

func seedCollateralFeeUtxo(
	t *testing.T,
	db *gorm.DB,
	txId lcommon.Blake2b256,
	outputIdx uint32,
	amount uint64,
) {
	t.Helper()
	require.NoError(t, db.Create(&models.Utxo{
		TxId:      txId.Bytes(),
		OutputIdx: outputIdx,
		AddedSlot: 1,
		Amount:    types.Uint64(amount),
	}).Error)
}

// TestCollateralFeeInvalidTransaction verifies that ingesting a
// phase-2-invalid transaction records the consumed collateral (inputs minus
// collateral return) as the transaction's fee-pot contribution, per the
// Alonzo/Babbage UTXOS rule, and that SumTransactionFeesInSlotRange counts
// declared fees for valid transactions plus consumed collateral for invalid
// ones.
func TestCollateralFeeInvalidTransaction(t *testing.T) {
	store := setupTestDB(t)
	db := store.DB()

	producerHash := collateralFeeTestHash(0xaa)
	seedCollateralFeeUtxo(t, db, producerHash, 0, 700_000)
	seedCollateralFeeUtxo(t, db, producerHash, 1, 300_000)

	invalidTx := &mockTransaction{
		hash:    collateralFeeTestHash(0x01),
		isValid: false,
		collateral: []lcommon.TransactionInput{
			mockTransactionInput{hash: producerHash, index: 0},
			mockTransactionInput{hash: producerHash, index: 1},
		},
		collReturn: &mockTransactionOutput{amount: big.NewInt(400_000)},
	}
	point := ocommon.Point{Slot: 50, Hash: collateralFeeTestHash(0xbb).Bytes()}
	require.NoError(t, store.SetTransaction(invalidTx, point, 0, nil, nil))

	validTx := &mockTransaction{
		hash:    collateralFeeTestHash(0x02),
		isValid: true,
		// Collateral on a valid transaction is not consumed and must not
		// contribute to the fee pot.
		collateral: []lcommon.TransactionInput{
			mockTransactionInput{hash: producerHash, index: 0},
		},
	}
	point2 := ocommon.Point{Slot: 60, Hash: collateralFeeTestHash(0xcc).Bytes()}
	require.NoError(t, store.SetTransaction(validTx, point2, 0, nil, nil))

	var rows []models.Transaction
	require.NoError(
		t,
		db.Order("slot").Find(&rows).Error,
	)
	require.Len(t, rows, 2)
	// 700000 + 300000 - 400000
	assert.Equal(t, uint64(600_000), uint64(rows[0].CollateralFee))
	assert.False(t, rows[0].Valid)
	assert.Equal(t, uint64(0), uint64(rows[1].CollateralFee))
	assert.True(t, rows[1].Valid)

	// The mock's declared fee is 1000: counted for the valid transaction,
	// replaced by consumed collateral for the invalid one.
	total, err := store.SumTransactionFeesInSlotRange(0, 100, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(600_000+1000), total)
}

// TestCollateralFeeTotalCollateralShortcut verifies the declared
// total-collateral field is used directly when present (the Babbage UTXO
// rule requires it to equal the consumed balance), without resolving the
// collateral inputs.
func TestCollateralFeeTotalCollateralShortcut(t *testing.T) {
	store := setupTestDB(t)
	db := store.DB()

	inputHash := collateralFeeTestHash(0xdd)
	shelleyInput := shelley.NewShelleyTransactionInput(
		inputHash.String(), 0,
	)
	tx := &babbage.BabbageTransaction{
		Body: babbage.BabbageTransactionBody{
			TxCollateral: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{shelleyInput},
				true,
			),
			TxTotalCollateral: 123_456,
		},
		TxIsValid: false,
	}
	// The collateral input is deliberately not present in the utxo table:
	// the declared total must satisfy the computation on its own.
	fee, resolved, err := collateralfee.ForTransaction(db, tx, nil)
	require.NoError(t, err)
	assert.True(t, resolved)
	assert.Equal(t, uint64(123_456), fee)
}

// TestCollateralFeeUnresolvedInput verifies that an unresolvable collateral
// input yields a lower-bound amount and reports incomplete resolution
// instead of failing ingestion.
func TestCollateralFeeUnresolvedInput(t *testing.T) {
	store := setupTestDB(t)
	db := store.DB()

	producerHash := collateralFeeTestHash(0xee)
	seedCollateralFeeUtxo(t, db, producerHash, 0, 500_000)

	tx := &mockTransaction{
		hash:    collateralFeeTestHash(0x03),
		isValid: false,
		collateral: []lcommon.TransactionInput{
			mockTransactionInput{hash: producerHash, index: 0},
			// Not present in the utxo table.
			mockTransactionInput{hash: collateralFeeTestHash(0xef), index: 9},
		},
	}
	fee, resolved, err := collateralfee.ForTransaction(db, tx, nil)
	require.NoError(t, err)
	assert.False(t, resolved)
	assert.Equal(t, uint64(500_000), fee)
}

// TestCollateralFeePendingProducer verifies the in-flight fallback used by
// the batched ingest path when the collateral producer has not been flushed
// to the utxo table yet.
func TestCollateralFeePendingProducer(t *testing.T) {
	store := setupTestDB(t)
	db := store.DB()

	producerHash := collateralFeeTestHash(0xf1)
	acc := NewBatchAccumulator()
	acc.AddUtxoOutput(models.Utxo{
		TxId:      producerHash.Bytes(),
		OutputIdx: 2,
		Amount:    types.Uint64(250_000),
	})

	tx := &mockTransaction{
		hash:    collateralFeeTestHash(0x04),
		isValid: false,
		collateral: []lcommon.TransactionInput{
			mockTransactionInput{hash: producerHash, index: 2},
		},
	}
	fee, resolved, err := collateralfee.ForTransaction(
		db, tx, acc.InFlightProducerAmount,
	)
	require.NoError(t, err)
	assert.True(t, resolved)
	assert.Equal(t, uint64(250_000), fee)
}

// TestCollateralFeeReplayCorrectsStaleValue verifies the normal and batched
// SQLite transaction upsert paths refresh a previously stored collateral fee.
// This matters when ingestion resumes or replays after the collateral inputs
// become available.
func TestCollateralFeeReplayCorrectsStaleValue(t *testing.T) {
	testCases := []struct {
		name   string
		ingest func(
			*MetadataStoreSqlite,
			lcommon.Transaction,
			ocommon.Point,
		) error
	}{
		{
			name: "normal",
			ingest: func(
				store *MetadataStoreSqlite,
				tx lcommon.Transaction,
				point ocommon.Point,
			) error {
				return store.SetTransaction(tx, point, 0, nil, nil)
			},
		},
		{
			name: "batched",
			ingest: func(
				store *MetadataStoreSqlite,
				tx lcommon.Transaction,
				point ocommon.Point,
			) error {
				return store.SetTransactionBatched(
					tx,
					point,
					0,
					nil,
					NewBatchAccumulator(),
					nil,
				)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			store := setupTestDB(t)
			db := store.DB()
			producerHash := collateralFeeTestHash(0xf2)
			txHash := collateralFeeTestHash(0x05)
			seedCollateralFeeUtxo(t, db, producerHash, 0, 750_000)
			require.NoError(t, db.Create(&models.Transaction{
				Hash:          txHash.Bytes(),
				CollateralFee: types.Uint64(1),
			}).Error)

			tx := &mockTransaction{
				hash:    txHash,
				isValid: false,
				collateral: []lcommon.TransactionInput{
					mockTransactionInput{hash: producerHash, index: 0},
				},
			}
			point := ocommon.Point{
				Slot: 75,
				Hash: collateralFeeTestHash(0xf3).Bytes(),
			}
			require.NoError(t, testCase.ingest(store, tx, point))

			var row models.Transaction
			require.NoError(
				t,
				db.Where("hash = ?", txHash.Bytes()).Take(&row).Error,
			)
			assert.Equal(t, uint64(750_000), uint64(row.CollateralFee))
		})
	}
}

// TestGapBlockCollateralFeeInvalidTransaction verifies that ingesting a
// phase-2-invalid transaction through the Mithril gap-block path records its
// consumed collateral (inputs minus collateral return) as the fee-pot
// contribution, matching the normal SetTransaction path. Before the gap path
// computed the collateral fee it left this column at zero, silently
// undercounting the epoch fee pot for any invalid transaction in a gap range.
func TestGapBlockCollateralFeeInvalidTransaction(t *testing.T) {
	store := setupTestDB(t)
	db := store.DB()

	producerHash := collateralFeeTestHash(0x5a)
	seedCollateralFeeUtxo(t, db, producerHash, 0, 700_000)
	seedCollateralFeeUtxo(t, db, producerHash, 1, 300_000)

	invalidTx := &mockTransaction{
		hash:    collateralFeeTestHash(0x11),
		isValid: false,
		collateral: []lcommon.TransactionInput{
			mockTransactionInput{hash: producerHash, index: 0},
			mockTransactionInput{hash: producerHash, index: 1},
		},
		collReturn: &mockTransactionOutput{amount: big.NewInt(400_000)},
	}
	point := ocommon.Point{Slot: 50, Hash: collateralFeeTestHash(0xbb).Bytes()}
	require.NoError(t, store.SetGapBlockTransaction(invalidTx, point, 0, nil))

	var row models.Transaction
	require.NoError(
		t,
		db.Where("hash = ?", invalidTx.hash.Bytes()).Take(&row).Error,
	)
	// 700000 + 300000 - 400000
	assert.Equal(t, uint64(600_000), uint64(row.CollateralFee))
	assert.False(t, row.Valid)

	total, err := store.SumTransactionFeesInSlotRange(0, 100, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(600_000), total)
}

// TestRecomputeGapCollateralFeeAfterInputRecovery verifies the fee-pot
// correction path for Mithril gap imports. SetGapBlockTransaction computes the
// collateral fee before the consumed collateral inputs are recovered into the
// utxo table, so a transaction that declares no total collateral is initially
// undercounted (zero here, because the collateral return exceeds the resolved
// inputs). Once the inputs are materialized (as Database.ensureGapConsumedUtxos
// does at ingest time), RecomputeGapCollateralFee restores the correct
// consumed collateral so the epoch fee pot is not permanently short.
func TestRecomputeGapCollateralFeeAfterInputRecovery(t *testing.T) {
	store := setupTestDB(t)
	db := store.DB()

	producerHash := collateralFeeTestHash(0x6b)
	invalidTx := &mockTransaction{
		hash:    collateralFeeTestHash(0x12),
		isValid: false,
		collateral: []lcommon.TransactionInput{
			mockTransactionInput{hash: producerHash, index: 0},
			mockTransactionInput{hash: producerHash, index: 1},
		},
		collReturn: &mockTransactionOutput{amount: big.NewInt(400_000)},
	}
	point := ocommon.Point{Slot: 50, Hash: collateralFeeTestHash(0xbb).Bytes()}

	// Collateral inputs are not yet in the utxo table, so the gap insert can
	// only record a lower bound.
	require.NoError(t, store.SetGapBlockTransaction(invalidTx, point, 0, nil))
	var beforeRow models.Transaction
	require.NoError(
		t,
		db.Where("hash = ?", invalidTx.hash.Bytes()).Take(&beforeRow).Error,
	)
	assert.Equal(t, uint64(0), uint64(beforeRow.CollateralFee))

	// Recovery materializes the consumed collateral inputs (mirroring
	// Database.ensureGapConsumedUtxos), then the recompute corrects the fee.
	seedCollateralFeeUtxo(t, db, producerHash, 0, 700_000)
	seedCollateralFeeUtxo(t, db, producerHash, 1, 300_000)
	require.NoError(t, store.RecomputeGapCollateralFee(invalidTx, point, nil))

	var afterRow models.Transaction
	require.NoError(
		t,
		db.Where("hash = ?", invalidTx.hash.Bytes()).Take(&afterRow).Error,
	)
	// 700000 + 300000 - 400000
	assert.Equal(t, uint64(600_000), uint64(afterRow.CollateralFee))
}
