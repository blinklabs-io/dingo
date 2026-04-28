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

package postgres

import (
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	batchChunkSize = 499
	batchChunkRows = 100
)

type utxoSpend struct {
	TxId          []byte
	OutputIdx     uint32
	Slot          uint64
	SpentByTxHash []byte
}

// BatchAccumulator collects metadata rows across multiple transactions
// for bulk database insertion.
type BatchAccumulator struct {
	KeyWitnesses   []models.KeyWitness
	WitnessScripts []models.WitnessScripts
	Scripts        []models.Script
	PlutusData     []models.PlutusData
	Redeemers      []models.Redeemer
	AddressTxs     []models.AddressTransaction
	UtxoOutputs    []models.Utxo
	UtxoSpends     []utxoSpend
	CollateralRets []models.Utxo
	DeleteTxIDs    []uint
}

func NewBatchAccumulator() *BatchAccumulator {
	return &BatchAccumulator{}
}

// AddKeyWitness appends a key witness record to the batch.
func (b *BatchAccumulator) AddKeyWitness(kw models.KeyWitness) {
	b.KeyWitnesses = append(b.KeyWitnesses, kw)
}

// AddWitnessScript appends a witness script record to the batch.
func (b *BatchAccumulator) AddWitnessScript(ws models.WitnessScripts) {
	b.WitnessScripts = append(b.WitnessScripts, ws)
}

// AddScript appends a script record to the batch.
func (b *BatchAccumulator) AddScript(s models.Script) {
	b.Scripts = append(b.Scripts, s)
}

// AddPlutusData appends a plutus data record to the batch.
func (b *BatchAccumulator) AddPlutusData(pd models.PlutusData) {
	b.PlutusData = append(b.PlutusData, pd)
}

// AddRedeemer appends a redeemer record to the batch.
func (b *BatchAccumulator) AddRedeemer(r models.Redeemer) {
	b.Redeemers = append(b.Redeemers, r)
}

// AddAddressTx appends an address-transaction record to the batch.
func (b *BatchAccumulator) AddAddressTx(at models.AddressTransaction) {
	b.AddressTxs = append(b.AddressTxs, at)
}

// AddUtxoOutput appends a produced UTxO record to the batch.
func (b *BatchAccumulator) AddUtxoOutput(u models.Utxo) {
	b.UtxoOutputs = append(b.UtxoOutputs, u)
}

// AddUtxoSpend appends a consumed UTxO record to the batch.
func (b *BatchAccumulator) AddUtxoSpend(s utxoSpend) {
	b.UtxoSpends = append(b.UtxoSpends, s)
}

// AddCollateralReturn appends a collateral return UTxO to the batch.
func (b *BatchAccumulator) AddCollateralReturn(u models.Utxo) {
	b.CollateralRets = append(b.CollateralRets, u)
}

// AddDeleteTxID appends a transaction ID scheduled for idempotent
// retry deletion.
func (b *BatchAccumulator) AddDeleteTxID(id uint) {
	b.DeleteTxIDs = append(b.DeleteTxIDs, id)
}

// Reset clears all accumulated slices, reusing backing arrays to
// reduce GC pressure across flush cycles.
func (b *BatchAccumulator) Reset() {
	b.KeyWitnesses = b.KeyWitnesses[:0]
	b.WitnessScripts = b.WitnessScripts[:0]
	b.Scripts = b.Scripts[:0]
	b.PlutusData = b.PlutusData[:0]
	b.Redeemers = b.Redeemers[:0]
	b.AddressTxs = b.AddressTxs[:0]
	b.UtxoOutputs = b.UtxoOutputs[:0]
	b.UtxoSpends = b.UtxoSpends[:0]
	b.CollateralRets = b.CollateralRets[:0]
	b.DeleteTxIDs = b.DeleteTxIDs[:0]
}

// FlushBatch writes all accumulated records in a deterministic order.
func (d *MetadataStorePostgres) FlushBatch(
	batch *BatchAccumulator,
	txn types.Txn,
) error {
	if batch == nil {
		return nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("flush batch: resolve db: %w", err)
	}
	flushFn := func(db *gorm.DB) error {
		if err := batchDeleteByTxIDs(
			db,
			models.KeyWitness{}.TableName(),
			batch.DeleteTxIDs,
		); err != nil {
			return fmt.Errorf("flush batch: delete key witnesses: %w", err)
		}
		if err := batchDeleteByTxIDs(
			db,
			models.WitnessScripts{}.TableName(),
			batch.DeleteTxIDs,
		); err != nil {
			return fmt.Errorf("flush batch: delete witness scripts: %w", err)
		}
		if err := batchDeleteByTxIDs(
			db,
			models.PlutusData{}.TableName(),
			batch.DeleteTxIDs,
		); err != nil {
			return fmt.Errorf("flush batch: delete plutus data: %w", err)
		}
		if err := batchDeleteByTxIDs(
			db,
			models.Redeemer{}.TableName(),
			batch.DeleteTxIDs,
		); err != nil {
			return fmt.Errorf("flush batch: delete redeemers: %w", err)
		}
		if err := batchDeleteByTxIDs(
			db,
			models.AddressTransaction{}.TableName(),
			batch.DeleteTxIDs,
		); err != nil {
			return fmt.Errorf(
				"flush batch: delete address transactions: %w",
				err,
			)
		}

		if err := batchCreateUtxos(db, batch.UtxoOutputs); err != nil {
			return fmt.Errorf("flush batch: create utxo outputs: %w", err)
		}
		if err := batchCreateUtxos(db, batch.CollateralRets); err != nil {
			return fmt.Errorf(
				"flush batch: create collateral returns: %w",
				err,
			)
		}
		if err := batchSpendUtxos(db, batch.UtxoSpends); err != nil {
			return fmt.Errorf("flush batch: spend utxos: %w", err)
		}

		if err := batchCreate(db, batch.KeyWitnesses); err != nil {
			return fmt.Errorf("flush batch: create key witnesses: %w", err)
		}
		if err := batchCreate(db, batch.WitnessScripts); err != nil {
			return fmt.Errorf("flush batch: create witness scripts: %w", err)
		}
		if err := batchCreateScripts(db, batch.Scripts); err != nil {
			return fmt.Errorf("flush batch: create scripts: %w", err)
		}
		if err := batchCreate(db, batch.PlutusData); err != nil {
			return fmt.Errorf("flush batch: create plutus data: %w", err)
		}
		if err := batchCreate(db, batch.Redeemers); err != nil {
			return fmt.Errorf("flush batch: create redeemers: %w", err)
		}
		if err := batchCreate(db, batch.AddressTxs); err != nil {
			return fmt.Errorf("flush batch: create address txs: %w", err)
		}
		return nil
	}

	if txn != nil {
		return flushFn(db)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return flushFn(tx)
	}); err != nil {
		return fmt.Errorf("flush batch transaction: %w", err)
	}
	return nil
}

func batchCreate[T any](db *gorm.DB, items []T) error {
	if len(items) == 0 {
		return nil
	}
	if result := db.CreateInBatches(items, batchChunkRows); result.Error != nil {
		return result.Error
	}
	return nil
}

func batchCreateUtxos(db *gorm.DB, items []models.Utxo) error {
	if len(items) == 0 {
		return nil
	}
	if result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tx_id"}, {Name: "output_idx"}},
		DoNothing: true,
	}).CreateInBatches(items, batchChunkRows); result.Error != nil {
		return result.Error
	}
	return nil
}

func batchCreateScripts(db *gorm.DB, items []models.Script) error {
	if len(items) == 0 {
		return nil
	}
	if result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "hash"}},
		DoNothing: true,
	}).CreateInBatches(items, batchChunkRows); result.Error != nil {
		return result.Error
	}
	return nil
}

func batchDeleteByTxIDs(db *gorm.DB, table string, ids []uint) error {
	if len(ids) == 0 {
		return nil
	}
	for i := 0; i < len(ids); i += batchChunkSize {
		end := min(i+batchChunkSize, len(ids))
		if result := db.Table(table).
			Where("transaction_id IN ?", ids[i:end]).
			Delete(nil); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

func batchSpendUtxos(db *gorm.DB, spends []utxoSpend) error {
	if len(spends) == 0 {
		return nil
	}
	for i := 0; i < len(spends); i += batchChunkRows {
		end := min(i+batchChunkRows, len(spends))
		chunk := spends[i:end]
		var deletedSlotCases []string
		var spentAtCases []string
		var whereConditions []string
		var deletedSlotArgs []any
		var spentAtArgs []any
		var whereArgs []any
		for _, spend := range chunk {
			deletedSlotCases = append(
				deletedSlotCases,
				"WHEN tx_id = ? AND output_idx = ? THEN ?",
			)
			deletedSlotArgs = append(
				deletedSlotArgs,
				spend.TxId,
				spend.OutputIdx,
				spend.Slot,
			)

			spentAtCases = append(
				spentAtCases,
				"WHEN tx_id = ? AND output_idx = ? THEN ?",
			)
			spentAtArgs = append(
				spentAtArgs,
				spend.TxId,
				spend.OutputIdx,
				spend.SpentByTxHash,
			)

			whereConditions = append(
				whereConditions,
				"(tx_id = ? AND output_idx = ?)",
			)
			whereArgs = append(whereArgs, spend.TxId, spend.OutputIdx)
		}
		args := make([]any, 0, len(deletedSlotArgs)+len(spentAtArgs)+len(whereArgs))
		args = append(args, deletedSlotArgs...)
		args = append(args, spentAtArgs...)
		args = append(args, whereArgs...)
		sql := fmt.Sprintf(
			"UPDATE utxo SET deleted_slot = CASE %s ELSE deleted_slot END, spent_at_tx_id = CASE %s ELSE spent_at_tx_id END WHERE deleted_slot = 0 AND (%s)",
			strings.Join(deletedSlotCases, " "),
			strings.Join(spentAtCases, " "),
			strings.Join(whereConditions, " OR "),
		)
		if result := db.Exec(sql, args...); result.Error != nil {
			return result.Error
		}
	}
	return nil
}
