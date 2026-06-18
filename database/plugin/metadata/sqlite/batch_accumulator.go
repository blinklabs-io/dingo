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
	"fmt"
	"maps"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
)

const (
	batchChunkRows = 100
)

// utxoSpend captures the information needed to mark a UTxO as spent.
type utxoSpend struct {
	TxId          []byte
	OutputIdx     uint32
	Slot          uint64
	SpentByTxHash []byte
}

// BatchAccumulator collects metadata rows across multiple transactions
// for bulk database insertion. It is the foundational data structure for
// the backfill batching optimization that reduces per-block SQL statements
// from 30-150 down to 2-5 by batching writes across 50-100 blocks.
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
	stats          *types.BackfillHotPathStats
	// producedByRef indexes produced outputs and collateral returns by their
	// "%x:%d" tx-id/output-index ref so a later transaction in the same batch
	// can resolve provenance and address keys for an output that has not yet
	// been flushed to the database. Eagerly allocated so the Add/lookup
	// methods never need a nil check.
	producedByRef map[string]models.Utxo
}

// utxoRefKey builds the "%x:%d" lookup key shared by the in-flight index,
// GetUtxoAddressKeysBatch's result map, and the consumed-input dedup loops.
func utxoRefKey(txId []byte, outputIdx uint32) string {
	return fmt.Sprintf("%x:%d", txId, outputIdx)
}

// NewBatchAccumulator returns an empty BatchAccumulator ready for use.
func NewBatchAccumulator() *BatchAccumulator {
	return &BatchAccumulator{
		producedByRef: make(map[string]models.Utxo),
	}
}

// NewBatchAccumulator creates an accumulator for this metadata store.
func (d *MetadataStoreSqlite) NewBatchAccumulator() types.MetadataBatchAccumulator {
	return NewBatchAccumulator()
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
	b.producedByRef[utxoRefKey(u.TxId, u.OutputIdx)] = u
}

// AddUtxoSpend appends a consumed UTxO record to the batch.
func (b *BatchAccumulator) AddUtxoSpend(s utxoSpend) {
	b.UtxoSpends = append(b.UtxoSpends, s)
}

// AddCollateralReturn appends a collateral return UTxO to the batch.
func (b *BatchAccumulator) AddCollateralReturn(u models.Utxo) {
	b.CollateralRets = append(b.CollateralRets, u)
	b.producedByRef[utxoRefKey(u.TxId, u.OutputIdx)] = u
}

// InFlightAddressKeys returns the address-key projection for an output
// produced earlier in the current batch (and not yet flushed), so a later
// transaction can resolve it without a database round trip.
func (b *BatchAccumulator) InFlightAddressKeys(
	txId []byte,
	outputIdx uint32,
) (UtxoAddressKeys, bool) {
	u, ok := b.producedByRef[utxoRefKey(txId, outputIdx)]
	if !ok {
		return UtxoAddressKeys{}, false
	}
	return utxoAddressKeysFromUtxo(u), true
}

// HasInFlightProducer reports whether an output produced earlier in the
// current batch (and not yet flushed) matches the given ref. The database
// layer uses this to skip blob recovery for same-batch provenance.
func (b *BatchAccumulator) HasInFlightProducer(
	txId []byte,
	outputIdx uint32,
) bool {
	_, ok := b.producedByRef[utxoRefKey(txId, outputIdx)]
	return ok
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
	clear(b.producedByRef)
	b.stats = nil
}

// SetBackfillStats attaches the current backfill interval stats collector.
func (b *BatchAccumulator) SetBackfillStats(
	stats *types.BackfillHotPathStats,
) {
	if b != nil {
		b.stats = stats
	}
}

// Len returns the number of accumulated metadata rows. It is intentionally a
// simple row-count estimate used by backfill to bound batch memory.
func (b *BatchAccumulator) Len() int {
	if b == nil {
		return 0
	}
	return len(b.KeyWitnesses) +
		len(b.WitnessScripts) +
		len(b.Scripts) +
		len(b.PlutusData) +
		len(b.Redeemers) +
		len(b.AddressTxs) +
		len(b.UtxoOutputs) +
		len(b.UtxoSpends) +
		len(b.CollateralRets) +
		len(b.DeleteTxIDs)
}

// MergeFrom appends all records from other into b.
// It is a no-op when other is nil or other == b.
func (b *BatchAccumulator) MergeFrom(other *BatchAccumulator) {
	if other == nil || other == b {
		return
	}
	b.KeyWitnesses = append(b.KeyWitnesses, other.KeyWitnesses...)
	b.WitnessScripts = append(b.WitnessScripts, other.WitnessScripts...)
	b.Scripts = append(b.Scripts, other.Scripts...)
	b.PlutusData = append(b.PlutusData, other.PlutusData...)
	b.Redeemers = append(b.Redeemers, other.Redeemers...)
	b.AddressTxs = append(b.AddressTxs, other.AddressTxs...)
	b.UtxoOutputs = append(b.UtxoOutputs, other.UtxoOutputs...)
	b.UtxoSpends = append(b.UtxoSpends, other.UtxoSpends...)
	b.CollateralRets = append(b.CollateralRets, other.CollateralRets...)
	b.DeleteTxIDs = append(b.DeleteTxIDs, other.DeleteTxIDs...)
	maps.Copy(b.producedByRef, other.producedByRef)
}

// addPendingCountsToStats reports rows buffered for the next batch flush.
func (b *BatchAccumulator) addPendingCountsToStats() {
	if b == nil || b.stats == nil {
		return
	}
	// Count deferred metadata rows accumulated for the next FlushBatch.
	b.stats.Witnesses += uint64(len(b.KeyWitnesses))
	b.stats.WitnessScripts += uint64(len(b.WitnessScripts))
	b.stats.Scripts += uint64(len(b.Scripts))
	b.stats.PlutusData += uint64(len(b.PlutusData))
	b.stats.Redeemers += uint64(len(b.Redeemers))
	b.stats.AddressTxs += uint64(len(b.AddressTxs))
	b.stats.UtxoSpends += uint64(len(b.UtxoSpends))
	b.stats.CollateralRets += uint64(len(b.CollateralRets))
	b.stats.DeleteTxIDs += uint64(len(b.DeleteTxIDs))
}

// FlushBatch writes all accumulated records in a deterministic order.
func (d *MetadataStoreSqlite) FlushBatch(
	acc types.MetadataBatchAccumulator,
	txn types.Txn,
) error {
	batch, ok := acc.(*BatchAccumulator)
	if !ok {
		return fmt.Errorf("sqlite FlushBatch: wrong accumulator type %T", acc)
	}
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

		if err := insertKeyWitnesses(db, batch.KeyWitnesses); err != nil {
			return fmt.Errorf("flush batch: create key witnesses: %w", err)
		}
		if err := insertWitnessScripts(db, batch.WitnessScripts); err != nil {
			return fmt.Errorf("flush batch: create witness scripts: %w", err)
		}
		if err := insertScripts(db, batch.Scripts); err != nil {
			return fmt.Errorf("flush batch: create scripts: %w", err)
		}
		if err := insertPlutusData(db, batch.PlutusData); err != nil {
			return fmt.Errorf("flush batch: create plutus data: %w", err)
		}
		if err := insertRedeemers(db, batch.Redeemers); err != nil {
			return fmt.Errorf("flush batch: create redeemers: %w", err)
		}
		if err := insertAddressTxs(db, batch.AddressTxs); err != nil {
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

func batchCreateUtxos(db *gorm.DB, items []models.Utxo) error {
	if len(items) == 0 {
		return nil
	}
	return importUtxosWithDB(db, items)
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
	// Stable per-row UPDATE reused for every spend (one prepared statement),
	// replacing the prior dynamic CASE whose shape changed per chunk and so
	// could not be reused. The deleted_slot = 0 guard keeps re-application
	// idempotent: an already-spent row is skipped, which resumed backfill and
	// the #2459 create-before-spend flush ordering rely on.
	query := "UPDATE " + utxoRefIndexedTable() +
		" SET deleted_slot = ?, spent_at_tx_id = ?" +
		" WHERE deleted_slot = 0 AND tx_id = ? AND output_idx = ?"
	for i := range spends {
		s := &spends[i]
		if _, err := execRawOnConn(db, query, []any{
			s.Slot, s.SpentByTxHash, s.TxId, s.OutputIdx,
		}); err != nil {
			return err
		}
	}
	return nil
}
