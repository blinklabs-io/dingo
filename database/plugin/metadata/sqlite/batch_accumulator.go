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
	"github.com/blinklabs-io/dingo/database/models"
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
}

// NewBatchAccumulator returns an empty BatchAccumulator ready for use.
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
