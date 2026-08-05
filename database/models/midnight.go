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

package models

const (
	MidnightGovernanceDatumTypeTechnicalCommittee = "technical_committee"
	MidnightGovernanceDatumTypeCouncil            = "council"
)

// MidnightAssetCreate stores cNIGHT UTxO creations for the Midnight indexer.
type MidnightAssetCreate struct {
	ID               uint
	Address          []byte
	Quantity         uint64
	TxHash           []byte
	OutputIndex      uint32
	BlockNumber      uint64
	BlockHash        []byte
	TxIndex          uint32
	BlockTimestampMs uint64
}

// BlockTxPosition implements pagination.BlockTxPositioned.
func (r MidnightAssetCreate) BlockTxPosition() (blockNumber uint64, txIndex uint32) {
	return r.BlockNumber, r.TxIndex
}

// MidnightAssetSpend stores cNIGHT UTxO spends for the Midnight indexer.
type MidnightAssetSpend struct {
	ID               uint
	Address          []byte
	Quantity         uint64
	SpendingTxHash   []byte
	UtxoTxHash       []byte
	UtxoIndex        uint32
	BlockNumber      uint64
	BlockHash        []byte
	TxIndex          uint32
	BlockTimestampMs uint64
}

// BlockTxPosition implements pagination.BlockTxPositioned.
func (r MidnightAssetSpend) BlockTxPosition() (blockNumber uint64, txIndex uint32) {
	return r.BlockNumber, r.TxIndex
}

// MidnightRegistration stores mapping validator registration events.
type MidnightRegistration struct {
	ID               uint
	FullDatum        []byte
	TxHash           []byte
	OutputIndex      uint32
	BlockNumber      uint64
	BlockHash        []byte
	TxIndex          uint32
	BlockTimestampMs uint64
}

// BlockTxPosition implements pagination.BlockTxPositioned.
func (r MidnightRegistration) BlockTxPosition() (blockNumber uint64, txIndex uint32) {
	return r.BlockNumber, r.TxIndex
}

// MidnightDeregistration stores mapping validator deregistration events.
type MidnightDeregistration struct {
	ID               uint
	FullDatum        []byte
	TxHash           []byte
	UtxoTxHash       []byte
	UtxoIndex        uint32
	BlockNumber      uint64
	BlockHash        []byte
	TxIndex          uint32
	BlockTimestampMs uint64
}

// BlockTxPosition implements pagination.BlockTxPositioned.
func (r MidnightDeregistration) BlockTxPosition() (blockNumber uint64, txIndex uint32) {
	return r.BlockNumber, r.TxIndex
}

// MidnightGovernanceDatum stores latest Technical Committee and Council datums.
type MidnightGovernanceDatum struct {
	ID          uint
	DatumType   string
	TxHash      []byte
	OutputIndex uint32
	Datum       []byte
	BlockNumber uint64
}

// MidnightAriadneParams stores Ariadne parameters per epoch when changed.
type MidnightAriadneParams struct {
	ID    uint
	Epoch uint64
	Datum []byte
}

// MidnightAriadneRollback stores the previous Ariadne row for a block upsert,
// so a later rollback can restore state even after process restart.
type MidnightAriadneRollback struct {
	ID             uint
	BlockNumber    uint64
	Epoch          uint64
	PreviousExists bool
	PreviousDatum  []byte
}

// MidnightEpochCandidates stores candidate snapshots at epoch boundaries.
type MidnightEpochCandidates struct {
	ID             uint
	Epoch          uint64
	BlockNumber    uint64
	CandidatesCbor []byte
}

// MidnightCommitteeCandidateRegistration stores full on-chain provenance for
// a committee-candidate UTxO the first time it is observed as a transaction
// output: which block/slot/transaction created it and which UTxOs its
// creating transaction consumed. MidnightEpochCandidates.CandidatesCbor
// records only (tx_hash, output_index, datum) membership at each epoch
// boundary — this table is the durable side-store GetEpochCandidates joins
// against to fill in tx_inputs/slot_number/tx_index/block_number, since the
// in-memory candidate set is rebuilt on restart from the generic UTXO index
// (GetMidnightCandidates), which carries only tx_hash/output_index/datum.
type MidnightCommitteeCandidateRegistration struct {
	ID           uint
	TxHash       []byte
	OutputIndex  uint32
	BlockNumber  uint64
	SlotNumber   uint64
	TxIndex      uint32
	TxInputsCbor []byte
}
