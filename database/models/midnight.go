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
	ID               uint   `gorm:"primarykey"`
	Address          []byte `gorm:"not null"`
	Quantity         uint64 `gorm:"not null"`
	TxHash           []byte `gorm:"uniqueIndex:idx_midnight_asset_creates_utxo_lookup,priority:1;size:32;not null"`
	OutputIndex      uint32 `gorm:"uniqueIndex:idx_midnight_asset_creates_utxo_lookup,priority:2;not null"`
	BlockNumber      uint64 `gorm:"index:idx_midnight_asset_creates_block_tx,priority:1;not null"`
	BlockHash        []byte `gorm:"size:32;not null"`
	TxIndex          uint32 `gorm:"index:idx_midnight_asset_creates_block_tx,priority:2;not null"`
	BlockTimestampMs uint64 `gorm:"not null"`
}

func (MidnightAssetCreate) TableName() string {
	return "midnight_asset_creates"
}

// MidnightAssetSpend stores cNIGHT UTxO spends for the Midnight indexer.
type MidnightAssetSpend struct {
	ID               uint   `gorm:"primarykey"`
	Address          []byte `gorm:"not null"`
	Quantity         uint64 `gorm:"not null"`
	SpendingTxHash   []byte `gorm:"size:32;not null"`
	UtxoTxHash       []byte `gorm:"uniqueIndex:idx_midnight_asset_spends_utxo_ref,priority:1;size:32;not null"`
	UtxoIndex        uint32 `gorm:"uniqueIndex:idx_midnight_asset_spends_utxo_ref,priority:2;not null"`
	BlockNumber      uint64 `gorm:"index:idx_midnight_asset_spends_block_tx,priority:1;not null"`
	BlockHash        []byte `gorm:"size:32;not null"`
	TxIndex          uint32 `gorm:"index:idx_midnight_asset_spends_block_tx,priority:2;not null"`
	BlockTimestampMs uint64 `gorm:"not null"`
}

func (MidnightAssetSpend) TableName() string {
	return "midnight_asset_spends"
}

// MidnightRegistration stores mapping validator registration events.
type MidnightRegistration struct {
	ID               uint   `gorm:"primarykey"`
	FullDatum        []byte `gorm:"not null"`
	TxHash           []byte `gorm:"uniqueIndex:idx_midnight_registrations_utxo_lookup,priority:1;size:32;not null"`
	OutputIndex      uint32 `gorm:"uniqueIndex:idx_midnight_registrations_utxo_lookup,priority:2;not null"`
	BlockNumber      uint64 `gorm:"index:idx_midnight_registrations_block_tx,priority:1;not null"`
	BlockHash        []byte `gorm:"size:32;not null"`
	TxIndex          uint32 `gorm:"index:idx_midnight_registrations_block_tx,priority:2;not null"`
	BlockTimestampMs uint64 `gorm:"not null"`
}

func (MidnightRegistration) TableName() string {
	return "midnight_registrations"
}

// MidnightDeregistration stores mapping validator deregistration events.
type MidnightDeregistration struct {
	ID               uint   `gorm:"primarykey"`
	FullDatum        []byte `gorm:"not null"`
	TxHash           []byte `gorm:"size:32;not null"`
	UtxoTxHash       []byte `gorm:"uniqueIndex:idx_midnight_deregistrations_utxo_ref,priority:1;size:32;not null"`
	UtxoIndex        uint32 `gorm:"uniqueIndex:idx_midnight_deregistrations_utxo_ref,priority:2;not null"`
	BlockNumber      uint64 `gorm:"index:idx_midnight_deregistrations_block_tx,priority:1;not null"`
	BlockHash        []byte `gorm:"size:32;not null"`
	TxIndex          uint32 `gorm:"index:idx_midnight_deregistrations_block_tx,priority:2;not null"`
	BlockTimestampMs uint64 `gorm:"not null"`
}

func (MidnightDeregistration) TableName() string {
	return "midnight_deregistrations"
}

// MidnightGovernanceDatum stores latest Technical Committee and Council datums.
type MidnightGovernanceDatum struct {
	ID          uint   `gorm:"primarykey"`
	DatumType   string `gorm:"size:32;index:idx_midnight_governance_datums_latest,priority:1;uniqueIndex:idx_midnight_governance_datums_output,priority:1;not null"`
	TxHash      []byte `gorm:"uniqueIndex:idx_midnight_governance_datums_output,priority:2;size:32;not null"`
	OutputIndex uint32 `gorm:"uniqueIndex:idx_midnight_governance_datums_output,priority:3;not null"`
	Datum       []byte `gorm:"not null"`
	BlockNumber uint64 `gorm:"index:idx_midnight_governance_datums_latest,priority:2,sort:desc;not null"`
}

func (MidnightGovernanceDatum) TableName() string {
	return "midnight_governance_datums"
}

// MidnightAriadneParams stores Ariadne parameters per epoch when changed.
type MidnightAriadneParams struct {
	ID    uint   `gorm:"primarykey"`
	Epoch uint64 `gorm:"uniqueIndex;not null"`
	Datum []byte `gorm:"not null"`
}

func (MidnightAriadneParams) TableName() string {
	return "midnight_ariadne_params"
}

// MidnightAriadneRollback stores the previous Ariadne row for a block upsert,
// so a later rollback can restore state even after process restart.
type MidnightAriadneRollback struct {
	ID             uint   `gorm:"primarykey"`
	BlockNumber    uint64 `gorm:"uniqueIndex:idx_midnight_ariadne_rollbacks_block_epoch,priority:1;not null"`
	Epoch          uint64 `gorm:"uniqueIndex:idx_midnight_ariadne_rollbacks_block_epoch,priority:2;not null"`
	PreviousExists bool   `gorm:"not null"`
	PreviousDatum  []byte
}

func (MidnightAriadneRollback) TableName() string {
	return "midnight_ariadne_rollbacks"
}

// MidnightEpochCandidates stores candidate snapshots at epoch boundaries.
type MidnightEpochCandidates struct {
	ID             uint   `gorm:"primarykey"`
	Epoch          uint64 `gorm:"uniqueIndex;not null"`
	BlockNumber    uint64 `gorm:"index;not null;default:0"`
	CandidatesCbor []byte `gorm:"not null"`
}

func (MidnightEpochCandidates) TableName() string {
	return "midnight_epoch_candidates"
}
