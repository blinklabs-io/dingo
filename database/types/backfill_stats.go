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

package types

import "time"

// BackfillHotPathStats captures low-overhead interval counters for API-mode
// Mithril metadata backfill. Callers aggregate these per progress interval so
// dense and sparse historical ranges can be compared without pprof.
type BackfillHotPathStats struct {
	Blocks    uint64
	Txs       uint64
	Utxos     uint64
	InputRefs uint64

	BlobTxOffsetWrites   uint64
	BlobUtxoOffsetWrites uint64
	SkippedUtxoOffsets   uint64
	SkippedInputRecovery uint64

	AddressTxs      uint64
	Witnesses       uint64
	Scripts         uint64
	WitnessScripts  uint64
	PlutusData      uint64
	Redeemers       uint64
	UtxoSpends      uint64
	CollateralRets  uint64
	DeleteTxIDs     uint64
	Certificates    uint64
	MetadataLabels  uint64
	PParamUpdates   uint64
	RecoveredInputs uint64

	BlockReadDecode       time.Duration
	OffsetComputation     time.Duration
	BlobOffsetWrites      time.Duration
	SetTransactionBatched time.Duration
	ConsumedInputRecovery time.Duration
	UtxoAddressLookup     time.Duration
	AddressIndex          time.Duration
	FlushBatch            time.Duration
	CheckpointWrites      time.Duration
}

// Add folds another stats snapshot into s.
func (s *BackfillHotPathStats) Add(other BackfillHotPathStats) {
	s.Blocks += other.Blocks
	s.Txs += other.Txs
	s.Utxos += other.Utxos
	s.InputRefs += other.InputRefs
	s.BlobTxOffsetWrites += other.BlobTxOffsetWrites
	s.BlobUtxoOffsetWrites += other.BlobUtxoOffsetWrites
	s.SkippedUtxoOffsets += other.SkippedUtxoOffsets
	s.SkippedInputRecovery += other.SkippedInputRecovery
	s.AddressTxs += other.AddressTxs
	s.Witnesses += other.Witnesses
	s.Scripts += other.Scripts
	s.WitnessScripts += other.WitnessScripts
	s.PlutusData += other.PlutusData
	s.Redeemers += other.Redeemers
	s.UtxoSpends += other.UtxoSpends
	s.CollateralRets += other.CollateralRets
	s.DeleteTxIDs += other.DeleteTxIDs
	s.Certificates += other.Certificates
	s.MetadataLabels += other.MetadataLabels
	s.PParamUpdates += other.PParamUpdates
	s.RecoveredInputs += other.RecoveredInputs
	s.BlockReadDecode += other.BlockReadDecode
	s.OffsetComputation += other.OffsetComputation
	s.BlobOffsetWrites += other.BlobOffsetWrites
	s.SetTransactionBatched += other.SetTransactionBatched
	s.ConsumedInputRecovery += other.ConsumedInputRecovery
	s.UtxoAddressLookup += other.UtxoAddressLookup
	s.AddressIndex += other.AddressIndex
	s.FlushBatch += other.FlushBatch
	s.CheckpointWrites += other.CheckpointWrites
}

// Reset clears the interval counters while preserving the allocation.
func (s *BackfillHotPathStats) Reset() {
	*s = BackfillHotPathStats{}
}
