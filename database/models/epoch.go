// Copyright 2025 Blink Labs Software
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

type Epoch struct {
	Nonce         []byte
	EvolvingNonce []byte
	// CandidateNonce holds the frozen candidate nonce from the end
	// of the previous epoch (psCandidateNonce in Haskell). In
	// Ouroboros Praos, the candidate nonce tracks the evolving
	// nonce until the randomness stabilisation window cutoff
	// (4k/f slots before the end of the epoch), then freezes.
	// When 4k/f >= epochLength (e.g., devnets with short epochs),
	// the candidate nonce is never updated and stays at its
	// initial value (genesis hash). This field carries the
	// candidate nonce across epochs so it can seed the next
	// epoch's computation correctly.
	CandidateNonce []byte
	// LastEpochBlockNonce holds the prevHash of the last applied
	// block from the PREVIOUS epoch transition (praosStateLabNonce
	// in Haskell). In Ouroboros Praos, the epoch nonce formula uses
	// a lagged lab nonce: at the N→N+1 transition, the nonce saved
	// at N-1→N is used. This field is nil for epoch 0 (equivalent
	// to NeutralNonce / identity).
	LastEpochBlockNonce []byte
	ID                  uint `gorm:"primarykey"`
	// NOTE: we would normally use this as the primary key, but GORM doesn't
	// like a primary key value of 0
	EpochId       uint64 `gorm:"uniqueIndex"`
	StartSlot     uint64
	EraId         uint
	SlotLength    uint
	LengthInSlots uint
}

func (Epoch) TableName() string {
	return "epoch"
}
