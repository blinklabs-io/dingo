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

// StakeDelegationEventRow is a pool delegation certificate of a stake
// credential, applied at AddedSlot. EffectiveSlot is the slot a Leios
// closure's certificate takes effect at when it differs from AddedSlot, and
// zero otherwise.
type StakeDelegationEventRow struct {
	CredentialTag uint8
	StakingKey    []byte
	PoolKeyHash   []byte
	AddedSlot     uint64
	EffectiveSlot uint64
	BlockIndex    uint32
	CertIndex     uint32
}
