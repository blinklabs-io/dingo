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

// LeiosClosureUtxoEffect is a staked output a Leios closure transaction
// created or spent, whose effect slot, the closure's context slot, is before
// the slot it was applied at.
type LeiosClosureUtxoEffect struct {
	TxHash        []byte
	Index         uint32
	Applied       uint64
	Effective     uint64
	Spent         bool
	CredentialTag uint8
	StakingKey    []byte
	Amount        uint64
}
