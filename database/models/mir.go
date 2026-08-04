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

import "github.com/blinklabs-io/dingo/database/types"

type MoveInstantaneousRewards struct {
	Rewards       []MoveInstantaneousRewardsReward
	Pot           uint
	CertificateID uint
	ID            uint
	AddedSlot     uint64
	// OtherPot holds the lovelace amount for a pot-to-pot transfer (non-zero
	// only when the cert moves coins between treasury and reserves rather than
	// distributing to staking credentials).
	OtherPot types.Uint64
}

// MIREffect is the processed form of a single MIR certificate used by the
// epoch-boundary application logic. One of OtherPot > 0 (pot-to-pot transfer)
// or len(Rewards) > 0 (credential distribution) will be non-empty.
type MIREffect struct {
	// ID is the move_instantaneous_rewards row ID. It is used as the stable
	// per-MIR reward-credit discriminator when applying epoch-boundary effects.
	ID uint
	// Pot is the source Ada pot: 0 = Reserves, 1 = Treasury.
	Pot uint
	// OtherPot is the amount for a pot-to-pot transfer (0 when distributing).
	OtherPot uint64
	// Rewards lists credential→amount pairs for a distribution MIR.
	Rewards []MIRReward
}

// MIRReward is a single credential→amount entry from a distribution MIR cert.
type MIRReward struct {
	Credential    []byte
	CredentialTag uint8
	Amount        uint64
}

type MoveInstantaneousRewardsReward struct {
	Credential    []byte
	CredentialTag uint8
	Amount        types.Uint64
	ID            uint
	MIRID         uint
}
