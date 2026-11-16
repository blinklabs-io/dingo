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

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// GenesisStakeAccounts returns the accounts Shelley genesis staking imports
// for stakeDelegations, a map of stake key hash to pool key hash, both hex,
// ordered by stake key hash. Each is active from slot 0 with keyDeposit as
// its import deposit. The metadata store and the live UTxO set both derive
// their genesis accounts from this, so they cannot differ.
func GenesisStakeAccounts(
	stakeDelegations map[string]string,
	keyDeposit uint64,
) ([]Account, error) {
	importDeposit := types.Uint64(keyDeposit)
	accounts := make([]Account, 0, len(stakeDelegations))
	for stakerHex, poolHex := range stakeDelegations {
		staker, err := hex.DecodeString(stakerHex)
		if err != nil {
			return nil, fmt.Errorf("decode staker hash %s: %w", stakerHex, err)
		}
		pool, err := hex.DecodeString(poolHex)
		if err != nil {
			return nil, fmt.Errorf("decode pool hash %s: %w", poolHex, err)
		}
		accounts = append(accounts, Account{
			StakingKey:    staker,
			CredentialTag: 0,
			Pool:          pool,
			Active:        true,
			AddedSlot:     0,
			CreatedSlot:   0,
			ImportDeposit: &importDeposit,
		})
	}
	slices.SortFunc(accounts, func(a, b Account) int {
		return bytes.Compare(a.StakingKey, b.StakingKey)
	})
	return accounts, nil
}

// GenesisDelegatee is the delegation a Conway genesis delegation states for
// its stake credential. Drep is nil for a predefined DRep and for a
// delegation that names none, and DrepType is then the predefined type or
// zero.
type GenesisDelegatee struct {
	Pool     []byte
	Drep     []byte
	DrepType uint64
}

// GenesisDelegateeOf returns the delegation a Conway genesis delegatee
// states.
func GenesisDelegateeOf(
	d conway.ConwayGenesisDelegatee,
) (GenesisDelegatee, error) {
	drepType, drepErr := DrepTypeFromInt(d.DRep.Type)
	if drepErr != nil && d.Type != conway.ConwayGenesisDelegateeTypeStake {
		return GenesisDelegatee{}, fmt.Errorf(
			"genesis delegatee drep type: %w", drepErr,
		)
	}
	out := GenesisDelegatee{DrepType: drepType}
	if d.Type != conway.ConwayGenesisDelegateeTypeStake &&
		drepType != DrepTypeAlwaysAbstain &&
		drepType != DrepTypeAlwaysNoConfidence {
		out.Drep = d.DRep.Credential
	}
	switch d.Type {
	case conway.ConwayGenesisDelegateeTypeStake,
		conway.ConwayGenesisDelegateeTypeStakeVote:
		out.Pool = bytes.Clone(d.PoolId[:])
	case conway.ConwayGenesisDelegateeTypeVote:
	default:
		return GenesisDelegatee{}, fmt.Errorf(
			"unknown genesis delegatee type: %d", d.Type,
		)
	}
	if len(out.Drep) > 0 && out.DrepType > DrepTypeScriptHash {
		return GenesisDelegatee{}, fmt.Errorf(
			"genesis DRep credential has non-credential type: %d",
			out.DrepType,
		)
	}
	return out, nil
}

// GenesisDRep returns the DRep and registration a Conway genesis initial DRep
// is imported as: registered and active from slot 0 with the deposit the
// genesis states.
func GenesisDRep(
	credential *lcommon.Credential,
	state conway.ConwayGenesisDRepState,
) (Drep, RegistrationDrep, error) {
	tag, err := CredentialTagFromUint(credential.CredType)
	if err != nil {
		return Drep{}, RegistrationDrep{}, fmt.Errorf(
			"genesis drep credential type: %w", err,
		)
	}
	drep := Drep{
		CredentialTag: tag,
		Credential:    bytes.Clone(credential.Credential[:]),
		AddedSlot:     0,
		ExpiryEpoch:   state.Expiry,
		Active:        true,
	}
	registration := RegistrationDrep{
		CredentialTag:  tag,
		DrepCredential: bytes.Clone(credential.Credential[:]),
		AddedSlot:      0,
		DepositAmount:  types.Uint64(state.Deposit),
	}
	if state.Anchor != nil {
		drep.AnchorURL = state.Anchor.Url
		drep.AnchorHash = bytes.Clone(state.Anchor.DataHash[:])
		registration.AnchorURL = state.Anchor.Url
		registration.AnchorHash = bytes.Clone(state.Anchor.DataHash[:])
	}
	return drep, registration, nil
}
