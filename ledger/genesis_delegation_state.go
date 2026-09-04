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

package ledger

import (
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/gouroboros/ledger/common"
)

var _ common.GenesisDelegationState = (*LedgerState)(nil)

var errGenesisDelegationStateUnavailable = errors.New(
	"genesis delegation state unavailable",
)

// GenesisDelegateKeyHashes implements common.GenesisDelegationState. Genesis
// delegation certificates are stored per genesis key, so the initial Shelley
// delegation set is used as the key set and the latest certificate before the
// applied tip supplies each key's current delegate.
func (ls *LedgerState) GenesisDelegateKeyHashes() ([]common.Blake2b224, error) {
	if ls == nil || ls.db == nil || ls.config.CardanoNodeConfig == nil {
		return nil, errGenesisDelegationStateUnavailable
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return nil, errGenesisDelegationStateUnavailable
	}
	initial, err := parseShelleyGenesisDelegations(shelleyGenesis)
	if err != nil {
		return nil, fmt.Errorf("parse genesis delegations: %w", err)
	}
	// The applied tip includes certificates in its block. The existing
	// certificate query is exclusive, so advance by one slot to include them.
	lookupSlot := ls.ChainTipSlot()
	if lookupSlot < math.MaxUint64 {
		lookupSlot++
	}
	delegates := make([]common.Blake2b224, 0, len(initial))
	for _, delegation := range initial {
		row, err := ls.db.Metadata().GetGenesisDelegationForSlot(
			delegation.genesisHash,
			lookupSlot,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"lookup genesis delegation for %x: %w",
				delegation.genesisHash,
				err,
			)
		}
		delegateHash := delegation.delegateHash
		if row != nil {
			if len(row.GenesisDelegateHash) != common.Blake2b224Size {
				return nil, fmt.Errorf(
					"invalid genesis delegate hash length %d for %x",
					len(row.GenesisDelegateHash),
					delegation.genesisHash,
				)
			}
			delegateHash = row.GenesisDelegateHash
		}
		delegates = append(delegates, common.NewBlake2b224(delegateHash))
	}
	return delegates, nil
}

// GenesisUpdateQuorum implements common.GenesisDelegationState using the
// Shelley genesis configuration, which is immutable for a network.
func (ls *LedgerState) GenesisUpdateQuorum() (uint, error) {
	if ls == nil || ls.config.CardanoNodeConfig == nil {
		return 0, errGenesisDelegationStateUnavailable
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil || shelleyGenesis.UpdateQuorum < 0 {
		return 0, errGenesisDelegationStateUnavailable
	}
	return uint(shelleyGenesis.UpdateQuorum), nil
}
