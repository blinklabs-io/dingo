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

package sqlstore

import (
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

func (s *Store) GetAccountsByCredentialAtSlot(
	refs []models.StakeCredentialRef,
	slot uint64,
	txn types.Txn,
) (map[string]*models.Account, error) {
	ret := make(map[string]*models.Account, len(refs))
	if len(refs) == 0 {
		return ret, nil
	}
	live, err := s.GetAccountsByCredential(refs, true, txn)
	if err != nil {
		return nil, err
	}
	db, ctx, err := s.readDBFromTxn(txn)
	if err != nil {
		return nil, err
	}
	selected := make(map[historicalRewardKey]struct{}, len(live))
	for key, account := range live {
		state := *account
		// Every write to active, pool or drep stamps added_slot, including
		// the ones no certificate records (POOLREAP and the PV10 dangling
		// DRep clear), so a row untouched since slot is exact there and the
		// certificate derivation, which cannot see those two, is not needed.
		if account.AddedSlot > slot {
			derived, err := deriveAccountStateAtSlot(
				ctx,
				db,
				account.CredentialTag,
				account.StakingKey,
				account.CreatedSlot,
				slot,
			)
			if err != nil {
				return nil, err
			}
			if derived.absent {
				continue
			}
			state.Active = derived.active
			if derived.setPool {
				state.Pool = derived.pool
			}
			if derived.setDrep {
				state.Drep = derived.drep
				state.DrepType = derived.drepType
			}
			state.AddedSlot = derived.latestSlot
		}
		if !state.Active {
			continue
		}
		ret[key] = &state
		selected[historicalRewardKey{
			tag: state.CredentialTag,
			key: string(state.StakingKey),
		}] = struct{}{}
	}
	rewards, err := s.historicalRewards(ctx, db, slot, selected)
	if err != nil {
		return nil, err
	}
	for _, account := range ret {
		account.Reward = types.Uint64(rewards[historicalRewardKey{
			tag: account.CredentialTag,
			key: string(account.StakingKey),
		}])
	}
	return ret, nil
}
