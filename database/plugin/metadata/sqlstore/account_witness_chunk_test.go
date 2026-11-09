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
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// AccountLastWitnessSlots reads reward_withdrawal witnesses for every
// requested credential, including those past the first
// rewardWithdrawalWitnessKeys of a request.
func TestAccountLastWitnessSlotsReadsEveryWithdrawal(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	const accounts = 2*rewardWithdrawalWitnessKeys + 50
	refs := make([]models.StakeCredentialRef, 0, accounts)
	for i := range accounts {
		key := bytesRepeat(0x70, 28)
		binary.BigEndian.PutUint32(key[24:], uint32(i))
		hash := bytesRepeat(0x71, 32)
		binary.BigEndian.PutUint32(hash[28:], uint32(i))
		rollbackAccount(t, store, key, 5)
		seedWithdrawingTransaction(t, store, hash)
		require.NoError(t, store.ApplyAccountRewardWithdrawal(
			0, key, 5, uint64(100+i), hash, nil))
		refs = append(refs, models.NewStakeCredentialRef(0, key))
	}
	slots, err := store.AccountLastWitnessSlots(refs, 10_000, nil)
	require.NoError(t, err)
	require.Len(t, slots, accounts)
	for i, ref := range refs {
		require.Equal(t, uint64(100+i), slots[ref.MapKey()])
	}
}
