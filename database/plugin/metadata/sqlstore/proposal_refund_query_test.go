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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestGetGovernanceProposalsRefundedAt(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	u64 := func(v uint64) *uint64 { return &v }
	seed := func(txByte byte, mutate func(*models.GovernanceProposal)) {
		p := &models.GovernanceProposal{
			TxHash:        credentialHash(txByte),
			ProposedEpoch: 1,
			ExpiresEpoch:  10,
			Deposit:       100,
			AddedSlot:     1,
		}
		mutate(p)
		require.NoError(t, store.SetGovernanceProposal(p, nil))
	}
	// Dropped at 678.
	seed(0x01, func(p *models.GovernanceProposal) {
		p.ExpiredEpoch, p.ExpiredSlot = u64(677), u64(6770)
		p.DroppedEpoch, p.DroppedSlot = u64(678), u64(6780)
	})
	// Enacted at 678.
	seed(0x02, func(p *models.GovernanceProposal) {
		p.RatifiedEpoch, p.RatifiedSlot = u64(677), u64(6770)
		p.EnactedEpoch, p.EnactedSlot = u64(678), u64(6780)
	})
	// Expired at 678 but not yet dropped: no deposit has moved.
	seed(0x03, func(p *models.GovernanceProposal) {
		p.ExpiredEpoch, p.ExpiredSlot = u64(678), u64(6780)
	})
	// Dropped at a neighbouring epoch.
	seed(0x04, func(p *models.GovernanceProposal) {
		p.DroppedEpoch, p.DroppedSlot = u64(679), u64(6790)
	})
	// Dropped at 678 and then rolled back.
	seed(0x05, func(p *models.GovernanceProposal) {
		p.DroppedEpoch, p.DroppedSlot = u64(678), u64(6780)
		p.DeletedSlot = u64(6781)
	})

	got, err := store.GetGovernanceProposalsRefundedAt(678, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)
	byHash := map[string]*models.GovernanceProposal{}
	for _, p := range got {
		byHash[string(p.TxHash)] = p
	}
	dropped := byHash[string(credentialHash(0x01))]
	require.NotNil(t, dropped)
	require.NotNil(t, dropped.DroppedSlot)
	require.Equal(t, uint64(6780), *dropped.DroppedSlot)
	enacted := byHash[string(credentialHash(0x02))]
	require.NotNil(t, enacted)
	require.NotNil(t, enacted.EnactedSlot)
	require.Equal(t, uint64(6780), *enacted.EnactedSlot)
}

func TestGetPostSnapshotRewardCreditEventsKeepsEachSource(t *testing.T) {
	t.Parallel()
	store, raw := newManagementTestStoreWithRawDB(t)

	const slot = 6780
	key := credentialHash(0x11)
	insert := func(
		source byte,
		amount string,
		addedSlot int,
		withdrawal, postSnapshot bool,
	) {
		_, err := raw.Exec(`
INSERT INTO account_reward_delta (
    staking_key, credential_tag, tx_hash, amount, previous_reward,
    added_slot, withdrawal, post_snapshot
) VALUES (?, 0, ?, ?, NULL, ?, ?, ?)`,
			key, credentialHash(source), amount, addedSlot, withdrawal,
			postSnapshot,
		)
		require.NoError(t, err)
	}
	// Two post-snapshot credits to one account at one boundary.
	insert(0xa1, "100", slot, false, true)
	insert(0xa2, "250", slot, false, true)
	// Not post-snapshot, a withdrawal, and another slot: all excluded.
	insert(0xa3, "1", slot, false, false)
	insert(0xa4, "2", slot, true, true)
	insert(0xa5, "3", slot+1, false, true)

	got, err := store.GetPostSnapshotRewardCreditEvents(slot, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)
	bySource := map[string]uint64{}
	for _, c := range got {
		require.Equal(t, key, c.StakingKey)
		require.Equal(t, uint64(slot), c.AddedSlot)
		require.True(t, c.PostSnapshot)
		bySource[string(c.TxHash)] = uint64(c.Amount)
	}
	require.Equal(t, uint64(100), bySource[string(credentialHash(0xa1))])
	require.Equal(t, uint64(250), bySource[string(credentialHash(0xa2))])
}
