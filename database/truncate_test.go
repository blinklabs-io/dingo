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

package database

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestTruncateAfterSlotRestoresPoolDenormalizedFields verifies that
// truncating past a pool re-registration reverts the pool's denormalized
// pledge/cost/VRF/reward-account fields to the surviving prior
// registration's values, rather than leaving the discarded
// registration's values in place. RestorePoolStateAtSlot detects which
// pools need reverting by querying PoolRegistration rows with
// added_slot > the target slot -- exactly the rows DeleteCertificatesAfterSlot
// removes, so this only passes if pool state is restored before
// certificates are deleted, not after.
func TestTruncateAfterSlotRestoresPoolDenormalizedFields(t *testing.T) {
	db := newTestDB(t)

	targetBlock := testIndexedBlock(1500, 1, 0x15)
	require.NoError(t, db.BlockCreate(targetBlock, nil))

	poolKeyHash := bytes.Repeat([]byte{0x99}, 28)
	vrfBefore := bytes.Repeat([]byte{0xa1}, 32)
	rewardBefore := bytes.Repeat([]byte{0xb1}, 28)
	vrfAfter := bytes.Repeat([]byte{0xa2}, 32)
	rewardAfter := bytes.Repeat([]byte{0xb2}, 28)

	// Initial registration, before the truncate target.
	require.NoError(t, db.ImportPool(nil,
		&models.Pool{
			PoolKeyHash:   poolKeyHash,
			Pledge:        100,
			Cost:          200,
			VrfKeyHash:    vrfBefore,
			RewardAccount: rewardBefore,
		},
		&models.PoolRegistration{
			PoolKeyHash:   poolKeyHash,
			AddedSlot:     1000,
			Pledge:        100,
			Cost:          200,
			VrfKeyHash:    vrfBefore,
			RewardAccount: rewardBefore,
		},
	))

	// Re-registration with different terms, after the truncate target.
	// This is the one truncate must discard, restoring the pool to its
	// pre-re-registration state.
	require.NoError(t, db.ImportPool(nil,
		&models.Pool{
			PoolKeyHash:   poolKeyHash,
			Pledge:        999,
			Cost:          888,
			VrfKeyHash:    vrfAfter,
			RewardAccount: rewardAfter,
		},
		&models.PoolRegistration{
			PoolKeyHash:   poolKeyHash,
			AddedSlot:     2000,
			Pledge:        999,
			Cost:          888,
			VrfKeyHash:    vrfAfter,
			RewardAccount: rewardAfter,
		},
	))

	// Sanity check: the pool currently reflects the later registration.
	poolBefore, err := db.GetPool(lcommon.PoolKeyHash(poolKeyHash), true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(999), uint64(poolBefore.Pledge))

	point := ocommon.Point{Slot: 1500, Hash: targetBlock.Hash}
	_, _, err = db.TruncateAfterSlot(point, 0, nil)
	require.NoError(t, err)

	poolAfter, err := db.GetPool(lcommon.PoolKeyHash(poolKeyHash), true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), uint64(poolAfter.Pledge),
		"pledge must revert to the surviving pre-truncate registration")
	require.Equal(t, uint64(200), uint64(poolAfter.Cost),
		"cost must revert to the surviving pre-truncate registration")
	require.Equal(t, vrfBefore, poolAfter.VrfKeyHash,
		"vrf_key_hash must revert to the surviving pre-truncate registration")
	require.Equal(t, rewardBefore, poolAfter.RewardAccount,
		"reward_account must revert to the surviving pre-truncate registration")
}
