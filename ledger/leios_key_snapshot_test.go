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
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestLedgerViewGetLeiosKeysUsesRequestedSnapshotAfterPoolRotation(
	t *testing.T,
) {
	db := newTestDB(t)
	poolKeyHash := bytes.Repeat([]byte{0x41}, 28)
	oldPublic := bytes.Repeat([]byte{0x51}, 96)
	oldProof := bytes.Repeat([]byte{0x61}, 48)
	newPublic := bytes.Repeat([]byte{0x52}, 96)
	newProof := bytes.Repeat([]byte{0x62}, 48)

	importPool := func(slot uint64, public, proof []byte) {
		t.Helper()
		record := &models.Pool{
			PoolKeyHash:             append([]byte(nil), poolKeyHash...),
			VrfKeyHash:              bytes.Repeat([]byte{0x71}, 32),
			LeiosKeyPublic:          append([]byte(nil), public...),
			LeiosKeyPossessionProof: append([]byte(nil), proof...),
		}
		registration := &models.PoolRegistration{
			PoolKeyHash:             append([]byte(nil), poolKeyHash...),
			VrfKeyHash:              bytes.Repeat([]byte{0x71}, 32),
			AddedSlot:               slot,
			Pledge:                  dbtypes.Uint64(1),
			Cost:                    dbtypes.Uint64(1),
			LeiosKeyPublic:          append([]byte(nil), public...),
			LeiosKeyPossessionProof: append([]byte(nil), proof...),
		}
		require.NoError(t, db.Metadata().ImportPool(record, registration, nil))
	}

	importPool(50, oldPublic, oldProof)
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:                         8,
			SnapshotType:                  models.PoolStakeSnapshotTypeMark,
			PoolKeyHash:                   append([]byte(nil), poolKeyHash...),
			TotalStake:                    dbtypes.Uint64(100),
			CapturedSlot:                  199,
			LeiosKeyPublic:                append([]byte(nil), oldPublic...),
			LeiosKeyPossessionProof:       append([]byte(nil), oldProof...),
			CalculationVersion:            1,
			RewardAccountAutoVote:         models.PoolRewardAccountAutoVoteNone,
			RewardAccountAutoVoteResolved: true,
		},
		nil,
	))

	// Rotate the live registration only after Mark[8] has frozen the old key.
	importPool(250, newPublic, newProof)
	var poolHash lcommon.PoolKeyHash
	copy(poolHash[:], poolKeyHash)
	current, err := db.Metadata().GetPools([]lcommon.PoolKeyHash{poolHash}, nil)
	require.NoError(t, err)
	require.Len(t, current, 1)
	require.Equal(t, newPublic, current[0].LeiosKeyPublic)

	txn := db.Transaction(false)
	defer txn.Release()
	view := (&LedgerState{db: db}).NewView(txn)
	keys, err := view.GetLeiosKeys(8, []lcommon.PoolKeyHash{poolHash})
	require.NoError(t, err)
	key := keys[hex.EncodeToString(poolKeyHash)]
	require.NotNil(t, key)
	require.Equal(t, oldPublic, key.PublicKey,
		"committee key must come from requested Mark[8], not live pool state")
	require.Equal(t, oldProof, key.PossessionProof)
}
