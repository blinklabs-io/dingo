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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestGetActivePoolKeyHashesPageOrdersAndBoundsInStorage(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 1)
	for _, seed := range []byte{0x30, 0x10, 0x20} {
		seedPoolWithSingleRegistration(
			t, store, bytes.Repeat([]byte{seed}, 28), 100,
		)
	}
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(200, make([]byte, 32)),
	}, nil))

	page, total, err := store.GetActivePoolKeyHashesPage(1, 1, false, nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Equal(t, [][]byte{bytes.Repeat([]byte{0x20}, 28)}, page)

	page, total, err = store.GetActivePoolKeyHashesPage(2, 0, true, nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Equal(t, [][]byte{
		bytes.Repeat([]byte{0x30}, 28),
		bytes.Repeat([]byte{0x20}, 28),
	}, page)

	page, total, err = store.GetActivePoolKeyHashesPage(10, 3, false, nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Empty(t, page)
}

func TestGetRetiringPoolsPageOrdersAndBoundsInStorage(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	for i, seed := range []byte{0x30, 0x10, 0x20} {
		hash := bytes.Repeat([]byte{seed}, 28)
		seedPoolWithSingleRegistration(t, store, hash, 100)
		var poolID int64
		require.NoError(t, store.readDB.QueryRow(
			"SELECT id FROM pool WHERE pool_key_hash = ?", hash,
		).Scan(&poolID))
		_, err := store.writeDB.Exec(`
INSERT INTO pool_retirement (
    pool_key_hash, certificate_id, pool_id, epoch, added_slot
) VALUES (?, 0, ?, ?, ?)`, hash, poolID, i+5, i+200)
		require.NoError(t, err)
	}

	page, total, err := store.GetRetiringPoolsPage(0, 1, 1, false, nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Equal(t, uint64(6), page[0].Epoch)

	page, total, err = store.GetRetiringPoolsPage(0, 2, 0, true, nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Equal(t, []uint64{7, 6}, []uint64{page[0].Epoch, page[1].Epoch})

	page, total, err = store.GetRetiringPoolsPage(0, 10, 3, false, nil)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Empty(t, page)
}

func TestGetDrepsPageFiltersAndBoundsInStorage(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	for epoch, start := range []int{0, 100, 200} {
		_, err := store.writeDB.Exec(`
INSERT INTO epoch (epoch_id, start_slot, length_in_slots)
VALUES (?, ?, 100)`, epoch, start)
		require.NoError(t, err)
	}
	for i, active := range []bool{true, false, true} {
		_, err := store.writeDB.Exec(`
INSERT INTO drep (
    credential, added_slot, last_activity_epoch, expiry_epoch, active
) VALUES (?, ?, 0, ?, ?)`, bytes.Repeat([]byte{byte(i + 1)}, 28),
			i*100+50, i+1, active)
		require.NoError(t, err)
	}
	retired := false
	page, total, err := store.GetDrepsPage(models.DrepListPageQuery{
		CurrentEpoch: 10,
		Retired:      &retired,
		Descending:   true,
		Limit:        1,
		Offset:       1,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, page, 1)
	require.Equal(t, bytes.Repeat([]byte{0x01}, 28), page[0].Credential)

	page, total, err = store.GetDrepsPage(models.DrepListPageQuery{
		CurrentEpoch: 10,
		Retired:      &retired,
		Limit:        10,
		Offset:       2,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Empty(t, page)
}

func TestGetDrepsPageUsesLastActivityForDerivedExpiry(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	_, err := store.writeDB.Exec(`
INSERT INTO epoch (epoch_id, start_slot, length_in_slots)
VALUES (1, 100, 100), (9, 900, 100)`)
	require.NoError(t, err)
	credential := bytes.Repeat([]byte{0x44}, 28)
	_, err = store.writeDB.Exec(`
INSERT INTO drep (
    credential, added_slot, last_activity_epoch, expiry_epoch, active
) VALUES (?, 100, 9, 0, TRUE)`, credential)
	require.NoError(t, err)
	expired := false
	page, total, err := store.GetDrepsPage(models.DrepListPageQuery{
		CurrentEpoch:     10,
		InactivityPeriod: 2,
		InactivityKnown:  true,
		Expired:          &expired,
		Limit:            10,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, page, 1)
	require.Equal(t, credential, page[0].Credential)
}

func TestGetDrepsPageKeepsPredefinedDrepsUnexpired(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	_, err := store.writeDB.Exec(
		"INSERT INTO vote_delegation (drep_type, added_slot) VALUES (?, 1)",
		models.DrepTypeAlwaysAbstain,
	)
	require.NoError(t, err)

	expired := false
	page, total, err := store.GetDrepsPage(models.DrepListPageQuery{
		CurrentEpoch:     100,
		InactivityPeriod: 20,
		InactivityKnown:  true,
		Expired:          &expired,
		Limit:            10,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, page, 1)
	require.NotNil(t, page[0].Predefined)
	require.Equal(
		t,
		uint64(models.DrepTypeAlwaysAbstain),
		*page[0].Predefined,
	)
}

func TestGetDrepsPageOrdersByVotingPowerBeforePaging(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	low := bytes.Repeat([]byte{0x11}, 28)
	high := bytes.Repeat([]byte{0x22}, 28)
	for i, credential := range [][]byte{low, high} {
		_, err := store.writeDB.Exec(`
INSERT INTO drep (credential, added_slot, active) VALUES (?, ?, TRUE)`,
			credential, i+1)
		require.NoError(t, err)
	}
	_, err := store.writeDB.Exec(`
INSERT INTO account (
    staking_key, credential_tag, drep, drep_type, reward, active
) VALUES (?, 0, ?, 0, '10', TRUE), (?, 0, ?, 0, '20', TRUE)`,
		bytes.Repeat([]byte{0xa1}, 28), low,
		bytes.Repeat([]byte{0xa2}, 28), high)
	require.NoError(t, err)
	page, total, err := store.GetDrepsPage(models.DrepListPageQuery{
		OrderByAmount: true,
		Descending:    true,
		PowerAdjustments: []models.DrepPowerAdjustment{{
			Credential: low,
			Amount:     15,
		}},
		Limit:  1,
		Offset: 0,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, page, 1)
	require.Equal(t, low, page[0].Credential)
	require.Equal(t, uint64(25), page[0].Amount)
}

func TestGetDrepsPageAmountUsesMaintainedLiveStake(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	low := bytes.Repeat([]byte{0x31}, 28)
	high := bytes.Repeat([]byte{0x32}, 28)
	lowStake := bytes.Repeat([]byte{0xb1}, 28)
	highStake := bytes.Repeat([]byte{0xb2}, 28)
	for i, credential := range [][]byte{low, high} {
		_, err := store.writeDB.Exec(`
INSERT INTO drep (credential, added_slot, active) VALUES (?, ?, TRUE)`,
			credential, i+1)
		require.NoError(t, err)
	}
	_, err := store.writeDB.Exec(`
INSERT INTO account (
    staking_key, credential_tag, drep, drep_type, reward, active
) VALUES (?, 0, ?, 0, '0', TRUE), (?, 0, ?, 0, '20', TRUE)`,
		lowStake, low, highStake, high)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`
INSERT INTO utxo (
    tx_id, staking_key, credential_tag, deleted_slot, amount, output_idx
) VALUES (?, ?, 0, 0, '1', 0)`, bytes.Repeat([]byte{0xc1}, 32), lowStake)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`
INSERT INTO reward_live_stake (
    staking_key, credential_tag, utxo_stake, reward_stake, total_stake,
    registered, updated_slot, calculation_version
) VALUES (?, 0, '30', '0', '30', TRUE, 1, ?)`,
		lowStake, models.RewardStakeCalculationVersion)
	require.NoError(t, err)

	page, total, err := store.GetDrepsPage(models.DrepListPageQuery{
		OrderByAmount: true,
		Descending:    true,
		Limit:         1,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, page, 1)
	require.Equal(t, low, page[0].Credential)
	require.Equal(t, uint64(30), page[0].Amount)
}

func TestGetDrepsPageAmountExcludesUndelegatedAccounts(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	credential := bytes.Repeat([]byte{0x41}, 28)
	_, err := store.writeDB.Exec(`
INSERT INTO drep (credential, added_slot, active) VALUES (?, 1, TRUE)`,
		credential)
	require.NoError(t, err)
	for i := range 2 {
		_, err = store.writeDB.Exec(`
INSERT INTO account (
    staking_key, credential_tag, drep, drep_type, reward, active
) VALUES (?, 0, NULL, 0, '9223372036854775807', TRUE)`,
			bytes.Repeat([]byte{byte(0xd1 + i)}, 28))
		require.NoError(t, err)
	}

	page, total, err := store.GetDrepsPage(models.DrepListPageQuery{
		OrderByAmount: true,
		Descending:    true,
		Limit:         1,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, page, 1)
	require.Equal(t, credential, page[0].Credential)
	require.Zero(t, page[0].Amount)
}
