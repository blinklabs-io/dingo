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

package sqlite

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/blinklabs-io/dingo/database/models"
)

// normalizedLiveStakeRow is the backend-agnostic shape used to compare
// reward_live_stake rows across sqlite, mysql, and postgres: every field
// except the auto-increment ID, which is expected to differ across
// backends/runs.
type normalizedLiveStakeRow struct {
	CredentialTag            uint8
	StakingKey               string
	PoolKeyHash              string
	UtxoStake                uint64
	RewardStake              uint64
	TotalStake               uint64
	Registered               bool
	PoolDelegationSlot       uint64
	PoolDelegationBlockIndex uint64
	PoolDelegationCertIndex  uint32
	UpdatedSlot              uint64
}

func normalizeLiveStakeRows(
	rows []models.RewardLiveStake,
) []normalizedLiveStakeRow {
	out := make([]normalizedLiveStakeRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, normalizedLiveStakeRow{
			CredentialTag:            row.CredentialTag,
			StakingKey:               string(row.StakingKey),
			PoolKeyHash:              string(row.PoolKeyHash),
			UtxoStake:                uint64(row.UtxoStake),
			RewardStake:              uint64(row.RewardStake),
			TotalStake:               uint64(row.TotalStake),
			Registered:               row.Registered,
			PoolDelegationSlot:       row.PoolDelegationSlot,
			PoolDelegationBlockIndex: row.PoolDelegationBlockIndex,
			PoolDelegationCertIndex:  row.PoolDelegationCertIndex,
			UpdatedSlot:              row.UpdatedSlot,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CredentialTag != out[j].CredentialTag {
			return out[i].CredentialTag < out[j].CredentialTag
		}
		return out[i].StakingKey < out[j].StakingKey
	})
	return out
}

// rewardLiveStakeEquivalenceHashes are the fixed 28-byte test vectors used
// (by identical construction, not by cross-package import) across the
// sqlite/mysql/postgres equivalence tests, so every backend rebuilds from
// byte-identical input.
type rewardLiveStakeEquivalenceHashes struct {
	poolA, poolB           []byte
	stakeA, stakeB, stakeC []byte
	stakeD                 []byte
}

func newRewardLiveStakeEquivalenceHashes() rewardLiveStakeEquivalenceHashes {
	return rewardLiveStakeEquivalenceHashes{
		poolA:  bytes.Repeat([]byte{0xA1}, 28),
		poolB:  bytes.Repeat([]byte{0xB1}, 28),
		stakeA: bytes.Repeat([]byte{0x10}, 28),
		stakeB: bytes.Repeat([]byte{0x20}, 28),
		stakeC: bytes.Repeat([]byte{0x30}, 28),
		stakeD: bytes.Repeat([]byte{0x40}, 28),
	}
}

// seedRewardLiveStakeEquivalenceScenario populates the same
// accounts/UTxOs/delegation-certificate scenario used by every backend's
// cross-backend equivalence test:
//
//   - stakeA (key credential): active, delegated to poolA both via
//     account.Pool and a stake_delegation cert recorded after the account's
//     own AddedSlot (exercises the latest_delegation CTE overriding the
//     account fallback), one live UTxO and one deleted (excluded) UTxO.
//   - stakeB (script credential): active, delegated to poolB with no
//     delegation certificate on record (exercises the account.AddedSlot
//     fallback), and one live UTxO whose amount exceeds 2^53 (exercises the
//     backend-native integer CAST that avoids MySQL's lossy implicit DOUBLE
//     conversion on a text-encoded SUM).
//   - stakeC: a deregistered (inactive) account with a nonzero reward and no
//     UTxOs (exercises registered=false with pool_key_hash NULL and
//     reward_stake carried through the account.reward CAST fix).
//   - stakeD: a live UTxO with no matching account row at all (exercises the
//     UTxO-only fallback branch of the creds UNION).
func seedRewardLiveStakeEquivalenceScenario(
	t *testing.T,
	db *gorm.DB,
	h rewardLiveStakeEquivalenceHashes,
) {
	t.Helper()

	accounts := []models.Account{
		{
			StakingKey:    h.stakeA,
			CredentialTag: 0,
			Pool:          h.poolA,
			Reward:        1_000_000,
			Active:        true,
			AddedSlot:     10,
		},
		{
			StakingKey:    h.stakeB,
			CredentialTag: 1,
			Pool:          h.poolB,
			Reward:        0,
			Active:        true,
			AddedSlot:     20,
		},
		{
			// Active starts true and is flipped to false below: GORM's
			// Create skips zero-value fields when the model has a
			// `default:true` tag (Account.Active does), so Active: false
			// here would otherwise be silently stored as true.
			StakingKey:    h.stakeC,
			CredentialTag: 0,
			Pool:          nil,
			Reward:        500_000,
			Active:        true,
			AddedSlot:     5,
		},
	}
	for i := range accounts {
		require.NoError(t, db.Create(&accounts[i]).Error)
	}
	require.NoError(
		t,
		db.Model(&accounts[2]).Update("active", false).Error,
	)

	require.NoError(t, db.Create(&models.StakeDelegation{
		StakingKey:    h.stakeA,
		CredentialTag: 0,
		PoolKeyHash:   h.poolA,
		AddedSlot:     15,
	}).Error)

	utxos := []models.Utxo{
		{
			TxId:          bytes.Repeat([]byte{0x01}, 32),
			OutputIdx:     0,
			StakingKey:    h.stakeA,
			CredentialTag: 0,
			Amount:        5_000_000,
			AddedSlot:     11,
		},
		{
			TxId:          bytes.Repeat([]byte{0x02}, 32),
			OutputIdx:     0,
			StakingKey:    h.stakeA,
			CredentialTag: 0,
			Amount:        2_000_000,
			AddedSlot:     12,
			DeletedSlot:   50,
		},
		{
			TxId:          bytes.Repeat([]byte{0x03}, 32),
			OutputIdx:     0,
			StakingKey:    h.stakeB,
			CredentialTag: 1,
			// Exceeds 2^53 (9007199254740992): exposes MySQL's lossy
			// implicit DOUBLE conversion on a text-encoded SUM without an
			// explicit backend-native integer CAST.
			Amount:    9_007_199_254_740_993,
			AddedSlot: 21,
		},
		{
			TxId:          bytes.Repeat([]byte{0x04}, 32),
			OutputIdx:     0,
			StakingKey:    h.stakeD,
			CredentialTag: 0,
			Amount:        3_000_000,
			AddedSlot:     30,
		},
	}
	for i := range utxos {
		require.NoError(t, db.Create(&utxos[i]).Error)
	}
}

// expectedRewardLiveStakeEquivalenceRows is the expected reward_live_stake
// content after RebuildRewardLiveStake over
// seedRewardLiveStakeEquivalenceScenario's scenario, at slot 100.
func expectedRewardLiveStakeEquivalenceRows(
	h rewardLiveStakeEquivalenceHashes,
) []normalizedLiveStakeRow {
	rows := []normalizedLiveStakeRow{
		{
			CredentialTag:      0,
			StakingKey:         string(h.stakeA),
			PoolKeyHash:        string(h.poolA),
			UtxoStake:          5_000_000,
			RewardStake:        1_000_000,
			TotalStake:         6_000_000,
			Registered:         true,
			PoolDelegationSlot: 15,
			UpdatedSlot:        100,
		},
		{
			CredentialTag:      1,
			StakingKey:         string(h.stakeB),
			PoolKeyHash:        string(h.poolB),
			UtxoStake:          9_007_199_254_740_993,
			RewardStake:        0,
			TotalStake:         9_007_199_254_740_993,
			Registered:         true,
			PoolDelegationSlot: 20,
			UpdatedSlot:        100,
		},
		{
			CredentialTag:      0,
			StakingKey:         string(h.stakeC),
			PoolKeyHash:        "",
			UtxoStake:          0,
			RewardStake:        500_000,
			TotalStake:         500_000,
			Registered:         false,
			PoolDelegationSlot: 0,
			UpdatedSlot:        100,
		},
		{
			CredentialTag:      0,
			StakingKey:         string(h.stakeD),
			PoolKeyHash:        "",
			UtxoStake:          3_000_000,
			RewardStake:        0,
			TotalStake:         3_000_000,
			Registered:         false,
			PoolDelegationSlot: 0,
			UpdatedSlot:        100,
		},
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].CredentialTag != rows[j].CredentialTag {
			return rows[i].CredentialTag < rows[j].CredentialTag
		}
		return rows[i].StakingKey < rows[j].StakingKey
	})
	return rows
}

// TestRewardLiveStakeRebuildCrossBackendEquivalenceSqlite seeds the shared
// scenario on sqlite and asserts the rebuilt reward_live_stake rows match
// the expected values exactly. mysql/postgres run the identical scenario
// under the dingo_extra_plugins build tag
// (reward_live_stake_equivalence_test.go in each of those packages); all
// three are expected to produce byte-identical normalized rows from the same
// input, which is the cross-backend payoff of consolidating the three
// RebuildRewardLiveStake SQL copies into one.
func TestRewardLiveStakeRebuildCrossBackendEquivalenceSqlite(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	h := newRewardLiveStakeEquivalenceHashes()
	seedRewardLiveStakeEquivalenceScenario(t, store.DB(), h)

	require.NoError(t, store.RebuildRewardLiveStake(100, nil))

	var rows []models.RewardLiveStake
	require.NoError(t, store.DB().Order(
		"credential_tag ASC, staking_key ASC",
	).Find(&rows).Error)

	require.Equal(
		t,
		expectedRewardLiveStakeEquivalenceRows(h),
		normalizeLiveStakeRows(rows),
	)
}
