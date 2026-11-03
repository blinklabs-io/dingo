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
	_ "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/bigintsum"
	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func poolStakeTestKey(pool byte, index int) []byte {
	key := bytes.Repeat([]byte{pool}, 28)
	key[24] = byte(index >> 24)
	key[25] = byte(index >> 16)
	key[26] = byte(index >> 8)
	key[27] = byte(index)
	return key
}

// seedActivePool registers pool as an active pool with a tip and epoch, so
// the active pool set is non-empty.
func seedActivePool(t *testing.T, store *Store, pool []byte) {
	t.Helper()
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: pool},
		&models.PoolRegistration{PoolKeyHash: pool, AddedSlot: 10},
		nil,
	))
}

func seedPoolTip(t *testing.T, store *Store) {
	t.Helper()
	require.NoError(t, store.SetEpoch(
		2, 0, nil, nil, nil, nil, 0, 1, 100, nil,
	))
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 25, Hash: []byte("tip")},
		BlockNumber: 1,
	}, nil))
}

// seedPoolDelegator delegates a new active account to pool and, when stake is
// non-empty, overwrites the account's live UTxO stake with it. The live row
// is written by CreateAccount, so a stake of "" leaves it empty and
// removeLive deletes it.
func seedPoolDelegator(
	t *testing.T,
	store *Store,
	pool []byte,
	index int,
	stake string,
) []byte {
	t.Helper()
	key := poolStakeTestKey(pool[0], index)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, Pool: pool, Active: true, AddedSlot: 1,
	}))
	if stake != "" {
		res, err := store.writeDB.Exec(
			store.dialect.Rebind(
				"UPDATE reward_live_stake SET utxo_stake = ? WHERE staking_key = ?",
			),
			stake, key,
		)
		require.NoError(t, err)
		n, err := res.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, n, "CreateAccount writes the live row")
	}
	return key
}

// TestGetStakeByPoolsDoesNotStreamDelegators proves the per-pool live stake
// and delegator count cross to the caller as one row per pool, whatever the
// number of delegators.
func TestGetStakeByPoolsDoesNotStreamDelegators(t *testing.T) {
	t.Parallel()
	store, counter := newRowCountingStore(t)
	pools := [][]byte{
		bytes.Repeat([]byte{0xa1}, 28),
		bytes.Repeat([]byte{0xa2}, 28),
		bytes.Repeat([]byte{0xa3}, 28),
	}
	const delegators = 20
	for _, pool := range pools {
		for i := range delegators {
			seedPoolDelegator(t, store, pool, i, "1000")
		}
	}

	counter.armed.Store(true)
	stakes, counts, err := store.GetStakeByPools(pools, nil)
	counter.armed.Store(false)
	require.NoError(t, err)

	for _, pool := range pools {
		require.EqualValues(t, delegators*1000, stakes[string(pool)])
		require.EqualValues(t, delegators, counts[string(pool)])
	}
	require.LessOrEqual(
		t, counter.rows.Load(), int64(len(pools)),
		"the database must return at most one row per pool",
	)
}

// TestGetActivePoolStakeTotalDoesNotStreamDelegators proves the network-wide
// total is a single aggregate row.
func TestGetActivePoolStakeTotalDoesNotStreamDelegators(t *testing.T) {
	t.Parallel()
	store, counter := newRowCountingStore(t)
	pools := [][]byte{
		bytes.Repeat([]byte{0xa1}, 28),
		bytes.Repeat([]byte{0xa2}, 28),
	}
	for _, pool := range pools {
		seedActivePool(t, store, pool)
		for i := range 20 {
			seedPoolDelegator(t, store, pool, i, "1000")
		}
	}
	seedPoolTip(t, store)

	counter.armed.Store(true)
	total, err := store.GetActivePoolStakeTotal(nil)
	counter.armed.Store(false)
	require.NoError(t, err)

	require.EqualValues(t, 2*20*1000, total)
	// The tip, epoch and active-pool lookups each return one row; the
	// delegators must add none.
	require.LessOrEqual(
		t, counter.rows.Load(), int64(4),
		"the total must not stream per-delegator rows",
	)
}

// seedPoolUtxo gives key a live UTxO of amount.
func seedPoolUtxo(
	t *testing.T,
	store *Store,
	key []byte,
	id byte,
	amount uint64,
) {
	t.Helper()
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		Address:    []byte("addr"),
		TxId:       bytes.Repeat([]byte{id}, 32),
		StakingKey: key, Amount: types.Uint64(amount), AddedSlot: 15,
	}))
}

// testPoolStakeAggregateContract pins the exact-value contract of
// GetStakeByPools and GetActivePoolStakeTotal on every dialect: sums past
// int64, a pool at the uint64 ceiling, an error rather than a wrapped value
// above it, a fall back to the UTxO table when a live row is missing or
// empty, and zero for a pool with no delegators.
func testPoolStakeAggregateContract(t *testing.T, store *Store) {
	t.Helper()
	poolWide := bytes.Repeat([]byte{0xb1}, 28)
	poolCeiling := bytes.Repeat([]byte{0xb2}, 28)
	poolNone := bytes.Repeat([]byte{0xb3}, 28)
	poolMissing := bytes.Repeat([]byte{0xb4}, 28)
	poolBlank := bytes.Repeat([]byte{0xb5}, 28)
	poolOver := bytes.Repeat([]byte{0xb6}, 28)
	for _, pool := range [][]byte{poolWide, poolNone} {
		seedActivePool(t, store, pool)
	}
	seedPoolTip(t, store)

	seedPoolDelegator(t, store, poolWide, 0, "9223372036854775807")
	seedPoolDelegator(t, store, poolWide, 1, "5")
	seedPoolDelegator(t, store, poolCeiling, 0, "18446744073709551614")
	seedPoolDelegator(t, store, poolCeiling, 1, "1")

	// A missing live row makes the live aggregate incomplete: the result is
	// the UTxO truth (700), not the stale live value of the other delegator.
	missingKey := seedPoolDelegator(t, store, poolMissing, 0, "")
	seedPoolUtxo(t, store, missingKey, 0x71, 700)
	_, err := store.writeDB.Exec(
		store.dialect.Rebind(
			"DELETE FROM reward_live_stake WHERE staking_key = ?",
		),
		missingKey,
	)
	require.NoError(t, err)
	seedPoolDelegator(t, store, poolMissing, 1, "999999")
	// An empty live stake is incomplete too.
	blankKey := seedPoolDelegator(t, store, poolBlank, 0, "")
	seedPoolUtxo(t, store, blankKey, 0x72, 300)
	_, err = store.writeDB.Exec(
		store.dialect.Rebind(
			"UPDATE reward_live_stake SET utxo_stake = '' WHERE staking_key = ?",
		),
		blankKey,
	)
	require.NoError(t, err)

	stakes, counts, err := store.GetStakeByPools(
		[][]byte{poolWide, poolCeiling, poolNone}, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(9223372036854775812), stakes[string(poolWide)])
	require.Equal(t, uint64(2), counts[string(poolWide)])
	require.Equal(t, uint64(18446744073709551615), stakes[string(poolCeiling)])
	require.Equal(t, uint64(2), counts[string(poolCeiling)])
	require.Zero(t, stakes[string(poolNone)])
	require.Zero(t, counts[string(poolNone)])

	stakes, counts, err = store.GetStakeByPools(
		[][]byte{poolMissing, poolBlank}, nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(700), stakes[string(poolMissing)])
	require.Equal(t, uint64(2), counts[string(poolMissing)])
	require.Equal(t, uint64(300), stakes[string(poolBlank)])
	require.Equal(t, uint64(1), counts[string(poolBlank)])

	total, err := store.GetActivePoolStakeTotal(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(9223372036854775812), total)

	// One incomplete pool makes the whole live aggregate incomplete, so the
	// total is the UTxO truth for every active pool: poolWide holds no UTxOs.
	seedActivePool(t, store, poolMissing)
	seedActivePool(t, store, poolBlank)
	total, err = store.GetActivePoolStakeTotal(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(700+300), total)

	seedPoolDelegator(t, store, poolOver, 0, "18446744073709551615")
	seedPoolDelegator(t, store, poolOver, 1, "1")
	_, _, err = store.GetStakeByPools([][]byte{poolOver}, nil)
	require.ErrorContains(t, err, "overflow")
	seedActivePool(t, store, poolOver)
	_, err = store.GetActivePoolStakeTotal(nil)
	require.Error(t, err)
}

// TestGetActivePoolStakeTotalRejectsTotalPastUint64 proves a complete live
// aggregate whose pools each fit in uint64 but whose sum does not is an error
// rather than a wrapped total.
func TestGetActivePoolStakeTotalRejectsTotalPastUint64(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	poolFull := bytes.Repeat([]byte{0xc1}, 28)
	poolOne := bytes.Repeat([]byte{0xc2}, 28)
	seedActivePool(t, store, poolFull)
	seedActivePool(t, store, poolOne)
	seedPoolTip(t, store)
	seedPoolDelegator(t, store, poolFull, 0, "18446744073709551615")
	seedPoolDelegator(t, store, poolOne, 0, "1")

	stakes, _, err := store.GetStakeByPools([][]byte{poolFull, poolOne}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(18446744073709551615), stakes[string(poolFull)])
	require.Equal(t, uint64(1), stakes[string(poolOne)])

	_, err = store.GetActivePoolStakeTotal(nil)
	require.ErrorContains(t, err, "outside uint64")
}

func TestSQLitePoolStakeAggregateContract(t *testing.T) {
	t.Parallel()
	testPoolStakeAggregateContract(t, newMigratedSQLiteStore(t))
}
