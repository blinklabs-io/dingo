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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func poolreapBytes(seed byte) []byte {
	out := make([]byte, 28)
	for i := range out {
		out[i] = seed
	}
	return out
}

// seedRetiringPool creates a pool with a registration and (optionally) a
// retirement, returning the pool ID.
func seedRetiringPool(
	t *testing.T,
	store *MetadataStoreSqlite,
	keyHash, rewardAccount []byte,
	deposit, regSlot uint64,
) *models.Pool {
	t.Helper()
	pool := &models.Pool{PoolKeyHash: keyHash}
	require.NoError(t, store.DB().Create(pool).Error)
	require.NoError(t, store.DB().Create(&models.PoolRegistration{
		PoolID:        pool.ID,
		PoolKeyHash:   keyHash,
		RewardAccount: rewardAccount,
		DepositAmount: types.Uint64(deposit),
		AddedSlot:     regSlot,
	}).Error)
	return pool
}

// TestGetPoolsRetiringAtEpoch covers: a pool retiring at the target epoch is
// returned with its reward account and deposit; a pool retiring at a different
// epoch is excluded; and a pool that re-registers after its retirement (later
// slot) is excluded because the retirement was cancelled.
func TestGetPoolsRetiringAtEpoch(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	// Pool A: retires at epoch 5 — expected in results.
	poolA := seedRetiringPool(t, store, poolreapBytes(0xAA), poolreapBytes(0x11), 500, 100)
	require.NoError(t, store.DB().Create(&models.PoolRetirement{
		PoolID: poolA.ID, PoolKeyHash: poolA.PoolKeyHash, Epoch: 5, AddedSlot: 200,
	}).Error)

	// Pool B: retires at epoch 6 — excluded (wrong epoch).
	poolB := seedRetiringPool(t, store, poolreapBytes(0xBB), poolreapBytes(0x22), 500, 100)
	require.NoError(t, store.DB().Create(&models.PoolRetirement{
		PoolID: poolB.ID, PoolKeyHash: poolB.PoolKeyHash, Epoch: 6, AddedSlot: 200,
	}).Error)

	// Pool C: retires at epoch 5 then re-registers at a later slot — excluded
	// (retirement cancelled).
	poolC := seedRetiringPool(t, store, poolreapBytes(0xCC), poolreapBytes(0x33), 500, 100)
	require.NoError(t, store.DB().Create(&models.PoolRetirement{
		PoolID: poolC.ID, PoolKeyHash: poolC.PoolKeyHash, Epoch: 5, AddedSlot: 200,
	}).Error)
	require.NoError(t, store.DB().Create(&models.PoolRegistration{
		PoolID: poolC.ID, PoolKeyHash: poolC.PoolKeyHash,
		RewardAccount: poolreapBytes(0x33), DepositAmount: types.Uint64(500), AddedSlot: 300,
	}).Error)

	refunds, err := store.GetPoolsRetiringAtEpoch(5, 1000, nil)
	require.NoError(t, err)
	require.Len(t, refunds, 1, "only pool A retires at epoch 5")
	assert.Equal(t, poolA.PoolKeyHash, refunds[0].PoolKeyHash)
	assert.Equal(t, poolreapBytes(0x11), refunds[0].RewardAccount)
	assert.Equal(t, uint64(500), uint64(refunds[0].DepositAmount))

	// Retirement certs added at/after the boundary slot are not yet effective.
	none, err := store.GetPoolsRetiringAtEpoch(5, 150, nil)
	require.NoError(t, err)
	assert.Empty(t, none, "retirement cert added at slot 200 is excluded before slot 150")
}

func TestGetPoolsRetiringAtEpochTieBreaksSameAddedSlot(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	const (
		retireEpoch = uint64(5)
		certSlot    = uint64(300)
		querySlot   = uint64(301)
	)

	tx := &models.Transaction{
		Hash:       []byte("poolreap_same_slot_tx_hash_0001"),
		Slot:       certSlot,
		BlockIndex: 0,
		Valid:      true,
	}
	require.NoError(t, store.DB().Create(tx).Error)
	cert0 := &models.Certificate{
		TransactionID: tx.ID,
		Slot:          certSlot,
		CertIndex:     0,
	}
	cert1 := &models.Certificate{
		TransactionID: tx.ID,
		Slot:          certSlot,
		CertIndex:     1,
	}
	require.NoError(t, store.DB().Create(cert0).Error)
	require.NoError(t, store.DB().Create(cert1).Error)

	laterTx := &models.Transaction{
		Hash:       []byte("poolreap_same_slot_tx_hash_0002"),
		Slot:       certSlot,
		BlockIndex: 1,
		Valid:      true,
	}
	require.NoError(t, store.DB().Create(laterTx).Error)
	cert2 := &models.Certificate{
		TransactionID: laterTx.ID,
		Slot:          certSlot,
		CertIndex:     0,
	}
	require.NoError(t, store.DB().Create(cert2).Error)

	// Pool A: the retirement appears first and a same-slot registration
	// appears later in the transaction. The registration cancels the
	// retirement, so no POOLREAP refund should be emitted.
	poolA := seedRetiringPool(t, store, poolreapBytes(0xAA), poolreapBytes(0x11), 500, 100)
	require.NoError(t, store.DB().Create(&models.PoolRetirement{
		PoolID:        poolA.ID,
		PoolKeyHash:   poolA.PoolKeyHash,
		Epoch:         retireEpoch,
		AddedSlot:     certSlot,
		CertificateID: cert0.ID,
	}).Error)
	require.NoError(t, store.DB().Create(&models.PoolRegistration{
		PoolID:        poolA.ID,
		PoolKeyHash:   poolA.PoolKeyHash,
		RewardAccount: poolreapBytes(0x22),
		DepositAmount: types.Uint64(700),
		AddedSlot:     certSlot,
		CertificateID: cert1.ID,
	}).Error)

	// Pool B: the same-slot retirement appears after the registration and is
	// therefore effective. The refund uses that latest registration.
	poolB := &models.Pool{PoolKeyHash: poolreapBytes(0xBB)}
	require.NoError(t, store.DB().Create(poolB).Error)
	require.NoError(t, store.DB().Create(&models.PoolRegistration{
		PoolID:        poolB.ID,
		PoolKeyHash:   poolB.PoolKeyHash,
		RewardAccount: poolreapBytes(0x33),
		DepositAmount: types.Uint64(900),
		AddedSlot:     certSlot,
		CertificateID: cert0.ID,
	}).Error)
	require.NoError(t, store.DB().Create(&models.PoolRetirement{
		PoolID:        poolB.ID,
		PoolKeyHash:   poolB.PoolKeyHash,
		Epoch:         retireEpoch,
		AddedSlot:     certSlot,
		CertificateID: cert1.ID,
	}).Error)

	// Pool C: the earlier transaction's retirement has a higher CertIndex,
	// but the later transaction's registration wins because BlockIndex is
	// compared before CertIndex.
	poolC := &models.Pool{PoolKeyHash: poolreapBytes(0xCC)}
	require.NoError(t, store.DB().Create(poolC).Error)
	require.NoError(t, store.DB().Create(&models.PoolRetirement{
		PoolID:        poolC.ID,
		PoolKeyHash:   poolC.PoolKeyHash,
		Epoch:         retireEpoch,
		AddedSlot:     certSlot,
		CertificateID: cert1.ID,
	}).Error)
	require.NoError(t, store.DB().Create(&models.PoolRegistration{
		PoolID:        poolC.ID,
		PoolKeyHash:   poolC.PoolKeyHash,
		RewardAccount: poolreapBytes(0x44),
		DepositAmount: types.Uint64(1100),
		AddedSlot:     certSlot,
		CertificateID: cert2.ID,
	}).Error)

	refunds, err := store.GetPoolsRetiringAtEpoch(retireEpoch, querySlot, nil)
	require.NoError(t, err)
	require.Len(t, refunds, 1, "later BlockIndex must precede CertIndex")
	assert.Equal(t, poolB.PoolKeyHash, refunds[0].PoolKeyHash)
	assert.Equal(t, poolreapBytes(0x33), refunds[0].RewardAccount)
	assert.Equal(t, uint64(900), uint64(refunds[0].DepositAmount))
}
