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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlite

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetRetiringPools covers pending-retirement selection: a future
// retirement is pending, a later registration cancels it (including a
// same-slot registration ordered after it by cert_index), past-epoch
// retirements are excluded, and results order by retirement epoch.
func TestGetRetiringPools(t *testing.T) {
	store := setupTestDB(t)

	seenTxs := map[uint]bool{}
	makeCert := func(id uint, txID uint, certIndex uint, blockIndex uint32) {
		if !seenTxs[txID] {
			seenTxs[txID] = true
			require.NoError(t, store.DB().Create(&models.Transaction{
				ID:         txID,
				Hash:       append(bytes.Repeat([]byte{0x00}, 31), byte(txID)),
				Slot:       100,
				BlockIndex: blockIndex,
			}).Error)
		}
		require.NoError(t, store.DB().Exec(
			"INSERT INTO certs (id, transaction_id, cert_index, cert_type, slot) VALUES (?, ?, ?, 0, 100)",
			id, txID, certIndex,
		).Error)
	}

	poolA := bytes.Repeat([]byte{0xA1}, 28) // pending at epoch 20
	poolB := bytes.Repeat([]byte{0xB2}, 28) // cancelled by later-slot rereg
	poolC := bytes.Repeat([]byte{0xC3}, 28) // cancelled by same-slot rereg (cert order)
	poolD := bytes.Repeat([]byte{0xD4}, 28) // retirement epoch already passed
	poolE := bytes.Repeat([]byte{0xE5}, 28) // pending at epoch 15 (orders first)

	makeCert(1, 1, 0, 0) // poolA reg
	makeCert(2, 2, 0, 0) // poolA retire
	makeCert(3, 3, 0, 0) // poolB reg
	makeCert(4, 4, 0, 0) // poolB retire
	makeCert(5, 5, 0, 0) // poolB rereg (later slot)
	makeCert(6, 6, 0, 0) // poolC reg
	makeCert(7, 7, 0, 0) // poolC retire (same slot as rereg, lower cert_index)
	makeCert(8, 7, 1, 0) // poolC rereg (same slot+tx block, higher cert_index)
	makeCert(9, 9, 0, 0)  // poolD reg
	makeCert(10, 10, 0, 0) // poolD retire (past epoch)
	makeCert(11, 11, 0, 0) // poolE reg
	makeCert(12, 12, 0, 0) // poolE retire

	pools := [][]byte{poolA, poolB, poolC, poolD, poolE}
	poolIDs := map[string]uint{}
	for i, keyHash := range pools {
		pool := models.Pool{
			PoolKeyHash: keyHash,
			VrfKeyHash:  bytes.Repeat([]byte{byte(0xF0 + i)}, 32),
		}
		require.NoError(t, store.DB().Create(&pool).Error)
		poolIDs[string(keyHash)] = pool.ID
	}

	regs := []models.PoolRegistration{
		{PoolKeyHash: poolA, AddedSlot: 100, CertificateID: 1},
		{PoolKeyHash: poolB, AddedSlot: 100, CertificateID: 3},
		{PoolKeyHash: poolB, AddedSlot: 300, CertificateID: 5},
		{PoolKeyHash: poolC, AddedSlot: 100, CertificateID: 6},
		{PoolKeyHash: poolC, AddedSlot: 200, CertificateID: 8},
		{PoolKeyHash: poolD, AddedSlot: 100, CertificateID: 9},
		{PoolKeyHash: poolE, AddedSlot: 100, CertificateID: 11},
	}
	for i := range regs {
		regs[i].PoolID = poolIDs[string(regs[i].PoolKeyHash)]
		require.NoError(t, store.DB().Create(&regs[i]).Error)
	}
	rets := []models.PoolRetirement{
		{PoolKeyHash: poolA, Epoch: 20, AddedSlot: 200, CertificateID: 2},
		{PoolKeyHash: poolB, Epoch: 20, AddedSlot: 200, CertificateID: 4},
		{PoolKeyHash: poolC, Epoch: 20, AddedSlot: 200, CertificateID: 7},
		{PoolKeyHash: poolD, Epoch: 5, AddedSlot: 200, CertificateID: 10},
		{PoolKeyHash: poolE, Epoch: 15, AddedSlot: 300, CertificateID: 12},
	}
	for i := range rets {
		rets[i].PoolID = poolIDs[string(rets[i].PoolKeyHash)]
		require.NoError(t, store.DB().Create(&rets[i]).Error)
	}

	rows, err := store.GetRetiringPools(10, nil)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	// Ordered by retirement epoch: poolE (15) before poolA (20).
	assert.Equal(t, poolE, rows[0].PoolKeyHash)
	assert.Equal(t, uint64(15), rows[0].Epoch)
	assert.Equal(t, poolA, rows[1].PoolKeyHash)
	assert.Equal(t, uint64(20), rows[1].Epoch)
}
