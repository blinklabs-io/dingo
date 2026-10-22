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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// leiosKeyEpochsInOrder returns the stored epoch of every registration of the
// pool in write order, with -1 for NULL.
func leiosKeyEpochsInOrder(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
) []int64 {
	t.Helper()
	rows, err := store.writeDB.Query(`
SELECT leios_key_registration_epoch FROM pool_registration
WHERE pool_key_hash = ? ORDER BY id`, pool.Bytes())
	require.NoError(t, err)
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var epoch *int64
		require.NoError(t, rows.Scan(&epoch))
		if epoch == nil {
			out = append(out, -1)
			continue
		}
		out = append(out, *epoch)
	}
	require.NoError(t, rows.Err())
	return out
}

// A retirement names the epoch whose boundary reaps the pool. A pool whose
// re-registration was staged in the retirement's own epoch adopts the staged
// key at the next boundary, and is reaped at the boundary the retirement names:
// a registration in that epoch is a first registration.
func TestPoolLeiosKeyEpochStagedKeyThenRetirementReapedAtNamedEpoch(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 12)
	pool := depositHeldPoolKey(0xd1)

	writeDepositHeldCert(t, store, 2_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(pool, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 5_200, 0, depositHeldRetirement(pool, 7), 0)
	// Still registered in epoch 6, holding the key adopted at that boundary.
	writeDepositHeldCert(t, store, 6_100, 0, leiosKeyEpochCert(pool, 0x12, 0x22), 0)
	// The re-registration above cancelled the retirement, so the pool is
	// still registered at the boundary into 7.
	writeDepositHeldCert(t, store, 7_100, 0, leiosKeyEpochCert(pool, 0x12, 0x22), 0)

	require.Equal(t, []int64{2, 6, 6, 6}, leiosKeyEpochsInOrder(t, store, pool))
}

func TestPoolLeiosKeyEpochRetirementNamingNextEpochReapsStagedKey(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 12)
	reaped7 := depositHeldPoolKey(0xd2)
	reaped6 := depositHeldPoolKey(0xd3)
	cancelled6 := depositHeldPoolKey(0xd4)

	// Each pool's certificates sit in the same slots, told apart by block
	// index.
	pools := []lcommon.PoolKeyHash{reaped7, reaped6, cancelled6}
	at := func(slot uint64, pool lcommon.PoolKeyHash, cert lcommon.Certificate, deposit uint64) {
		t.Helper()
		for index, candidate := range pools {
			if candidate == pool {
				writeDepositHeldCert(t, store, slot, uint32(index), cert, deposit) // #nosec G115 -- three pools
			}
		}
	}
	for _, pool := range pools {
		at(2_100, pool, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
		at(5_100, pool, leiosKeyEpochCert(pool, 0x12, 0x22), 0)
	}
	// Retirement naming 7: reaped at the boundary into 7, after the staged key
	// was adopted at 6.
	at(5_300, reaped7, depositHeldRetirement(reaped7, 7), 0)
	// Retirement naming 6: reaped at the boundary into 6, which discards the
	// staged key with the pool.
	at(5_300, reaped6, depositHeldRetirement(reaped6, 6), 0)
	at(5_300, cancelled6, depositHeldRetirement(cancelled6, 6), 0)
	// A registration before the boundary cancels the retirement.
	at(5_400, cancelled6, leiosKeyEpochCert(cancelled6, 0x12, 0x22), 0)

	at(7_100, reaped7, leiosKeyEpochCert(reaped7, 0x12, 0x22), 500)
	at(6_100, reaped6, leiosKeyEpochCert(reaped6, 0x12, 0x22), 500)
	at(7_200, cancelled6, leiosKeyEpochCert(cancelled6, 0x12, 0x22), 0)

	reapedAt7 := leiosKeyEpochsInOrder(t, store, reaped7)
	reapedAt6 := leiosKeyEpochsInOrder(t, store, reaped6)
	cancelledAt6 := leiosKeyEpochsInOrder(t, store, cancelled6)
	require.Len(t, reapedAt7, 3)
	require.Len(t, reapedAt6, 3)
	require.Len(t, cancelledAt6, 4)
	require.Equal(t, int64(7), reapedAt7[2])
	require.Equal(t, int64(6), reapedAt6[2])
	// The cancelling registration's staged key was adopted at 6.
	require.Equal(t, int64(6), cancelledAt6[3])
}

// Certificates in one block share a slot and are ordered by the block index
// of their transaction, then by their index inside it.
func TestPoolLeiosKeyEpochOrdersCertificatesWithinOneBlock(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 12)
	pool := depositHeldPoolKey(0xd5)

	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 5_100, 1, leiosKeyEpochCert(pool, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 5_100, 2, leiosKeyEpochCert(pool, 0x11, 0x21), 0)
	// Active at the boundary is the last staged key, 0x11, the original.
	writeDepositHeldCert(t, store, 7_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 0)

	require.Equal(t, []int64{5, 6, 5, 5}, leiosKeyEpochsInOrder(t, store, pool))
}

func TestPoolLeiosKeyEpochOrdersCertificatesWithinOneTransaction(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 12)
	pool := depositHeldPoolKey(0xd6)

	hash := bytes.Repeat([]byte{0xd6}, 32)
	tx := mockledger.NewTransactionBuilder().WithCertificates(
		leiosKeyEpochCert(pool, 0x11, 0x21),
		leiosKeyEpochCert(pool, 0x12, 0x22),
	)
	tx.WithId(hash)
	tx.WithValid(true)
	require.NoError(t, store.SetTransaction(
		tx,
		ocommon.Point{Slot: 6_100, Hash: hash},
		0,
		map[int]uint64{0: 500},
		false,
		nil, 0,
	))

	// The first certificate registers the pool in epoch 6; the second is a
	// re-registration with another key, adopted at the next boundary.
	require.Equal(t, []int64{6, 7}, leiosKeyEpochsInOrder(t, store, pool))
}

func importLeiosKeyPoolStaged(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
	slot uint64,
	currentSeed byte,
	currentEpoch uint64,
	stagedSeed byte,
) {
	t.Helper()
	for index, seed := range []byte{currentSeed, stagedSeed} {
		key := bytes.Repeat([]byte{seed}, 96)
		proof := bytes.Repeat([]byte{seed + 0x10}, 48)
		registration := &models.PoolRegistration{
			PoolKeyHash:             pool.Bytes(),
			VrfKeyHash:              bytes.Repeat([]byte{0x5b}, 32),
			RewardAccount:           bytes.Repeat([]byte{0x5c}, 28),
			AddedSlot:               slot,
			RegistrationIndex:       uint(index),
			LeiosKeyPublic:          key,
			LeiosKeyPossessionProof: proof,
		}
		if index == 0 {
			registration.LeiosKeyRegistrationEpoch = &currentEpoch
		} else {
			registration.LeiosKeyRegistrationAgeUnknown = true
		}
		require.NoError(t, store.ImportPool(
			&models.Pool{
				PoolKeyHash:             pool.Bytes(),
				VrfKeyHash:              registration.VrfKeyHash,
				RewardAccount:           registration.RewardAccount,
				LeiosKeyPublic:          key,
				LeiosKeyPossessionProof: proof,
			},
			registration,
			nil,
		))
	}
}

// A snapshot's staged parameters (registration index 1) are the key the
// ledger adopts at the boundary after the snapshot's epoch, registered in that
// next epoch unless it equals the key in effect.
func TestPoolLeiosKeyEpochSnapshotStagedKeyIsAdoptedAtNextEpoch(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 12)
	stagedKey := depositHeldPoolKey(0xd7)
	currentKey := depositHeldPoolKey(0xd8)
	rotatedAgain := depositHeldPoolKey(0xd9)

	// Snapshot at epoch 4: current key 0x11 registered in epoch 2, staged 0x12.
	importLeiosKeyPoolStaged(t, store, stagedKey, 4_050, 0x11, 2, 0x12)
	importLeiosKeyPoolStaged(t, store, currentKey, 4_051, 0x11, 2, 0x12)
	importLeiosKeyPoolStaged(t, store, rotatedAgain, 4_052, 0x11, 2, 0x12)

	// The staged key is adopted in epoch 5, so a later same-key registration
	// keeps 5.
	writeDepositHeldCert(t, store, 6_100, 0, leiosKeyEpochCert(stagedKey, 0x12, 0x22), 0)
	// Registering the original key in epoch 4, before the boundary, restages it
	// against the current key, which it equals: it keeps epoch 2.
	writeDepositHeldCert(t, store, 4_500, 0, leiosKeyEpochCert(currentKey, 0x11, 0x21), 0)
	// A key other than the adopted one, after the boundary, is registered in
	// the epoch after the transaction's.
	writeDepositHeldCert(t, store, 6_200, 0, leiosKeyEpochCert(rotatedAgain, 0x11, 0x21), 0)

	staged := leiosKeyEpochsInOrder(t, store, stagedKey)
	require.Equal(t, int64(5), staged[len(staged)-1])
	current := leiosKeyEpochsInOrder(t, store, currentKey)
	require.Equal(t, int64(2), current[len(current)-1])
	rotated := leiosKeyEpochsInOrder(t, store, rotatedAgain)
	require.Equal(t, int64(7), rotated[len(rotated)-1])
}

// A snapshot import replaces the pool's state where it lands, even when the
// pool registered earlier in the same epoch: the import's current parameters
// are the active state for the rest of the epoch. A re-registration keeping
// the snapshot's key keeps the snapshot's epoch, and one registering the
// pre-import key stages it for the next boundary.
func TestPoolLeiosKeyEpochSnapshotImportReplacesSameEpochLiveState(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 12)
	kept := depositHeldPoolKey(0xdb)
	stale := depositHeldPoolKey(0xdc)

	// Both pools register live in epoch 4 with key 0x11, told apart by the
	// block index of their transaction.
	writeDepositHeldCert(t, store, 4_100, 0, leiosKeyEpochCert(kept, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 4_100, 1, leiosKeyEpochCert(stale, 0x11, 0x21), 500)
	// A snapshot import later in the same epoch carries key 0x12 registered
	// in epoch 2: the live row's key no longer reflects the ledger state.
	importLeiosKeyPoolStaged(t, store, kept, 4_500, 0x12, 2, 0x13)
	importLeiosKeyPoolStaged(t, store, stale, 4_501, 0x12, 2, 0x13)

	writeDepositHeldCert(t, store, 4_600, 0, leiosKeyEpochCert(kept, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 4_600, 1, leiosKeyEpochCert(stale, 0x11, 0x21), 0)

	requireLeiosKeyEpoch(t, store, kept, 4_600, 2)
	requireLeiosKeyEpoch(t, store, stale, 4_600, 5)
}

// A pool reaped before the epoch and registered afresh in it holds the fresh
// registration's key for the rest of the epoch, not the key of its earlier
// lifetime: a later re-registration in the same epoch is compared with it.
func TestPoolLeiosKeyEpochReRegistrationAfterFreshLifetimeInSameEpoch(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 12)
	pool := depositHeldPoolKey(0xda)

	writeDepositHeldCert(t, store, 2_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 3_100, 0, depositHeldRetirement(pool, 5), 0)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 5_200, 0, leiosKeyEpochCert(pool, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 5_300, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 0)

	require.Equal(t, []int64{2, 5, 6, 5}, leiosKeyEpochsInOrder(t, store, pool))
}
