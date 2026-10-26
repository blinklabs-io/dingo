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
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// The registered-in epoch of a pool's BLS key follows cardano-ledger's
// Dijkstra POOL and POOLREAP rules (IntersectMBO/cardano-ledger at
// 9df9f98c): a pool that is not registered gets the epoch of the registering
// transaction (Dijkstra Rules/Pool.hs:261, mkStakePoolState cEpoch SNothing); a
// re-registration is staged and adopted at the next epoch boundary, keeping
// the active key's epoch when the key is unchanged and otherwise taking the
// boundary's epoch (Dijkstra Rules/PoolReap.hs:130, State/StakePool.hs
// mkStakePoolState). See leioskeyepoch.Registration for the rule chain.
// These tests drive SetTransaction and read the persisted column, using
// nonzero epochs throughout so an unwritten column cannot pass for epoch 0.

func leiosKeyEpochCert(
	pool lcommon.PoolKeyHash,
	keySeed byte,
	proofSeed byte,
) *lcommon.PoolRegistrationCertificate {
	cert := depositHeldRegistration(pool)
	if keySeed != 0 {
		cert.LeiosKey = &lcommon.LeiosKey{
			PublicKey:       bytes.Repeat([]byte{keySeed}, 96),
			PossessionProof: bytes.Repeat([]byte{proofSeed}, 48),
		}
	}
	return cert
}

type leiosKeyEpochColumns struct {
	epoch      sql.NullInt64
	ageUnknown bool
}

func leiosKeyEpochColumn(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
	slot uint64,
) leiosKeyEpochColumns {
	t.Helper()
	var got leiosKeyEpochColumns
	require.NoError(t, store.writeDB.QueryRow(`
SELECT leios_key_registration_epoch, leios_key_registration_age_unknown
FROM pool_registration
WHERE pool_key_hash = ? AND added_slot = ?`,
		pool.Bytes(),
		slot,
	).Scan(&got.epoch, &got.ageUnknown))
	return got
}

func requireLeiosKeyEpoch(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
	slot uint64,
	want uint64,
) {
	t.Helper()
	got := leiosKeyEpochColumn(t, store, pool, slot)
	require.True(t, got.epoch.Valid, "registration at slot %d has no epoch", slot)
	require.Equal(t, int64(want), got.epoch.Int64, "slot %d", slot)
	require.False(t, got.ageUnknown)
}

func TestPoolLeiosKeyEpochFreshRegistrationIsTransactionEpoch(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 10)
	pool := depositHeldPoolKey(0xb1)

	writeDepositHeldCert(t, store, 3_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)

	requireLeiosKeyEpoch(t, store, pool, 3_100, 3)
}

func TestPoolLeiosKeyEpochSameKeyKeepsOriginalEpoch(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 10)
	pool := depositHeldPoolKey(0xb2)

	writeDepositHeldCert(t, store, 2_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 0)

	requireLeiosKeyEpoch(t, store, pool, 2_100, 2)
	requireLeiosKeyEpoch(t, store, pool, 5_100, 2)
}

// A rotated key is adopted at the boundary after the re-registration, so its
// epoch is the transaction's epoch plus one.
func TestPoolLeiosKeyEpochRotatedKeyTakesNextBoundary(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 10)
	pool := depositHeldPoolKey(0xb3)

	writeDepositHeldCert(t, store, 2_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(pool, 0x12, 0x22), 0)
	// The rotated key is now active, so a later same-key registration keeps
	// the epoch the rotation took effect in.
	writeDepositHeldCert(t, store, 8_100, 0, leiosKeyEpochCert(pool, 0x12, 0x22), 0)
	// Same public key under a different possession proof is a different BlsKey.
	writeDepositHeldCert(t, store, 8_200, 0, leiosKeyEpochCert(pool, 0x12, 0x23), 0)

	requireLeiosKeyEpoch(t, store, pool, 2_100, 2)
	requireLeiosKeyEpoch(t, store, pool, 5_100, 6)
	requireLeiosKeyEpoch(t, store, pool, 8_100, 6)
	requireLeiosKeyEpoch(t, store, pool, 8_200, 9)
}

// Only the last re-registration staged in an epoch is adopted, and it is
// compared with the key active before the epoch, not with the staged one.
func TestPoolLeiosKeyEpochRevertWithinEpochKeepsActiveEpoch(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 10)
	pool := depositHeldPoolKey(0xb4)

	writeDepositHeldCert(t, store, 2_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(pool, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 5_200, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 0)

	requireLeiosKeyEpoch(t, store, pool, 5_100, 6)
	requireLeiosKeyEpoch(t, store, pool, 5_200, 2)
}

// A key dropped by a re-registration is gone after the boundary, so a later
// registration with a key starts a fresh lifetime. The dropping registration
// carries no key and keeps its column NULL.
func TestPoolLeiosKeyEpochDroppedKeyRestartsLifetime(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 10)
	pool := depositHeldPoolKey(0xb5)

	writeDepositHeldCert(t, store, 2_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 4_100, 0, leiosKeyEpochCert(pool, 0, 0), 0)
	writeDepositHeldCert(t, store, 6_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 0)

	require.False(t, leiosKeyEpochColumn(t, store, pool, 4_100).epoch.Valid)
	requireLeiosKeyEpoch(t, store, pool, 6_100, 7)
}

func TestPoolLeiosKeyEpochKeyAddedToKeylessPoolTakesNextBoundary(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 10)
	pool := depositHeldPoolKey(0xb6)

	writeDepositHeldCert(t, store, 2_100, 0, leiosKeyEpochCert(pool, 0, 0), 500)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 0)

	requireLeiosKeyEpoch(t, store, pool, 5_100, 6)
}

// A registration after the retirement was reaped is a first registration, so
// the key's epoch is the transaction's. One that cancels a pending retirement
// is a re-registration and keeps the original epoch.
func TestPoolLeiosKeyEpochRetirementReapRestartsLifetime(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 10)
	reaped := depositHeldPoolKey(0xb7)
	cancelled := depositHeldPoolKey(0xb8)

	writeDepositHeldCert(t, store, 2_100, 0, leiosKeyEpochCert(reaped, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 3_100, 0, depositHeldRetirement(reaped, 4), 0)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(reaped, 0x11, 0x21), 500)

	writeDepositHeldCert(t, store, 2_200, 0, leiosKeyEpochCert(cancelled, 0x11, 0x21), 500)
	writeDepositHeldCert(t, store, 3_200, 0, depositHeldRetirement(cancelled, 6), 0)
	writeDepositHeldCert(t, store, 4_200, 0, leiosKeyEpochCert(cancelled, 0x11, 0x21), 0)
	// Past the epoch the cancelled retirement named, the pool is still registered.
	writeDepositHeldCert(t, store, 7_200, 0, leiosKeyEpochCert(cancelled, 0x11, 0x21), 0)

	requireLeiosKeyEpoch(t, store, reaped, 5_100, 5)
	requireLeiosKeyEpoch(t, store, cancelled, 4_200, 2)
	requireLeiosKeyEpoch(t, store, cancelled, 7_200, 2)
}

// Registrations without a BLS key, which is every registration on a network
// before the protocol version that introduced the key, leave the column as it
// was written before this change.
func TestPoolLeiosKeyEpochKeylessRegistrationLeavesColumnNull(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 10)
	pool := depositHeldPoolKey(0xb9)

	writeDepositHeldCert(t, store, 3_100, 0, leiosKeyEpochCert(pool, 0, 0), 500)
	writeDepositHeldCert(t, store, 5_100, 0, leiosKeyEpochCert(pool, 0, 0), 0)

	for _, slot := range []uint64{3_100, 5_100} {
		got := leiosKeyEpochColumn(t, store, pool, slot)
		require.False(t, got.epoch.Valid, "slot %d", slot)
		require.False(t, got.ageUnknown, "slot %d", slot)
	}
}

// Without epoch rows the transaction cannot be placed, so the column stays
// NULL rather than recording epoch zero.
func TestPoolLeiosKeyEpochUnresolvedEpochLeavesColumnNull(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	pool := depositHeldPoolKey(0xba)

	writeDepositHeldCert(t, store, 3_100, 0, leiosKeyEpochCert(pool, 0x11, 0x21), 500)

	got := leiosKeyEpochColumn(t, store, pool, 3_100)
	require.False(t, got.epoch.Valid)
	require.False(t, got.ageUnknown)
}

func importLeiosKeyPool(
	t *testing.T,
	store *Store,
	pool lcommon.PoolKeyHash,
	slot uint64,
	epoch *uint64,
	ageUnknown bool,
) {
	t.Helper()
	key := bytes.Repeat([]byte{0x11}, 96)
	proof := bytes.Repeat([]byte{0x21}, 48)
	require.NoError(t, store.ImportPool(
		&models.Pool{
			PoolKeyHash:             pool.Bytes(),
			VrfKeyHash:              bytes.Repeat([]byte{0x5b}, 32),
			RewardAccount:           bytes.Repeat([]byte{0x5c}, 28),
			LeiosKeyPublic:          key,
			LeiosKeyPossessionProof: proof,
		},
		&models.PoolRegistration{
			PoolKeyHash:                    pool.Bytes(),
			VrfKeyHash:                     bytes.Repeat([]byte{0x5b}, 32),
			RewardAccount:                  bytes.Repeat([]byte{0x5c}, 28),
			AddedSlot:                      slot,
			LeiosKeyPublic:                 key,
			LeiosKeyPossessionProof:        proof,
			LeiosKeyRegistrationEpoch:      epoch,
			LeiosKeyRegistrationAgeUnknown: ageUnknown,
		},
		nil,
	))
}

// A snapshot-imported key is the active state: a live re-registration with the
// same key keeps the imported epoch, or the imported absence of one, and a
// rotation starts a new lifetime.
func TestPoolLeiosKeyEpochCarriesImportedState(t *testing.T) {
	t.Parallel()
	store := newDepositHeldStore(t)
	depositHeldEpochs(t, store, 10)
	known := depositHeldPoolKey(0xbb)
	unknown := depositHeldPoolKey(0xbc)
	importedEpoch := uint64(4)

	importLeiosKeyPool(t, store, known, 4_050, &importedEpoch, false)
	importLeiosKeyPool(t, store, unknown, 4_060, nil, true)

	writeDepositHeldCert(t, store, 6_100, 0, leiosKeyEpochCert(known, 0x11, 0x21), 0)
	writeDepositHeldCert(t, store, 6_200, 0, leiosKeyEpochCert(unknown, 0x11, 0x21), 0)
	writeDepositHeldCert(t, store, 7_100, 0, leiosKeyEpochCert(known, 0x12, 0x22), 0)
	writeDepositHeldCert(t, store, 7_200, 0, leiosKeyEpochCert(unknown, 0x12, 0x22), 0)

	requireLeiosKeyEpoch(t, store, known, 6_100, 4)
	carried := leiosKeyEpochColumn(t, store, unknown, 6_200)
	require.False(t, carried.epoch.Valid)
	require.True(t, carried.ageUnknown)
	requireLeiosKeyEpoch(t, store, known, 7_100, 8)
	requireLeiosKeyEpoch(t, store, unknown, 7_200, 8)
}
