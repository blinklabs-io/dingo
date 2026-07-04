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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVerifyRegisteredVrfKey_RejectsUnregisteredOrMismatchedKey verifies the
// consensus-critical binding of a block's VRF verification key to the producing
// pool's on-chain registered VRF key. The VRF proof is validated only against
// the key carried in the header, so a block whose VRF key is not the one the
// pool registered must be rejected — otherwise an attacker can grind VRF keys
// offline and win slots regardless of stake. With no pool registration present
// for the block's issuer, the block's VRF key cannot match any registered key,
// so verification must fail (it must never pass by default).
func TestVerifyRegisteredVrfKey_RejectsUnregisteredOrMismatchedKey(t *testing.T) {
	tb := createTestBlock(t, [32]byte{71}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	// No pool registration seeded for this block's issuer, so the header's VRF
	// key is not bound to any registered VRF key.
	err := ls.verifyRegisteredVrfKey(tb.block)
	require.Error(
		t,
		err,
		"a block whose VRF key is not the issuer pool's registered VRF key must be rejected",
	)
	// The rejection is specifically about the VRF key / pool registration, not
	// some unrelated failure.
	msg := err.Error()
	assert.True(
		t,
		containsAny(msg, "VRF key", "registered VRF key", "registration lookup"),
		"rejection should cite the VRF-key registration binding, got: %s",
		msg,
	)

	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, vrfKey)
	poolKeyHash := tb.block.IssuerVkey().Hash()
	store, ok := db.Metadata().(*sqlite.MetadataStoreSqlite)
	require.True(t, ok, "expected sqlite metadata store")
	require.NoError(t, store.DB().Create(&models.Pool{
		PoolKeyHash: poolKeyHash[:],
		VrfKeyHash:  lcommon.Blake2b256Hash(vrfKey).Bytes(),
	}).Error)

	err = ls.verifyRegisteredVrfKey(tb.block)
	require.Error(
		t,
		err,
		"a denormalized pool row without a registration must not bind "+
			"the header VRF key",
	)
	assert.Contains(t, err.Error(), "registered VRF key hash unavailable")
}

// TestVerifyRegisteredVrfKey_AcceptsMatchingKeyRejectsMismatch is the
// positive-and-mismatch counterpart to the unregistered-pool case: it proves
// the binding accepts a block whose header VRF key hashes to the pool's
// registered VRF key hash, and rejects one whose does not. The VRF proof is
// only ever validated against the header-carried key, so this equality is the
// sole barrier preventing an attacker from registering with one VRF key and
// producing blocks with a different, offline-ground key.
func TestVerifyRegisteredVrfKey_AcceptsMatchingKeyRejectsMismatch(
	t *testing.T,
) {
	// --- Matching registered VRF key is accepted ---
	tbMatch := createTestBlock(t, [32]byte{72}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tbMatch.epochNonce)

	matchVrfKey, ok, err := headerVrfKeyFromBodyCbor(tbMatch.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, matchVrfKey)

	matchPoolKeyHash := tbMatch.block.IssuerVkey().Hash()
	seedPoolRegistration(
		t,
		db,
		matchPoolKeyHash[:],
		lcommon.Blake2b256Hash(matchVrfKey).Bytes(),
	)

	require.NoError(
		t,
		ls.verifyRegisteredVrfKey(tbMatch.block),
		"block whose header VRF key hashes to the pool's registered "+
			"VRF key hash must be accepted",
	)

	// --- Mismatched registered VRF key is rejected ---
	tbMismatch := createTestBlock(t, [32]byte{73}, 0, tamperNone)
	mismatchVrfKey, ok, err := headerVrfKeyFromBodyCbor(
		tbMismatch.block.Header(),
	)
	require.NoError(t, err)
	require.True(t, ok)

	wrongVrfKeyHash := make([]byte, len(lcommon.Blake2b256{}))
	for i := range wrongVrfKeyHash {
		wrongVrfKeyHash[i] = 0xAB
	}
	// Guard against an accidental collision with the block's real VRF key hash.
	require.NotEqual(
		t,
		lcommon.Blake2b256Hash(mismatchVrfKey).Bytes(),
		wrongVrfKeyHash,
	)

	mismatchPoolKeyHash := tbMismatch.block.IssuerVkey().Hash()
	seedPoolRegistration(t, db, mismatchPoolKeyHash[:], wrongVrfKeyHash)

	err = ls.verifyRegisteredVrfKey(tbMismatch.block)
	require.Error(
		t,
		err,
		"block whose header VRF key does not match the registered "+
			"VRF key must be rejected",
	)
	assert.Contains(t, err.Error(), "VRF key does not match")
}

func TestVerifyRegisteredVrfKey_AcceptsRetiredPoolRegistration(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{74}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, vrfKey)

	poolKeyHash := tb.block.IssuerVkey().Hash()
	seedPoolRegistration(
		t,
		db,
		poolKeyHash[:],
		lcommon.Blake2b256Hash(vrfKey).Bytes(),
	)

	require.NoError(t, db.SetEpoch(
		0,
		0,
		nil,
		nil,
		nil,
		nil,
		0,
		1,
		1_000,
		nil,
	))
	require.NoError(t, db.SetEpoch(
		2_000,
		2,
		nil,
		nil,
		nil,
		nil,
		0,
		1,
		1_000,
		nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 2_000,
			Hash: make([]byte, 32),
		},
		BlockNumber: 2,
	}, nil))

	store, ok := db.Metadata().(*sqlite.MetadataStoreSqlite)
	require.True(t, ok, "expected sqlite metadata store")
	var pool models.Pool
	require.NoError(t, store.DB().
		Where("pool_key_hash = ?", poolKeyHash[:]).
		First(&pool).Error)
	require.NoError(t, store.DB().Create(&models.PoolRetirement{
		PoolID:      pool.ID,
		PoolKeyHash: poolKeyHash[:],
		Epoch:       2,
		AddedSlot:   2,
	}).Error)

	require.NoError(
		t,
		ls.verifyRegisteredVrfKey(tb.block),
		"registered VRF-key binding must not depend on current-tip "+
			"active pool filtering",
	)
}

func TestVerifyRegisteredVrfKey_UsesLatestRegistrationBeforePoolRowHash(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{75}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	vrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, vrfKey)
	registeredVrfHash := lcommon.Blake2b256Hash(vrfKey).Bytes()

	staleVrfHash := make([]byte, len(lcommon.Blake2b256{}))
	for i := range staleVrfHash {
		staleVrfHash[i] = 0xCD
	}
	require.NotEqual(t, registeredVrfHash, staleVrfHash)

	poolKeyHash := tb.block.IssuerVkey().Hash()
	err = db.Metadata().ImportPool(
		&models.Pool{
			PoolKeyHash: poolKeyHash[:],
			VrfKeyHash:  staleVrfHash,
		},
		&models.PoolRegistration{
			PoolKeyHash: poolKeyHash[:],
			VrfKeyHash:  registeredVrfHash,
			AddedSlot:   1,
		},
		nil,
	)
	require.NoError(t, err)

	require.NoError(
		t,
		ls.verifyRegisteredVrfKey(tb.block),
		"latest registration VRF hash should take precedence over stale "+
			"denormalized pool VRF hash",
	)
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(sub) > 0 && len(s) >= len(sub) {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}
