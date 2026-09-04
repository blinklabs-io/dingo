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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seedPoolRegistrationAtSlot(
	t *testing.T,
	db *database.Database,
	poolKeyHash []byte,
	vrfKeyHash []byte,
	addedSlot uint64,
) {
	t.Helper()
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: poolKeyHash, VrfKeyHash: vrfKeyHash},
		&models.PoolRegistration{
			PoolKeyHash: poolKeyHash,
			VrfKeyHash:  vrfKeyHash,
			AddedSlot:   addedSlot,
		},
		nil,
	))
}

// TestVerifyRegisteredVrfKeyUsesElectingRegistration is the offline #3842
// regression. The real header fixture carries oldVrfKey, while the pool's
// current registration carries newVrfKey. The mark snapshot that elected the
// header was captured before the rotation, so the old key is valid for it.
func TestVerifyRegisteredVrfKeyUsesElectingRegistration(t *testing.T) {
	tb := createTestBlock(t, [32]byte{77}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	// Model a mark snapshot captured at the end of epoch 4, with epoch 5
	// beginning at the capture boundary. The block fixture itself is deliberately
	// small and independent of these synthetic epoch numbers.
	ls.epochCache = []models.Epoch{
		{EpochId: 3, StartSlot: 0, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 4, StartSlot: 100, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 5, StartSlot: 200, LengthInSlots: 1_000, Nonce: tb.epochNonce},
	}
	ls.publishSnapshotsLocked()

	poolKeyHash := tb.block.IssuerVkey().Hash()
	headerVrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	oldVrfKeyHash := lcommon.Blake2b256Hash(headerVrfKey).Bytes()
	newVrfKeyHash := make([]byte, len(oldVrfKeyHash))
	for i := range newVrfKeyHash {
		newVrfKeyHash[i] = 0xFA
	}
	require.NotEqual(t, oldVrfKeyHash, newVrfKeyHash)

	seedPoolRegistrationAtSlot(t, db, poolKeyHash[:], oldVrfKeyHash, 1)
	seedPoolRegistrationAtSlot(t, db, poolKeyHash[:], newVrfKeyHash, 101)
	seedPoolStakeSnapshotOfTypeAtSlot(
		t,
		db,
		4,
		models.PoolStakeSnapshotTypeMark,
		poolKeyHash[:],
		1_000,
		1_000,
		199,
	)

	// The current pool row contains newVrfKeyHash, but epoch 5's electing
	// snapshot uses the registration that was active before the rotation.
	require.NoError(t, ls.verifyBlockHeaderState(tb.block, 5, false))

	// Reuse the same producer identity with a different real header fixture.
	// This reaches the same production validator and proves that historical
	// lookup does not become an unconditional accept of any header key.
	invalid := createTestBlock(t, [32]byte{78}, 0, tamperNone)
	invalid.block.header.Body.IssuerVkey = tb.block.header.Body.IssuerVkey
	invalidKey, ok, err := headerVrfKeyFromBodyCbor(invalid.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEqual(t, oldVrfKeyHash, lcommon.Blake2b256Hash(invalidKey).Bytes())

	err = ls.verifyBlockHeaderState(invalid.block, 5, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "VRF key does not match")
}

func TestVerifyRegisteredVrfKeyRejectsMissingHistoricalRegistration(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{79}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	ls.epochCache = []models.Epoch{
		{EpochId: 3, StartSlot: 0, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 4, StartSlot: 100, LengthInSlots: 100, Nonce: tb.epochNonce},
		{EpochId: 5, StartSlot: 200, LengthInSlots: 1_000, Nonce: tb.epochNonce},
	}
	ls.publishSnapshotsLocked()

	poolKeyHash := tb.block.IssuerVkey().Hash()
	headerVrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	currentVrfKeyHash := lcommon.Blake2b256Hash(headerVrfKey).Bytes()
	seedPoolRegistrationAtSlot(t, db, poolKeyHash[:], currentVrfKeyHash, 101)
	seedPoolStakeSnapshotOfTypeAtSlot(
		t,
		db,
		4,
		models.PoolStakeSnapshotTypeMark,
		poolKeyHash[:],
		1_000,
		1_000,
		199,
	)

	// The live row matches the header, but there is no registration at the
	// historical parameter cutoff. Validation must fail closed instead of
	// silently accepting a key that cannot be proven to have elected the pool.
	err = ls.verifyBlockHeaderState(tb.block, 5, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, errVrfKeyRegistrationHistoryUnavailable)
}
