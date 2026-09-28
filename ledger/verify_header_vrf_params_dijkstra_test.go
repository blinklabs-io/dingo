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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fixture below is the ledger state of the node reported in
// blinklabs-io/dingo#4326, read out of its own metadata.sqlite: 21600-slot
// epochs from slot 0, a pool registered four times on one VRF key and
// re-registered on another at slot 639855 (inside epoch 29), the mark
// snapshot that elects epoch 31 captured at 647999, and the block that
// wedged the node at slot 678720 (epoch 31) carrying the rotated key.
const (
	musashiEpochLength   = 21_600
	musashiRotationSlot  = 639_855 // epoch 29
	musashiMark30Capture = 647_999 // last slot of epoch 29
	musashiFailingSlot   = 678_720 // epoch 31
	musashiFailingEpoch  = 31
	// The cutoff the pre-#4326 code derives: the last slot of epoch 28.
	musashiLaggedCutoff = 626_399
)

// musashiEpochs builds an epoch cache of 21600-slot epochs, all stamped with
// one era. The era is a parameter because the parameter cutoff depends on it:
// see poolParamsMergedBeforeSnapshot.
func musashiEpochs(from, to uint64, eraId uint, nonce []byte) []models.Epoch {
	epochs := make([]models.Epoch, 0, to-from+1)
	for e := from; e <= to; e++ {
		epochs = append(epochs, models.Epoch{
			EpochId:       e,
			StartSlot:     e * musashiEpochLength,
			LengthInSlots: musashiEpochLength,
			EraId:         eraId,
			Nonce:         nonce,
		})
	}
	return epochs
}

// seedMusashiRotation writes the #4326 registration history: four
// registrations on oldKey, then the rotation to newKey inside the epoch the
// electing snapshot is captured in.
func seedMusashiRotation(
	t *testing.T,
	ls *LedgerState,
	pool lcommon.PoolKeyHash,
	oldKey, newKey []byte,
) {
	t.Helper()
	for _, slot := range []uint64{334_940, 337_768, 369_168, 369_195} {
		seedPoolRegistrationAtSlot(t, ls.db, pool[:], oldKey, slot)
	}
	seedPoolRegistrationAtSlot(t, ls.db, pool[:], newKey, musashiRotationSlot)
	seedPoolStakeSnapshotOfTypeAtSlot(t, ls.db, 30,
		models.PoolStakeSnapshotTypeMark, pool[:], 1_000, 10_000,
		musashiMark30Capture)
}

// TestElectingVrfKeyHashUsesTheCaptureSlotInDijkstra is the dingo #4326
// regression.
//
// Dijkstra's EPOCH rule runs POOLREAP -- which merges psFutureStakePoolParams
// into psStakePools -- before SNAP freezes the stake snapshot, the reverse of
// Conway's ordering. A re-registration submitted during the captured epoch is
// therefore already merged when the snapshot is taken, so the snapshot carries
// the rotated key and the parameter cutoff is the capture slot itself.
//
// Lagging the cutoff by an epoch here resolves the pre-rotation key, and the
// node rejects the first block the pool produces after the rotation is in
// force -- a block on the canonical chain -- then rewinds, restarts the ledger
// pipeline, re-reads the same block and fails identically.
func TestElectingVrfKeyHashUsesTheCaptureSlotInDijkstra(t *testing.T) {
	t.Parallel()

	nonce := bytes.Repeat([]byte{0x07}, 32)
	tb := createTestBlock(t, [32]byte{61}, 61, tamperNone)
	tb.block.slot = musashiFailingSlot
	ls, _ := newEligibilityTestLedger(t, nonce)
	ls.epochCache = musashiEpochs(26, 31, dijkstra.EraIdDijkstra, nonce)
	ls.publishSnapshotsLocked()

	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x11}, 28))
	oldKey := bytes.Repeat([]byte{0x94}, 32)
	newKey := bytes.Repeat([]byte{0x44}, 32)
	seedMusashiRotation(t, ls, pool, oldKey, newKey)

	cutoff, captured, ok, err := ls.electingPoolParamsCutoffSlot(
		tb.block, musashiFailingEpoch, pool,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint64(musashiMark30Capture), cutoff,
		"Dijkstra merges the captured epoch's re-registrations before SNAP, "+
			"so the cutoff is the capture slot")
	assert.Equal(t, uint64(musashiMark30Capture), captured)
	require.Less(t, uint64(musashiLaggedCutoff), uint64(musashiRotationSlot),
		"the fixture must reproduce the report: the rotation lands after the "+
			"lagged cutoff and before the capture")
	require.Less(t, uint64(musashiRotationSlot), uint64(musashiMark30Capture))

	gotKey, ok, err := ls.electingVrfKeyHash(
		tb.block, musashiFailingEpoch, pool,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, newKey, gotKey.Bytes(),
		"the chain elected this pool on the rotated key from epoch 31")
	assert.NotEqual(t, oldKey, gotKey.Bytes(),
		"resolving at the lagged cutoff discards the rotation and wedges "+
			"the node on a canonical block")
}

// TestVerifyRegisteredVrfKeyAcceptsARotationInsideTheCapturedEpochInDijkstra
// is the same defect at the layer that rejected the block: the header check
// itself, with the block's own VRF key rather than a fixture hash.
//
// Before the fix this fails with "producer pool ... VRF key does not match
// registered VRF key", which is verbatim what the wedged node logged 59 times
// at this one slot.
func TestVerifyRegisteredVrfKeyAcceptsARotationInsideTheCapturedEpochInDijkstra(
	t *testing.T,
) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{62}, 62, tamperNone)
	tb.block.slot = musashiFailingSlot
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.epochCache = musashiEpochs(
		26, 31, dijkstra.EraIdDijkstra, tb.epochNonce,
	)
	ls.publishSnapshotsLocked()

	headerVrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, headerVrfKey)

	pool := lcommon.PoolKeyHash(tb.block.IssuerVkey().Hash())
	rotatedKeyHash := lcommon.Blake2b256Hash(headerVrfKey).Bytes()
	preRotationKeyHash := bytes.Repeat([]byte{0x94}, 32)
	require.NotEqual(t, preRotationKeyHash, rotatedKeyHash)

	seedMusashiRotation(t, ls, pool, preRotationKeyHash, rotatedKeyHash)

	require.NoError(
		t,
		ls.verifyRegisteredVrfKey(tb.block, musashiFailingEpoch),
		"the rotated key is the one the snapshot carries in Dijkstra, so "+
			"the canonical block must be accepted",
	)
}

// TestElectingVrfKeyHashStillLagsPoolParamsBeforeDijkstra pins the half of the
// rule the #4326 fix must not erase. On the identical fixture, with the
// captured epoch in Conway, SNAP runs before POOLREAP, so the snapshot does
// not carry a re-registration from its own epoch and the cutoff still lags by
// one epoch. Resolving at the capture slot here is the #3842 wedge.
func TestElectingVrfKeyHashStillLagsPoolParamsBeforeDijkstra(t *testing.T) {
	t.Parallel()

	nonce := bytes.Repeat([]byte{0x07}, 32)
	tb := createTestBlock(t, [32]byte{63}, 63, tamperNone)
	tb.block.slot = musashiFailingSlot
	ls, _ := newEligibilityTestLedger(t, nonce)
	ls.epochCache = musashiEpochs(26, 31, conway.EraIdConway, nonce)
	ls.publishSnapshotsLocked()

	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x11}, 28))
	oldKey := bytes.Repeat([]byte{0x94}, 32)
	newKey := bytes.Repeat([]byte{0x44}, 32)
	seedMusashiRotation(t, ls, pool, oldKey, newKey)

	cutoff, captured, ok, err := ls.electingPoolParamsCutoffSlot(
		tb.block, musashiFailingEpoch, pool,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint64(musashiLaggedCutoff), cutoff,
		"Conway takes the snapshot before the merge, so parameters lag the "+
			"capture by one epoch")
	assert.Equal(t, uint64(musashiMark30Capture), captured)

	gotKey, ok, err := ls.electingVrfKeyHash(
		tb.block, musashiFailingEpoch, pool,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, oldKey, gotKey.Bytes(),
		"a re-registration inside the captured epoch is deferred past SNAP "+
			"in Conway")
}

// TestElectingPoolParamsCutoffSlotUsesTheCapturedEpochsEra pins which era
// decides the ordering.
//
// The EPOCH transition out of epoch N runs under the protocol version in
// force during N, and HARDFORK is a sub-rule of that same transition, so the
// snapshot frozen at that boundary was frozen by epoch N's rules. A block two
// epochs after the hard fork is therefore elected by a snapshot Conway's
// ordering froze, and asking the block's era -- or the current one -- gives
// the wrong cutoff for exactly the two epochs following every hard fork into
// Dijkstra.
func TestElectingPoolParamsCutoffSlotUsesTheCapturedEpochsEra(t *testing.T) {
	t.Parallel()

	nonce := bytes.Repeat([]byte{0x07}, 32)
	tb := createTestBlock(t, [32]byte{64}, 64, tamperNone)
	tb.block.slot = musashiFailingSlot
	ls, _ := newEligibilityTestLedger(t, nonce)

	// Epoch 29 -- the epoch the electing snapshot is captured in -- is the
	// last Conway epoch; epochs 30 and 31, including the block's own, are
	// Dijkstra.
	epochs := musashiEpochs(26, 29, conway.EraIdConway, nonce)
	epochs = append(
		epochs,
		musashiEpochs(30, 31, dijkstra.EraIdDijkstra, nonce)...,
	)
	ls.epochCache = epochs
	ls.publishSnapshotsLocked()

	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x11}, 28))
	oldKey := bytes.Repeat([]byte{0x94}, 32)
	newKey := bytes.Repeat([]byte{0x44}, 32)
	seedMusashiRotation(t, ls, pool, oldKey, newKey)

	cutoff, _, ok, err := ls.electingPoolParamsCutoffSlot(
		tb.block, musashiFailingEpoch, pool,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint64(musashiLaggedCutoff), cutoff,
		"the capture was frozen by epoch 29's rules, which are Conway's, "+
			"even though the validated block is Dijkstra")

	gotKey, ok, err := ls.electingVrfKeyHash(
		tb.block, musashiFailingEpoch, pool,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, oldKey, gotKey.Bytes())
}
