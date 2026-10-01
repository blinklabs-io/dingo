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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fixture below is the ledger state of a node from a field
// report, read out of its own metadata.sqlite: 21600-slot
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
	// The cutoff the code derives: the last slot of epoch 28.
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

// seedMusashiRotation writes the registration history: four
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

// TestElectingVrfKeyHashUsesTheCaptureSlotInDijkstra is the
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
// rule the fix must not erase. On the identical fixture, with the
// captured epoch in Conway, SNAP runs before POOLREAP, so the snapshot does
// not carry a re-registration from its own epoch and the cutoff still lags by
// one epoch. Resolving at the capture slot here is the wedge.
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

// seedPoolRegistrationAtSlot adds one registration to a pool's history. Repeated
// calls accumulate rows, and the pool row keeps the most recently written key --
// the same shape the live registration lookup reads.
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

// previewEpochs builds an epoch cache with Preview's 86400-slot epochs.
func previewEpochs(from, to uint64, nonce []byte) []models.Epoch {
	const epochLen = 86_400
	epochs := make([]models.Epoch, 0, to-from+1)
	for e := from; e <= to; e++ {
		epochs = append(epochs, models.Epoch{
			EpochId:       e,
			StartSlot:     e * epochLen,
			LengthInSlots: epochLen,
			Nonce:         nonce,
		})
	}
	return epochs
}

// TestElectingVrfKeyHashLagsPoolParamsByOneEpoch is the pre-rotation-key
// regression, built from the rotation that wedged a Preview replay twice.
//
// The pool ran on oldKey, rotated to newKey at slot 3279920 (epoch 37), and
// rotated back at slot 3366753 (epoch 38). The chain elected it on oldKey in
// both epoch 38 and epoch 39.
//
// Binding the key to the live registration wedges epoch 38. Binding it to the
// electing snapshot's capture slot clears epoch 38 but still wedges epoch 39,
// because mark(38) was captured at 3283199 -- after the rotation -- while the
// parameters frozen in it are those in force through the end of epoch 36.
//
// Only the parameter cutoff, the last slot of the epoch preceding the capture,
// resolves oldKey for both epochs.
func TestElectingVrfKeyHashLagsPoolParamsByOneEpoch(t *testing.T) {
	t.Parallel()

	nonce := bytes.Repeat([]byte{0x07}, 32)
	tb := createTestBlock(t, [32]byte{51}, 51, tamperNone)
	ls, db := newEligibilityTestLedger(t, nonce)
	ls.epochCache = previewEpochs(35, 39, nonce)
	ls.publishSnapshotsLocked()

	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x11}, 28))
	oldKey := bytes.Repeat([]byte{0xB5}, 32)
	newKey := bytes.Repeat([]byte{0xFA}, 32)

	seedPoolRegistrationAtSlot(t, db, pool[:], oldKey, 2_479_516)
	seedPoolRegistrationAtSlot(t, db, pool[:], newKey, 3_279_920)
	seedPoolRegistrationAtSlot(t, db, pool[:], oldKey, 3_366_753)

	// mark(37) elects epoch 38; mark(38) elects epoch 39.
	seedPoolStakeSnapshotOfTypeAtSlot(t, db, 37,
		models.PoolStakeSnapshotTypeMark, pool[:], 1_000, 10_000, 3_196_799)
	seedPoolStakeSnapshotOfTypeAtSlot(t, db, 38,
		models.PoolStakeSnapshotTypeMark, pool[:], 1_000, 10_000, 3_283_199)

	for _, tc := range []struct {
		name   string
		epoch  uint64
		cutoff uint64
	}{
		// Capture 3196799 sits in epoch 36, whose start is 3110400.
		{"epoch 38 elected by mark(37)", 38, 3_110_399},
		// Capture 3283199 sits in epoch 37, whose start is 3196800.
		{"epoch 39 elected by mark(38)", 39, 3_196_799},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gotCutoff, _, ok, err := ls.electingPoolParamsCutoffSlot(
				tb.block, tc.epoch, pool,
			)
			require.NoError(t, err)
			require.True(t, ok)
			assert.Equal(t, tc.cutoff, gotCutoff,
				"parameters lag the capture by one epoch")

			gotKey, ok, err := ls.electingVrfKeyHash(tb.block, tc.epoch, pool)
			require.NoError(t, err)
			require.True(t, ok)
			assert.Equal(t, oldKey, gotKey.Bytes(),
				"the chain elected this pool on the old key in this epoch")
		})
	}

	// The rotation is not ignored forever: once a full epoch has passed since
	// it was merged, the new key is the electing one.
	seedPoolStakeSnapshotOfTypeAtSlot(t, db, 39,
		models.PoolStakeSnapshotTypeMark, pool[:], 1_000, 10_000, 3_369_599)
	gotKey, ok, err := ls.electingVrfKeyHash(tb.block, 40, pool)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(
		t,
		newKey,
		gotKey.Bytes(),
		"epoch 40 is elected by parameters in force through the end of epoch 37",
	)
}

// blockEpochId resolves the epoch a block falls in, the way
// verifyBlockHeaderState resolves it before calling verifyRegisteredVrfKey.
// Tests use it rather than a literal so they keep matching the harness's epoch
// cache if that changes.
func blockEpochId(
	t *testing.T,
	ls *LedgerState,
	block gledger.Block,
) uint64 {
	t.Helper()
	epoch, err := ls.epochForSlot(block.SlotNumber())
	require.NoError(t, err)
	return epoch.EpochId
}

// TestElectingVrfKeyHashResolvesTheEarlierKeyWhenAReRegistrationFollowsTheCutoff
// pins the resolved key rather than the cutoff, for the case the cutoff exists
// to handle: a pool with an earlier registration whose re-registration lands
// after the parameter cutoff must elect on the earlier key.
//
// cardano-ledger routes a re-registration through psFutureStakePoolParams,
// which POOLREAP merges only after SNAP has run, so the snapshot still carries
// the parameters in force before it.
//
// Three registrations, not two, so the assertion distinguishes which lookup
// answered. The first-registration fallback resolves the EARLIEST registration
// at or before the capture; the cutoff lookup resolves the LATEST at or before
// the cutoff. With a registration before both, those differ — the fallback
// would yield originalKey and only the cutoff lookup yields cutoffKey. A
// two-registration fixture makes them coincide, so it would pass whichever
// path ran.
func TestElectingVrfKeyHashResolvesTheEarlierKeyWhenAReRegistrationFollowsTheCutoff(
	t *testing.T,
) {
	t.Parallel()

	nonce := bytes.Repeat([]byte{0x07}, 32)
	tb := createTestBlock(t, [32]byte{51}, 51, tamperNone)
	ls, db := newEligibilityTestLedger(t, nonce)
	ls.epochCache = previewEpochs(35, 39, nonce)
	ls.publishSnapshotsLocked()

	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x11}, 28))
	originalKey := bytes.Repeat([]byte{0xC3}, 32)
	cutoffKey := bytes.Repeat([]byte{0xB5}, 32)
	rotatedKey := bytes.Repeat([]byte{0xFA}, 32)

	// Cutoff for epoch 38 is 3110399, capture is 3196799. The first two
	// registrations precede the cutoff; the re-registration falls between the
	// cutoff and the capture.
	seedPoolRegistrationAtSlot(t, db, pool[:], originalKey, 2_479_516)
	seedPoolRegistrationAtSlot(t, db, pool[:], cutoffKey, 3_000_000)
	seedPoolRegistrationAtSlot(t, db, pool[:], rotatedKey, 3_150_000)
	seedPoolStakeSnapshotOfTypeAtSlot(t, db, 37,
		models.PoolStakeSnapshotTypeMark, pool[:], 1_000, 10_000, 3_196_799)

	got, ok, err := ls.electingVrfKeyHash(tb.block, 38, pool)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t,
		lcommon.NewBlake2b256(cutoffKey), got,
		"the re-registration was deferred past SNAP, so the snapshot "+
			"carries the key in force at the parameter cutoff",
	)
	assert.NotEqual(t,
		lcommon.NewBlake2b256(rotatedKey), got,
		"resolving the latest registration at or before the capture would "+
			"pick the deferred key and reject a canonical block",
	)
	assert.NotEqual(t,
		lcommon.NewBlake2b256(originalKey), got,
		"the first-registration fallback must not answer when a "+
			"registration is in force at the cutoff",
	)
}

// TestElectingPoolParamsCutoffSlotUsesTheSuppliedEpochCache pins that the
// cutoff path resolves the Mithril trust boundary against the epoch cache it
// was handed, not against whatever cache is live when it runs.
//
// verifyBlockHeaderStateWithCache pins one immutable cache and threads it
// through so the VRF key and the stake eligibility check cannot be answered
// from different snapshot generations. shouldUseImportedActivePoolDistribution
// selects which snapshot elects the block, so a second, unpinned read there
// reopens the gap the pinning exists to close.
//
// The two caches disagree by construction: the live one starts at epoch 38 and
// cannot place the Mithril boundary at all, so reading it fails the lookup
// outright rather than returning a merely different answer.
func TestElectingPoolParamsCutoffSlotUsesTheSuppliedEpochCache(t *testing.T) {
	t.Parallel()

	nonce := bytes.Repeat([]byte{0x07}, 32)
	tb := createTestBlock(t, [32]byte{53}, 53, tamperNone)
	tb.block.slot = 3_400_000
	ls, db := newEligibilityTestLedger(t, nonce)

	// Live cache: epochs 38-39 only. The Mithril boundary predates it.
	ls.epochCache = previewEpochs(38, 39, nonce)
	ls.mithrilLedgerSlot = 3_150_000
	ls.publishSnapshotsLocked()

	// Supplied cache: epochs 35-39, which does place the boundary.
	supplied := previewEpochs(35, 39, nonce)

	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x11}, 28))
	seedPoolStakeSnapshotOfTypeAtSlot(t, db, 37,
		models.PoolStakeSnapshotTypeMark, pool[:], 1_000, 10_000, 3_196_799)

	cutoff, captured, ok, err := ls.electingPoolParamsCutoffSlotWithCache(
		tb.block, 38, pool, supplied,
	)
	require.NoError(t, err,
		"the boundary must be resolved against the supplied cache")
	require.True(t, ok)
	assert.Equal(t, uint64(3_110_399), cutoff)
	assert.Equal(t, uint64(3_196_799), captured)
}

// TestLeaderEligibilityStakeUsesTheSuppliedEpochCache is the other half of the
// same pairing: the stake side must select its snapshot from the same cache
// the VRF key side used, or the two can disagree about whether the imported
// active distribution elects this block.
//
// Only the active snapshot is seeded. Selecting the mark snapshot instead --
// which is what resolving the boundary against the live cache produces here --
// finds nothing and rejects.
func TestLeaderEligibilityStakeUsesTheSuppliedEpochCache(t *testing.T) {
	t.Parallel()

	nonce := bytes.Repeat([]byte{0x07}, 32)
	tb := createTestBlock(t, [32]byte{54}, 54, tamperNone)
	tb.block.slot = 3_400_000
	ls, db := newEligibilityTestLedger(t, nonce)

	// Live cache places the Mithril boundary in epoch 39, so epoch 38 would
	// not be the imported epoch and the mark snapshot would be selected.
	ls.epochCache = previewEpochs(38, 39, nonce)
	ls.mithrilLedgerSlot = 3_370_000
	ls.publishSnapshotsLocked()

	// Supplied cache places the same boundary in epoch 38, the epoch under
	// verification, so the imported active distribution is the electing one.
	supplied := []models.Epoch{
		{
			EpochId:       38,
			StartSlot:     3_283_200,
			LengthInSlots: 172_800,
			Nonce:         nonce,
		},
	}

	pool := lcommon.PoolKeyHash(tb.block.IssuerVkey().Hash())
	seedPoolStakeSnapshotOfType(t, db, 38,
		models.PoolStakeSnapshotTypeActive, pool[:], 1_000, 10_000)

	poolStake, totalStake, snapshotEpoch, snapshotType, skip, err :=
		ls.leaderEligibilityStakeWithCache(tb.block, 38, pool, supplied)
	require.NoError(t, err,
		"the electing snapshot must be selected from the supplied cache")
	assert.False(t, skip)
	assert.Equal(t, models.PoolStakeSnapshotTypeActive, snapshotType)
	assert.Equal(t, uint64(38), snapshotEpoch)
	assert.Equal(t, uint64(1_000), poolStake)
	assert.Equal(t, uint64(10_000), totalStake)
}

// TestLeaderEligibilityStakeSkipDecisionUsesTheSuppliedEpochCache completes the
// pairing. shouldSkipPostMithrilMarkEligibility decides whether to bypass the
// leader-eligibility threshold entirely for a mark snapshot reconstructed after
// its own boundary, and it read ls.epochCache directly -- a third generation,
// and the mutable field rather than a published snapshot.
//
// A bypass is the most consequential of the three decisions in this path: it
// admits a block whose stake eligibility nothing checked. It must be taken
// against the same cache as the VRF key it is paired with.
//
// The two caches place epoch 38's start on either side of the capture, so they
// disagree on the bypass: the supplied cache starts epoch 38 after the capture
// and must not skip, while the live cache starts it before and would.
func TestLeaderEligibilityStakeSkipDecisionUsesTheSuppliedEpochCache(
	t *testing.T,
) {
	t.Parallel()

	nonce := bytes.Repeat([]byte{0x07}, 32)
	tb := createTestBlock(t, [32]byte{55}, 55, tamperNone)
	tb.block.slot = 3_400_000
	ls, db := newEligibilityTestLedger(t, nonce)

	// Live cache: epoch 38 starts at 3_283_200, before the capture below, so
	// the bypass would fire.
	ls.epochCache = previewEpochs(38, 39, nonce)
	ls.mithrilLedgerSlot = 3_000_000
	ls.publishSnapshotsLocked()

	// Supplied cache: epoch 38 starts after the capture, so the mark row was
	// not reconstructed past its boundary and eligibility must be evaluated.
	supplied := []models.Epoch{
		{
			EpochId:       36,
			StartSlot:     2_900_000,
			LengthInSlots: 200_000,
			Nonce:         nonce,
		},
		{
			EpochId:       37,
			StartSlot:     3_100_000,
			LengthInSlots: 200_000,
			Nonce:         nonce,
		},
		{
			EpochId:       38,
			StartSlot:     3_300_000,
			LengthInSlots: 200_000,
			Nonce:         nonce,
		},
	}

	pool := lcommon.PoolKeyHash(tb.block.IssuerVkey().Hash())
	other := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x21}, 28))
	seedPoolStakeSnapshotOfTypeAtSlot(t, db, 38,
		models.PoolStakeSnapshotTypeMark, pool[:], 1_000, 0, 3_290_000)
	seedPoolStakeSnapshotOfTypeAtSlot(t, db, 38,
		models.PoolStakeSnapshotTypeMark, other[:], 9_000, 0, 3_290_000)

	poolStake, totalStake, _, _, skip, err :=
		ls.leaderEligibilityStakeWithCache(tb.block, 39, pool, supplied)
	require.NoError(t, err)
	assert.False(t, skip,
		"the bypass must be decided on the supplied cache, which places the "+
			"capture before epoch 38 rather than inside it")
	assert.Equal(t, uint64(1_000), poolStake)
	assert.Equal(t, uint64(10_000), totalStake,
		"not skipping means the threshold's denominator is actually read")
}

// TestElectingVrfKeyHashResolvesBelowMithrilBootstrapAnchor is the
// Mithril-anchor regression: a Mithril-bootstrapped node wedges at its first
// epoch boundary because the pool's only registration row is stamped at the
// bootstrap anchor slot, which is later than the parameter cutoff and
// stake-snapshot capture slots the electing snapshot resolves against.
//
// A Mithril snapshot import writes the pool's live-at-anchor registration
// (ImportPool) -- it never replays the certificate history that produced it
// -- so seedPoolRegistrationAtSlot here models exactly one such bootstrap
// row, at anchor slot 3_200_000: after mark(37)'s capture (3_196_799) and
// cutoff (3_110_399), which elect epoch 38. ls.mithrilLedgerSlot is set to
// the same anchor, the way LedgerState restores it from the persisted
// mithril_ledger_slot sync-state key at startup.
//
// Before the fix, both GetPoolVrfKeyHashAtSlot(cutoff) and
// GetPoolEarliestVrfKeyHashAtSlot(capture) miss -- the only row about this
// pool is time-stamped after both slots -- and electingVrfKeyHash returns
// errVrfKeyRegistrationHistoryUnavailable. That is what wedges the node: the
// error is unconditional once the ledger tip has caught up to the checked
// slot, which is exactly the epoch-boundary case being validated live. The
// fix recognizes capturedSlot <= mithrilLedgerSlot as diagnostic of that
// bootstrap gap and falls back to the pool's live registration instead.
func TestElectingVrfKeyHashResolvesBelowMithrilBootstrapAnchor(t *testing.T) {
	nonce := bytes.Repeat([]byte{0x07}, 32)
	tb := createTestBlock(t, [32]byte{56}, 56, tamperNone)
	ls, db := newEligibilityTestLedger(t, nonce)
	ls.epochCache = previewEpochs(35, 39, nonce)

	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x11}, 28))
	vrfKey := bytes.Repeat([]byte{0xB5}, 32)

	// The bootstrap's only registration row for this pool: no history below
	// it, matching a Mithril import that never saw the pool's real,
	// pre-anchor registration certificate.
	const anchorSlot = 3_200_000
	seedPoolRegistrationAtSlot(t, db, pool[:], vrfKey, anchorSlot)
	ls.mithrilLedgerSlot = anchorSlot
	ls.publishSnapshotsLocked()

	// mark(37) elects epoch 38. Its capture (3_196_799) and the resulting
	// parameter cutoff (3_110_399, the last slot of epoch 36) both precede
	// the anchor, reproducing the reported gap.
	seedPoolStakeSnapshotOfTypeAtSlot(t, db, 37,
		models.PoolStakeSnapshotTypeMark, pool[:], 1_000, 10_000, 3_196_799)

	cutoff, captured, ok, err := ls.electingPoolParamsCutoffSlot(
		tb.block, 38, pool,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Less(t, cutoff, uint64(anchorSlot),
		"the fixture must reproduce the reported gap: cutoff below the anchor")
	require.Less(t, captured, uint64(anchorSlot),
		"the fixture must reproduce the reported gap: capture below the anchor")

	gotKey, ok, err := ls.electingVrfKeyHash(tb.block, 38, pool)
	require.NoError(t, err,
		"a bootstrap-only registration below the anchor must not raise "+
			"errVrfKeyRegistrationHistoryUnavailable for a pool that has, "+
			"in fact, always been registered")
	require.True(t, ok)
	assert.Equal(t, vrfKey, gotKey.Bytes())
}

// TestElectingVrfKeyHashStillRejectsAPoolWithNoRegistrationAtAll pins the
// boundary the Mithril-anchor fix must not erase: on a Mithril-bootstrapped
// node, a
// pool with no registration row at all still raises
// errVrfKeyRegistrationHistoryUnavailable rather than silently resolving. The
// fallback reads the pool's live registration; a pool nothing was ever imported
// or registered for has none, so GetPool finds nothing and the fallback yields
// ok == false, falling through to the same error as before the fix.
func TestElectingVrfKeyHashStillRejectsAPoolWithNoRegistrationAtAll(
	t *testing.T,
) {
	nonce := bytes.Repeat([]byte{0x07}, 32)
	tb := createTestBlock(t, [32]byte{57}, 57, tamperNone)
	ls, db := newEligibilityTestLedger(t, nonce)
	ls.epochCache = previewEpochs(35, 39, nonce)
	ls.mithrilLedgerSlot = 3_200_000
	ls.publishSnapshotsLocked()

	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x11}, 28))
	// No seedPoolRegistrationAtSlot call: this pool has no registration row
	// anywhere in the database.

	seedPoolStakeSnapshotOfTypeAtSlot(t, db, 37,
		models.PoolStakeSnapshotTypeMark, pool[:], 1_000, 10_000, 3_196_799)

	_, _, err := ls.electingVrfKeyHash(tb.block, 38, pool)
	require.Error(t, err)
	assert.True(t,
		errors.Is(err, errVrfKeyRegistrationHistoryUnavailable),
		"a pool with no registration history at all must still be rejected: "+
			"got %v",
		err,
	)
}

// TestElectingVrfKeyHashDoesNotFallBackWithoutAMithrilBoundary pins the other
// half of the boundary: on a node with no Mithril bootstrap
// (mithrilLedgerSlot == 0, e.g. a genesis sync), a missing-history gap must
// still hard-reject even though the pool has a resolvable live registration.
// This is the guarantee the fix must not erase: falling back to the
// live registration whenever history merely looks incomplete reintroduces
// the VRF-rotation wedge that issue fixed. The fallback here fires only when
// mithrilLedgerSlot pins a bootstrap boundary that explains the gap.
func TestElectingVrfKeyHashDoesNotFallBackWithoutAMithrilBoundary(
	t *testing.T,
) {
	nonce := bytes.Repeat([]byte{0x07}, 32)
	tb := createTestBlock(t, [32]byte{58}, 58, tamperNone)
	ls, db := newEligibilityTestLedger(t, nonce)
	ls.epochCache = previewEpochs(35, 39, nonce)
	// mithrilLedgerSlot left at its zero value: no Mithril bootstrap.
	ls.publishSnapshotsLocked()

	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x11}, 28))
	vrfKey := bytes.Repeat([]byte{0xB5}, 32)

	// A live, resolvable registration exists, but only after both the
	// cutoff and the capture -- the same "both lookups miss" shape as the
	// bootstrap gap, reached here by a certificate genuinely arriving late
	// rather than by an import never seeing one at all.
	seedPoolRegistrationAtSlot(t, db, pool[:], vrfKey, 3_300_000)

	seedPoolStakeSnapshotOfTypeAtSlot(t, db, 37,
		models.PoolStakeSnapshotTypeMark, pool[:], 1_000, 10_000, 3_196_799)

	_, _, err := ls.electingVrfKeyHash(tb.block, 38, pool)
	require.Error(t, err,
		"without a Mithril boundary, a history gap must hard-reject rather "+
			"than fall back to the pool's live (later) registration")
	assert.True(t,
		errors.Is(err, errVrfKeyRegistrationHistoryUnavailable),
		"got %v", err,
	)
}
