package ledger

import (
	"bytes"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// TestElectingVrfKeyHashLagsPoolParamsByOneEpoch is the dingo #3842 regression,
// built from the rotation that wedged a Preview replay twice.
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
	assert.Equal(t, newKey, gotKey.Bytes(),
		"epoch 40 is elected by parameters in force through the end of epoch 37")
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
		{EpochId: 38, StartSlot: 3_283_200, LengthInSlots: 172_800, Nonce: nonce},
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
		{EpochId: 36, StartSlot: 2_900_000, LengthInSlots: 200_000, Nonce: nonce},
		{EpochId: 37, StartSlot: 3_100_000, LengthInSlots: 200_000, Nonce: nonce},
		{EpochId: 38, StartSlot: 3_300_000, LengthInSlots: 200_000, Nonce: nonce},
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

// TestElectingVrfKeyHashResolvesBelowMithrilBootstrapAnchor is the dingo #4047
// regression: a Mithril-bootstrapped node wedges at its first epoch boundary
// because the pool's only registration row is stamped at the bootstrap
// anchor slot, which is later than the parameter cutoff and stake-snapshot
// capture slots the electing snapshot resolves against.
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
// boundary the #4047 fix must not erase: on a Mithril-bootstrapped node, a
// pool with no registration row at all still raises
// errVrfKeyRegistrationHistoryUnavailable rather than silently resolving.
// The fallback reads the pool's live registration; a pool nothing was ever
// imported or registered for has none, so GetPool finds nothing and the
// fallback yields ok == false, falling through to the same error as before
// the fix.
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
// This is the #3842 guarantee the fix must not erase: falling back to the
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
