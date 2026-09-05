package ledger

import (
	"bytes"
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
