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
			gotCutoff, ok, err := ls.electingPoolParamsCutoffSlot(
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
