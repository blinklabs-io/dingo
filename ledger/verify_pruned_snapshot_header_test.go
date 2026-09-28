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

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// prunedSnapshotFixture builds a ledger that has advanced to epoch 8 while the
// block's mark snapshot (epoch 4) has been pruned by the default 3-epoch
// pool-snapshot retention window, the way cleanupOldSnapshots leaves it.
func prunedSnapshotFixture(
	t *testing.T,
	tb *testBlockResult,
	withSummary bool,
	onChain bool,
) *LedgerState {
	t.Helper()
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	pool := tb.block.IssuerVkey().Hash()
	seedPoolStakeSnapshot(t, db, 4, pool[:], 1_000_000_000)
	if withSummary {
		require.NoError(t, db.Metadata().SaveEpochSummary(&models.EpochSummary{
			Epoch:            4,
			TotalActiveStake: types.Uint64(1_000_000_000),
			TotalPoolCount:   1,
			SnapshotReady:    true,
		}, nil))
	}
	seedBlockPoolRegistration(t, db, tb.block)
	require.NoError(
		t,
		db.Metadata().DeletePoolStakeSnapshotsBeforeEpoch(5, nil),
	)

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	hash := tb.block.Header().Hash().Bytes()
	if !onChain {
		// A different block at the same slot: the header is on a fork.
		hash = append([]byte{0xff}, hash[1:]...)
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{{
		Slot:        tb.block.SlotNumber(),
		Hash:        hash,
		BlockNumber: 1,
		Type:        1,
		Cbor:        []byte{0x80},
	}}))
	ls.chain = cm.PrimaryChain()

	ls.currentEpoch = models.Epoch{EpochId: 8}
	ls.currentTip = ochainsync.Tip{Point: ocommon.Point{
		Slot: tb.block.SlotNumber() + 1_000,
	}}
	ls.publishSnapshotsLocked()
	return ls
}

// A header the node already applied must not be re-judged against pool
// snapshots the retention window has since pruned.
func TestValidateChainSelectionHeaderCryptoAppliedHeaderSurvivesPruning(
	t *testing.T,
) {
	t.Parallel()
	for _, withSummary := range []bool{false, true} {
		name := "no-summary"
		if withSummary {
			name = "summary"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tb := createTestBlock(t, [32]byte{30}, 0, tamperNone)
			ls := prunedSnapshotFixture(t, tb, withSummary, true)
			err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
			assert.NoError(t, err)
		})
	}
}

// A header on a fork is still verified, but pruned history is "cannot
// evaluate" (deferred), not proof the pool is absent.
func TestValidateChainSelectionHeaderCryptoForkHeaderPrunedSnapshotDefers(
	t *testing.T,
) {
	t.Parallel()
	for _, withSummary := range []bool{false, true} {
		name := "no-summary"
		if withSummary {
			name = "summary"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tb := createTestBlock(t, [32]byte{31}, 0, tamperNone)
			ls := prunedSnapshotFixture(t, tb, withSummary, false)
			err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
			require.Error(t, err)
			assert.True(
				t,
				IsHeaderVerificationDeferred(err),
				"pruned snapshot must defer, not reject: %v",
				err,
			)
		})
	}
}

// Header crypto is still checked for a header that is not on the chain, even
// when its stake snapshot is pruned.
func TestValidateChainSelectionHeaderCryptoForkHeaderStillVerified(
	t *testing.T,
) {
	t.Parallel()
	tb := createTestBlock(t, [32]byte{32}, 0, tamperVRFProof)
	ls := prunedSnapshotFixture(t, tb, true, false)
	err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
	require.Error(t, err)
	assert.False(t, IsHeaderVerificationDeferred(err), "%v", err)
}

// A pool absent from a populated, unpruned snapshot is still a hard
// rejection.
func TestValidateChainSelectionHeaderCryptoPoolAbsentFromPopulatedSnapshotRejects(
	t *testing.T,
) {
	t.Parallel()
	tb := createTestBlock(t, [32]byte{33}, 0, tamperNone)
	ls := prunedSnapshotFixture(t, tb, true, false)
	other := make([]byte, 28)
	other[0] = 0xee
	seedPoolStakeSnapshot(t, ls.db, 4, other, 1_000_000_000)
	err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
	require.Error(t, err)
	assert.False(t, IsHeaderVerificationDeferred(err), "%v", err)
	assert.Contains(t, err.Error(), "has no stake in epoch 4 snapshot")
}
