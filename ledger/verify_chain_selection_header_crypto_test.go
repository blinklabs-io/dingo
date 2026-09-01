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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidateChainSelectionHeaderCryptoAcceptsVerifiedHeader proves that a
// header whose crypto is valid and whose leader eligibility can already be
// checked against local ledger state passes with no error -- the baseline
// "fully verified" case chain selection must count toward Genesis density.
func TestValidateChainSelectionHeaderCryptoAcceptsVerifiedHeader(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{70}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)
	seedBlockPoolRegistration(t, db, tb.block)
	poolKeyHash := tb.block.IssuerVkey().Hash()
	// Pool owns 100% of stake, matching createTestBlock's threshold
	// assumption, at the epoch-5 block's "mark" snapshot (epoch 4).
	seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)
	ls.publishSnapshotsLocked()

	err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
	require.NoError(
		t,
		err,
		"a header with valid crypto and confirmed leader eligibility must verify",
	)
}

// TestValidateChainSelectionHeaderCryptoRejectsTamperedProof proves that a
// header with an internally-invalid VRF proof is a definite (non-deferred)
// failure, even while local ledger state has not caught up to the header's
// slot. An invalid header must never be counted toward Genesis density
// regardless of local sync state (dingo #3517).
func TestValidateChainSelectionHeaderCryptoRejectsTamperedProof(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{71}, 0, tamperVRFProof)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	// Behind the block's slot, same as the deferred-eligibility fixtures
	// below -- proves a real crypto failure is not masked by state-defer
	// tolerance.
	ls.currentTip.Point.Slot = tb.block.SlotNumber() - 1
	ls.publishSnapshotsLocked()

	err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
	require.Error(t, err)
	assert.False(
		t,
		IsHeaderVerificationDeferred(err),
		"a tampered VRF proof must be a definite failure, not deferred",
	)
}

// TestValidateChainSelectionHeaderCryptoDefersAheadOfLocalState proves that a
// header this node cannot yet confirm leader eligibility for -- because local
// ledger application has not reached its slot -- is reported as deferred, not
// rejected. This is the fast-sync/Genesis-bootstrap case the fix must
// preserve: an honest peer legitimately racing ahead of local ledger apply
// must still be eligible for chain-selection density.
func TestValidateChainSelectionHeaderCryptoDefersAheadOfLocalState(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{72}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.currentTip.Point.Slot = tb.block.SlotNumber() - 1
	ls.publishSnapshotsLocked()

	err := ls.ValidateChainSelectionHeaderCrypto(tb.block.Header())
	require.Error(t, err)
	assert.True(
		t,
		IsHeaderVerificationDeferred(err),
		"a header ahead of local ledger state must defer, not fail closed",
	)
}

// TestValidateChainSelectionHeaderCryptoDoesNotAdvanceEpochCache proves that,
// like ValidateBlockHeaderCrypto, verifying a header for chain selection
// never mutates the shared epoch cache -- an unauthenticated peer header must
// not be able to influence shared ledger state as a side effect of being
// observed for density.
func TestValidateChainSelectionHeaderCryptoDoesNotAdvanceEpochCache(
	t *testing.T,
) {
	const futureSlot = uint64(1001)
	ls := &LedgerState{
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(500, []byte("tip"))},
		epochCache: []models.Epoch{{
			EpochId:       500,
			StartSlot:     0,
			SlotLength:    1_000,
			LengthInSlots: 1_000,
			EraId:         eras.ConwayEraDesc.Id,
			Nonce:         []byte{0x01},
		}},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	err := ls.ValidateChainSelectionHeaderCrypto(
		&mockBabbageBlock{slot: futureSlot},
	)
	require.Error(t, err)
	assert.Len(
		t,
		ls.loadConsensusSnapshot().epochCache,
		1,
		"chain-selection header validation must not advance the shared epoch cache",
	)
}

// TestShouldVerifyChainSelectionHeaderCryptoMatchesChainsyncGate proves that
// ShouldVerifyChainSelectionHeaderCrypto shares the same fast-sync/Mithril
// exemptions as the ledger's own chainsync header-queue gate
// (shouldVerifyChainsyncHeaderCrypto), so a competing peer's header is exempt
// under exactly the same conditions the applied chain already is.
func TestShouldVerifyChainSelectionHeaderCryptoMatchesChainsyncGate(
	t *testing.T,
) {
	tb := createTestBlock(t, [32]byte{73}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	slot := tb.block.SlotNumber()

	assert.False(
		t,
		ls.ShouldVerifyChainSelectionHeaderCrypto(slot),
		"verification must be skipped before live validation is enabled, "+
			"matching shouldVerifyChainsyncHeaderCrypto",
	)

	ls.validationEnabled = true
	ls.publishSnapshotsLocked()
	assert.True(
		t,
		ls.ShouldVerifyChainSelectionHeaderCrypto(slot),
		"verification must run once live validation is enabled and the "+
			"epoch nonce is cached",
	)

	ls.mithrilLedgerSlot = slot
	ls.publishSnapshotsLocked()
	assert.False(
		t,
		ls.ShouldVerifyChainSelectionHeaderCrypto(slot),
		"a Mithril-covered slot must be exempt, matching the applied-chain gate",
	)
}
