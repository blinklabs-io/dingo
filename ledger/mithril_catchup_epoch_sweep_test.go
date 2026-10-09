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
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestMithrilCatchUpImportDeletesStalePostAnchorEpochRolloverResidue covers
// the invariant that a Mithril catch-up import must delete not just the
// epoch row above its own anchor but the rollover residue that boundary
// wrote (reward-credit round, reward outputs, block nonces, network state)
// before replay resumes. Deleting only the epoch row is not enough: if local
// replay before the import already ran that boundary's rollover once, a
// second import followed by a second rollover of the same boundary (the
// node re-crossing it after a repeated catch-up) collides with
// the first round's surviving reward_credit_round marker and hard-fails
// saveStakeRewardOutputs's credited-round guard instead of recomputing the
// round.
//
// This drives the real rollover twice through ImportLedgerState and
// processEpochRollover, the same production entry points replay uses, and
// compares the second round's result against the first instead of only
// checking it does not error.
func TestMithrilCatchUpImportDeletesStalePostAnchorEpochRolloverResidue(
	t *testing.T,
) {
	t.Parallel()

	ls, db := newRewardCalculationTestLedger(t)
	ls.metrics.init(prometheus.NewRegistry())
	seedEligiblePreviewGoRewardBasis(t, db)

	const (
		anchorEpoch   = uint64(1397)
		boundaryEpoch = uint64(1398)
		snapshotEpoch = uint64(1395)
		epochLength   = uint(1_000)
		anchorSlot    = uint64(1_397_799)
	)
	rewardAccount := rewardCalcHash(0x72)
	member := rewardCalcHash(0x73)

	currentParams := mithrilRewardConwayPParams()
	previousParams := *currentParams
	previousParams.MinFeeA++
	currentData, err := cbor.Encode(currentParams)
	require.NoError(t, err)
	previousData, err := cbor.Encode(&previousParams)
	require.NoError(t, err)

	eraBounds := make([]ledgerstate.EraBound, ledgerstate.EraConway+1)
	nonce := make([]byte, 32)

	// importAndRollOver drives the two real production entry points a
	// catch-up import plus replay use: ImportLedgerState (the
	// code under test, which must sweep the prior round's residue above the
	// anchor) and processEpochRollover (the real boundary trigger
	// ledgerProcessBlocks calls once a replayed block's slot crosses
	// currentEpoch.StartSlot+LengthInSlots). Called twice, it models local
	// replay re-crossing the same boundary after a second catch-up
	// import -- a repeated import -- the scenario an
	// epoch-only sweep could not survive.
	importAndRollOver := func() *EpochRolloverResult {
		t.Helper()
		require.NoError(t, ledgerstate.ImportLedgerState(
			context.Background(),
			ledgerstate.ImportConfig{
				Database: db,
				Logger: slog.New(
					slog.NewTextHandler(io.Discard, nil),
				),
				State: &ledgerstate.RawLedgerState{
					PParamsData:         currentData,
					PrevPParamsData:     previousData,
					Epoch:               anchorEpoch,
					EraIndex:            ledgerstate.EraConway,
					EraBounds:           eraBounds,
					EpochNonce:          nonce,
					EvolvingNonce:       nonce,
					CandidateNonce:      nonce,
					LastEpochBlockNonce: nonce,
					Reserves:            100_000_000,
					Tip: &ledgerstate.SnapshotTip{
						Slot:      anchorSlot,
						BlockHash: make([]byte, 32),
					},
				},
				EpochLength: func(uint) (uint, uint, error) {
					return 1, epochLength, nil
				},
			},
		))
		require.NoError(t, db.Metadata().SaveRewardAdaPots(
			&models.RewardAdaPots{
				Epoch:        anchorEpoch,
				Reserves:     100_000_000,
				CapturedSlot: anchorSlot,
			},
			nil,
		))

		epochs, err := db.GetEpochs(nil)
		require.NoError(t, err)
		require.NotEmpty(t, epochs)
		last := epochs[len(epochs)-1]
		require.Equal(t, anchorEpoch, last.EpochId,
			"a prior round's post-anchor epoch/rollover residue must be "+
				"swept before replay resumes")

		// Load the epoch cache the way startup does, through the real
		// caller of setEpochCache -- not by hand-assigning
		// ls.currentEpoch, which would make the rollover trigger below
		// true regardless of what the import actually swept.
		loadTxn := db.Transaction(context.Background(), true)
		require.NoError(t, loadTxn.Do(func(txn *database.Txn) error {
			return ls.loadEpochs(context.Background(), txn)
		}))
		require.Equal(t, anchorEpoch, ls.currentEpoch.EpochId,
			"current-epoch pointer must reflect the anchor epoch, not a "+
				"stale post-anchor row")
		ls.currentPParams = currentParams

		var rollover *EpochRolloverResult
		rolloverTxn := db.Transaction(context.Background(), true)
		require.NoError(t, rolloverTxn.Do(func(txn *database.Txn) error {
			var rolloverErr error
			rollover, rolloverErr = ls.processEpochRollover(
				context.Background(),
				txn,
				ls.currentEpoch,
				ls.currentEra,
				ls.currentPParams,
			)
			return rolloverErr
		}))
		require.NotNil(t, rollover)
		require.Equal(t, boundaryEpoch, rollover.NewCurrentEpoch.EpochId,
			"the boundary rollover must actually run and advance to E+1")
		return rollover
	}

	effectiveBalance := func(credentialTag uint8, stakingKey []byte) uint64 {
		t.Helper()
		account, err := db.GetAccountByCredential(
			context.Background(),
			credentialTag, stakingKey, true, nil,
		)
		require.NoError(t, err)
		pending, err := ls.PendingRewardCredit(nil, credentialTag, stakingKey)
		require.NoError(t, err)
		total, overflow := addRewardUint64(uint64(account.Reward), pending)
		require.False(t, overflow, "effective balance overflow")
		return total
	}

	creditedRoundCount := func(epoch uint64) int {
		t.Helper()
		rounds, err := db.Metadata().GetPendingRewardCreditRounds(nil)
		require.NoError(t, err)
		count := 0
		for _, round := range rounds {
			if round.SnapshotEpoch == epoch {
				count++
			}
		}
		return count
	}

	// Round 0: the boundary's reward distribution actually happens. Without
	// a working sweep at all, current-epoch would already read E+1
	// immediately after import and this rollover call -- crediting pool/
	// account outputs recorded against epoch snapshot 1395 -- would never
	// be reached by the real pipeline.
	importAndRollOver()

	poolOutputsRound0, err := db.Metadata().GetRewardPoolOutputs(
		snapshotEpoch, nil,
	)
	require.NoError(t, err)
	require.Len(t, poolOutputsRound0, 1)
	require.Positive(t, uint64(poolOutputsRound0[0].TotalReward))
	require.Equal(t, 1, creditedRoundCount(snapshotEpoch),
		"round 0 must register exactly one credited round for the "+
			"snapshot epoch")

	// member's delegated stake is too small a share of this fixture's pot
	// to round to a nonzero member reward; only the pool's leader reward
	// (credited to rewardAccount) is nonzero. Both are still checked for
	// double-crediting below: a regression that leaks residue into round 1
	// could just as easily turn member's untouched zero into something
	// nonzero as it could double rewardAccount's credit.
	rewardAccountBalanceRound0 := effectiveBalance(0, rewardAccount)
	memberBalanceRound0 := effectiveBalance(0, member)
	require.Positive(t, rewardAccountBalanceRound0)
	require.Zero(t, memberBalanceRound0)

	// Round 1: a second catch-up import re-crosses the same
	// boundary. The widened sweep must clear round 0's reward-credit round
	// and reward-state residue above the anchor so this rollover recomputes
	// cleanly instead of hitting saveStakeRewardOutputs's credited-round
	// guard.
	importAndRollOver()

	require.Equal(t, 1, creditedRoundCount(snapshotEpoch),
		"round 1 must leave exactly one credited round for the snapshot "+
			"epoch, not accumulate a second one")

	poolOutputsRound1, err := db.Metadata().GetRewardPoolOutputs(
		snapshotEpoch, nil,
	)
	require.NoError(t, err)
	require.Len(t, poolOutputsRound1, 1)
	require.Equal(t,
		poolOutputsRound0[0].TotalReward, poolOutputsRound1[0].TotalReward,
		"round 1 must recompute the same pool reward amount, not double it",
	)

	require.Equal(t,
		rewardAccountBalanceRound0, effectiveBalance(0, rewardAccount),
		"round 1 must leave the reward account's effective balance "+
			"unchanged, not double-credit it",
	)
	require.Equal(t,
		memberBalanceRound0, effectiveBalance(0, member),
		"round 1 must leave the member account's effective balance "+
			"unchanged, not double-credit it",
	)
}
