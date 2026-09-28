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

package governance

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// noConfidencePParams returns post-bootstrap (major 10) Conway pparams with
// a realistic SPO MotionNoConfidence threshold (0.51, Preview's real value)
// and the DRep MotionNoConfidence threshold zeroed so DRep approval is
// automatic -- isolating the assertions below to the SPO gate that
// stakeEpochFor's same-boundary fix affects. NoConfidence needs no CC
// approval (needsCCApproval returns false for it), so no committee fixture
// is needed either.
func noConfidencePParams() *conway.ConwayProtocolParameters {
	p := &conway.ConwayProtocolParameters{}
	p.ProtocolVersion.Major = 10
	p.PoolVotingThresholds.MotionNoConfidence = newRat(51, 100)
	return p
}

func seedNoConfidenceProposal(
	t *testing.T,
	db *database.Database,
	proposedEpoch, expiresEpoch uint64,
) *models.GovernanceProposal {
	t.Helper()
	actionCbor, err := cbor.Encode(&lcommon.NoConfidenceGovAction{
		Type: uint(lcommon.GovActionTypeNoConfidence),
	})
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, 0x60),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeNoConfidence),
		ProposedEpoch: proposedEpoch,
		ExpiresEpoch:  expiresEpoch,
		AnchorURL:     "https://example.invalid/no-confidence-ratify",
		AnchorHash:    testBytes(32, 0x61),
		ReturnAddress: testBytes(29, 0x62),
		GovActionCbor: actionCbor,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	loaded, err := db.GetGovernanceProposal(proposal.TxHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	return loaded
}

// TestProcessEpoch_NoConfidence_SPOThresholdUsesSameBoundaryMark covers the
// second SPO-gated action type dingo#4441's fix touches (HardForkInitiation
// being the first, covered by ledger's TestHardForkInitiation_* tests).
// stakeEpochFor(newEpoch) resolves to newEpoch, so the SPO tally must read
// mark[newEpoch] -- seeded here directly, matching how a standalone
// ProcessEpoch caller (as opposed to a real epoch-rollover transaction)
// seeds it. The 0.51 threshold and 0.4779/0.6283 stake ratios are the same
// real values dingo#4441 measured on Preview for HardForkInitiation; using
// them here for NoConfidence proves the fix is the shared stakeEpochFor
// primitive, not a HardForkInitiation-specific patch.
func TestProcessEpoch_NoConfidence_SPOThresholdUsesSameBoundaryMark(
	t *testing.T,
) {
	t.Parallel()

	const newEpoch = uint64(742)

	tests := []struct {
		name       string
		yesStake   uint64
		wantRatify bool
	}{
		{"below threshold", 4_779, false}, // ratio 0.4779 < 0.51
		{"above threshold", 6_283, true},  // ratio 0.6283 >= 0.51
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, store := newTallyTestDB(t)
			proposal := seedNoConfidenceProposal(t, db, newEpoch-5, newEpoch+10)

			yesPool := testBytes(28, 0x70)
			silentPool := testBytes(28, 0x71)
			seedPoolWithStake(
				t, store, yesPool, testBytes(29, 0x72), tt.yesStake, newEpoch,
			)
			seedPoolWithStake(
				t, store, silentPool, testBytes(29, 0x73),
				10_000-tt.yesStake, newEpoch,
			)
			require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
				ProposalID:      proposal.ID,
				VoterType:       models.VoterTypeSPO,
				VoterCredential: yesPool,
				Vote:            models.VoteYes,
				AddedSlot:       2,
			}, nil))

			txn := db.MetadataTxn(true)
			defer txn.Release()
			out, err := ProcessEpoch(&EpochInput{
				DB:           db,
				Txn:          txn,
				PrevEpoch:    newEpoch - 1,
				NewEpoch:     newEpoch,
				BoundarySlot: newEpoch * 100,
				PParams:      noConfidencePParams(),
				UpdateFn: func(
					pparams lcommon.ProtocolParameters,
					_ any,
				) (lcommon.ProtocolParameters, error) {
					return pparams, nil
				},
			})
			require.NoError(t, err)
			require.NoError(t, txn.Commit())

			reloaded, err := db.GetGovernanceProposal(
				proposal.TxHash, proposal.ActionIndex, nil,
			)
			require.NoError(t, err)
			if tt.wantRatify {
				require.Equal(t, 1, out.RatifiedCount)
				require.NotNil(t, reloaded.RatifiedEpoch)
				require.Equal(t, newEpoch, *reloaded.RatifiedEpoch)
			} else {
				require.Equal(t, 0, out.RatifiedCount)
				require.Nil(t, reloaded.RatifiedEpoch)
			}
		})
	}
}
