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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessEpochRatifiedParameterChangeBlocksCompetingSibling pins the
// purpose-root half of ParameterChange being non-delaying: after one action
// is accepted, a competing action with the same parent no longer matches the
// staged root and must not ratify in the same pass.
func TestProcessEpochRatifiedParameterChangeBlocksCompetingSibling(
	t *testing.T,
) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
	first := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x51), nil,
		400, 0, testBytes(29, 0), chainTestParameterChange(t, 61),
	)
	competing := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x50), nil,
		500, 0, testBytes(29, 0), chainTestParameterChange(t, 62),
	)
	stored := chainTestStore(t, db, first, competing)
	seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

	out := chainTestRunEpoch(t, db, stabilityTestEpoch, chainTestPParams())
	assert.Equal(t, 1, out.RatifiedCount)
	assert.NotNil(t, chainTestReload(t, db, first).RatifiedEpoch)
	assert.Nil(t, chainTestReload(t, db, competing).RatifiedEpoch)
}

// TestProcessEpochStagedCommitteeMinSizeBlocksTreasuryWithdrawal is the
// CommitteeMinSize/TreasuryWithdrawal regression: a ratified ParameterChange
// raising CommitteeMinSize above the active committee is staged before the
// lower-priority TreasuryWithdrawal is evaluated, so the withdrawal no longer
// has committee approval, whichever action was submitted first.
func TestProcessEpochStagedCommitteeMinSizeBlocksTreasuryWithdrawal(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name            string
		withParameter   bool
		treasuryFirst   bool
		wantRatified    int
		wantWithdrawal  bool
		wantParamChange bool
	}{
		{
			name:           "control without parameter change",
			wantRatified:   1,
			wantWithdrawal: true,
		},
		{
			name:            "withdrawal submitted first",
			withParameter:   true,
			treasuryFirst:   true,
			wantRatified:    1,
			wantParamChange: true,
		},
		{
			name:            "parameter change submitted first",
			withParameter:   true,
			wantRatified:    1,
			wantParamChange: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db, store := newTallyTestDB(t)
			require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
			withdrawalAddr, err := lcommon.NewAddressFromBytes(
				buildRewardAddr(t, testBytes(28, 0xE5)),
			)
			require.NoError(t, err)
			withdrawalCbor, err := cbor.Encode(
				&lcommon.TreasuryWithdrawalGovAction{
					Type: uint(lcommon.GovActionTypeTreasuryWithdrawal),
					Withdrawals: map[*lcommon.Address]uint64{
						&withdrawalAddr: 1,
					},
				},
			)
			require.NoError(t, err)
			minCommitteeSize := uint(2)
			parameterCbor, err := cbor.Encode(
				&conway.ConwayParameterChangeGovAction{
					Type: uint(lcommon.GovActionTypeParameterChange),
					ParamUpdate: conway.ConwayProtocolParameterUpdate{
						MinCommitteeSize: &minCommitteeSize,
					},
				},
			)
			require.NoError(t, err)
			treasurySlot, parameterSlot := uint64(200), uint64(100)
			if test.treasuryFirst {
				treasurySlot, parameterSlot = parameterSlot, treasurySlot
			}
			withdrawalProposal := chainTestProposal(
				lcommon.GovActionTypeTreasuryWithdrawal,
				testBytes(32, 0xE1), nil, treasurySlot, 0, testBytes(29, 0),
				withdrawalCbor,
			)
			proposals := []*models.GovernanceProposal{withdrawalProposal}
			parameterProposal := chainTestProposal(
				lcommon.GovActionTypeParameterChange,
				testBytes(32, 0xE2), nil, parameterSlot, 0, testBytes(29, 0),
				parameterCbor,
			)
			if test.withParameter {
				proposals = append(proposals, parameterProposal)
			}
			stored := chainTestStore(t, db, proposals...)
			seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

			out := chainTestRunEpoch(
				t, db, stabilityTestEpoch, chainTestPParams(),
			)
			assert.Equal(t, test.wantRatified, out.RatifiedCount)
			assert.Equal(
				t,
				test.wantWithdrawal,
				chainTestReload(t, db, withdrawalProposal).RatifiedEpoch != nil,
			)
			if test.withParameter {
				assert.Equal(
					t,
					test.wantParamChange,
					chainTestReload(t, db, parameterProposal).
						RatifiedEpoch != nil,
				)
			}
		})
	}
}

// TestProcessEpochImportedParameterChangeChain covers imported ledger state:
// a Mithril-seeded purpose root (the synthetic enacted row
// ledgerstate.seedPrevGovActionIds writes), and imported proposals that share
// their epoch anchor slot so that only the hash separates them. Pending
// imported proposals ratify parent then child in one pass; proposals the
// snapshot already carried as ratified enact parent then child.
func TestProcessEpochImportedParameterChangeChain(t *testing.T) {
	t.Parallel()

	for _, importedRatified := range []bool{false, true} {
		name := "pending"
		if importedRatified {
			name = "ratified"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db, store := newTallyTestDB(t)
			require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
			importEpoch := stabilityTestEpoch - 1
			importSlot := importEpoch * 100
			seededRoot := &models.GovernanceProposal{
				TxHash:        testBytes(32, 0xF0),
				ActionType:    uint8(lcommon.GovActionTypeParameterChange),
				EnactedEpoch:  &importEpoch,
				EnactedSlot:   &importSlot,
				ReturnAddress: make([]byte, 29),
				AnchorHash:    make([]byte, 32),
			}
			require.NoError(t, db.SetGovernanceProposal(seededRoot, nil))
			anchorSlot := (stabilityTestEpoch - 2) * 100
			parent := chainTestProposal(
				lcommon.GovActionTypeParameterChange, testBytes(32, 0xF2),
				seededRoot, anchorSlot, 0, testBytes(29, 0),
				chainTestParameterChange(t, 61),
			)
			child := chainTestProposal(
				lcommon.GovActionTypeParameterChange, testBytes(32, 0xF1),
				parent, anchorSlot, 0, testBytes(29, 0),
				chainTestParameterChange(t, 62),
			)
			enactEpoch := stabilityTestEpoch + 1
			if importedRatified {
				for _, proposal := range []*models.GovernanceProposal{
					parent, child,
				} {
					proposal.RatifiedEpoch = &importEpoch
					proposal.RatifiedSlot = &importSlot
				}
				enactEpoch = stabilityTestEpoch
			}
			stored := chainTestStore(t, db, child, parent)
			seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)
			pparams := chainTestPParams()

			if !importedRatified {
				out := chainTestRunEpoch(t, db, stabilityTestEpoch, pparams)
				assert.Equal(t, 2, out.RatifiedCount)
			}
			enactOut := chainTestRunEpoch(t, db, enactEpoch, pparams)
			assert.Equal(t, 2, enactOut.EnactedCount)
			assert.Equal(
				t,
				newRat(62, 100),
				chainTestMotionNoConfidence(t, enactOut.UpdatedPParams),
			)
			root := chainTestParameterRoot(t, db)
			require.NotNil(t, root)
			assert.Equal(t, child.TxHash, root.TxHash)
		})
	}
}
