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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// A member seated by an UpdateCommittee enactment keeps only the committee
// certificates it recorded in the epoch the boundary closes: at the boundary
// before, it was not in the committee, so cardano-ledger dropped its
// csCommitteeCreds entry there (Conway EPOCH, updateCommitteeState). The
// RATIFY tally at the enactment boundary sees the same state: a member with
// no committee entry is not counted, while one with a hot key that did not
// vote counts as No (committeeAcceptedRatio). Here that decides whether a
// treasury withdrawal the incumbent member voted for is ratified.
func TestEpochRolloverSeatsMemberWithOnlyItsClosingEpochAuthorization(
	t *testing.T,
) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		authSlot uint64
		hasHot   bool
	}{
		{name: "authorized before the closing epoch", authSlot: 499},
		{name: "authorized in the closing epoch", authSlot: 500, hasHot: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newTreasuryRolloverFixture(t, 100)
			require.Equal(t, uint64(500), f.currentEpoch.StartSlot)
			cold := lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.NewBlake2b224(repeatByte(28, 0xd1)),
			}
			hot := repeatByte(28, 0xd2)
			action, err := lcommon.NewUpdateCommitteeGovAction(
				nil,
				nil,
				map[*lcommon.Credential]uint64{
					&cold: f.currentEpoch.EpochId + 20,
				},
				cbor.Rat{Rat: big.NewRat(1, 1)},
			)
			require.NoError(t, err)
			actionCbor, err := cbor.Encode(action)
			require.NoError(t, err)
			ratifiedEpoch := f.currentEpoch.EpochId
			ratifiedSlot := f.currentEpoch.StartSlot + 50
			update := &models.GovernanceProposal{
				TxHash:        repeatByte(32, 0xd3),
				ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
				ProposedEpoch: f.currentEpoch.EpochId - 2,
				ExpiresEpoch:  f.currentEpoch.EpochId + 20,
				AnchorHash:    repeatByte(32, 0xd4),
				ReturnAddress: repeatByte(29, 0xd5),
				GovActionCbor: actionCbor,
				AddedSlot:     350,
				RatifiedEpoch: &ratifiedEpoch,
				RatifiedSlot:  &ratifiedSlot,
			}
			require.NoError(t, f.db.SetGovernanceProposal(update, nil))
			raw, err := dbtest.RawSQLiteMetadata(t, f.db)
			require.NoError(t, err)
			_, err = raw.Exec(`
INSERT INTO auth_committee_hot (
    cold_credential, host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?)`, cold.Credential[:], hot, 2, tc.authSlot)
			require.NoError(t, err)
			withdrawAddress, returnAddress, _ := f.rewardAddress(t, 0xd6)
			withdrawal := f.addProposal(
				t,
				0xd7,
				510,
				map[*lcommon.Address]uint64{withdrawAddress: 40},
				returnAddress,
				0,
				false,
			)

			result := f.rollover(t, f.currentEpoch, f.currentPParams)
			enacted := f.proposal(t, update)
			require.NotNil(t, enacted.EnactedSlot)

			state, err := governance.LoadCommitteeVotingState(
				f.db, nil, result.NewCurrentEpoch.EpochId,
			)
			require.NoError(t, err)
			wantActive := 1
			if tc.hasHot {
				wantActive = 2
			}
			require.Equal(t, wantActive, state.ActiveMemberCount)
			ratified := f.proposal(t, withdrawal)
			require.Equal(
				t,
				!tc.hasHot,
				ratified.RatifiedSlot != nil,
				"withdrawal ratification: %s",
				fmt.Sprint(ratified.RatifiedSlot),
			)

			lv := f.ls.NewView(nil)
			lv.epochStartSlot = result.NewCurrentEpoch.StartSlot
			member, err := lv.CommitteeHotCredentialMember(lcommon.Credential{
				CredType:   lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.NewBlake2b224(hot),
			})
			require.NoError(t, err)
			require.Equal(t, tc.hasHot, member != nil)

			members, err := f.db.GetCommitteeMembers(nil)
			require.NoError(t, err)
			var seated *models.CommitteeMember
			for _, member := range members {
				if lcommon.NewBlake2b224(
					member.ColdCredHash,
				) == cold.Credential {
					seated = member
				}
			}
			require.NotNil(t, seated)
			require.Equal(t, f.currentEpoch.StartSlot, seated.TermStartSlot)
		})
	}
}
