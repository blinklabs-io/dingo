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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// Committee pruning is applied when committee state is read, not by deleting
// rows, so rolling an enactment back restores the pre-boundary answers
// exactly: the seated member is pending again, and its authorization from
// the restored epoch counts again.
func TestCommitteeEnactmentRollbackRestoresPendingAuthorization(t *testing.T) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	cold := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(repeatByte(28, 0xe1)),
	}
	hot := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(repeatByte(28, 0xe2)),
	}
	action, err := lcommon.NewUpdateCommitteeGovAction(
		nil,
		nil,
		map[*lcommon.Credential]uint64{&cold: f.currentEpoch.EpochId + 20},
		cbor.Rat{Rat: big.NewRat(1, 1)},
	)
	require.NoError(t, err)
	actionCbor, err := cbor.Encode(action)
	require.NoError(t, err)
	ratifiedEpoch := f.currentEpoch.EpochId
	ratifiedSlot := f.currentEpoch.StartSlot + 50
	update := &models.GovernanceProposal{
		TxHash:        repeatByte(32, 0xe3),
		ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
		ProposedEpoch: f.currentEpoch.EpochId - 2,
		ExpiresEpoch:  f.currentEpoch.EpochId + 20,
		AnchorHash:    repeatByte(32, 0xe4),
		ReturnAddress: repeatByte(29, 0xe5),
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
) VALUES (?, ?, ?, ?)`, cold.Credential[:], hot.Credential[:], 2, 520)
	require.NoError(t, err)

	type committeeAnswers struct {
		hotKnown bool
		colds    []lcommon.Credential
		elected  bool
		resigned bool
	}
	answers := func(epoch, epochStartSlot uint64) committeeAnswers {
		t.Helper()
		lv := f.ls.NewView(nil).pinCommitteeState(epoch, f.currentPParams)
		lv.epochStartSlot = epochStartSlot
		member, err := lv.CommitteeHotCredentialMember(hot)
		require.NoError(t, err)
		colds, err := lv.CommitteeHotCredentialColdCredentials(hot)
		require.NoError(t, err)
		elected, err := lv.CommitteeCredentialIsElected(cold)
		require.NoError(t, err)
		coldMember, err := lv.CommitteeCredentialMember(cold)
		require.NoError(t, err)
		require.NotNil(t, coldMember)
		return committeeAnswers{
			hotKnown: member != nil,
			colds:    colds,
			elected:  elected,
			resigned: coldMember.Resigned,
		}
	}
	before := answers(f.currentEpoch.EpochId, f.currentEpoch.StartSlot)
	require.Equal(t, committeeAnswers{
		hotKnown: true,
		colds:    []lcommon.Credential{cold},
	}, before)

	result := f.rollover(t, f.currentEpoch, f.currentPParams)
	require.Equal(t, committeeAnswers{
		hotKnown: true,
		colds:    []lcommon.Credential{cold},
		elected:  true,
	}, answers(
		result.NewCurrentEpoch.EpochId,
		result.NewCurrentEpoch.StartSlot,
	))

	boundary := result.NewCurrentEpoch.StartSlot
	require.NoError(t, f.db.DeleteCommitteeMembersAfterSlot(boundary-1, nil))
	require.NoError(t, f.db.DeleteGovernanceProposalsAfterSlot(boundary-1, nil))
	require.Equal(
		t,
		before,
		answers(f.currentEpoch.EpochId, f.currentEpoch.StartSlot),
	)
	// Without the rollback, the same authorization would not survive into the
	// next epoch for a credential that stayed pending.
	require.Equal(t, committeeAnswers{
		colds: []lcommon.Credential{},
	}, answers(result.NewCurrentEpoch.EpochId, boundary))
}
