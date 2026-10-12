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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// GOVCERT rejects an AuthCommitteeHot from a cold credential whose
// csCommitteeCreds entry is CommitteeMemberResigned
// (checkAndOverwriteCommitteeMemberState, ConwayCommitteeHasPreviouslyResigned).
// A potential future member's entry lasts until the next epoch boundary
// (Conway EPOCH, updateCommitteeState), while a seated member's entry
// survives every boundary, so a seated member that resigned stays resigned
// even when a pending proposal would re-elect it.
func TestValidateTxCommitteeAuthorizationAfterResignation(t *testing.T) {
	t.Parallel()

	const epochStartSlot = 100
	tests := []struct {
		name       string
		seated     bool
		resignSlot uint64
		rejected   bool
	}{
		{
			name:       "pending member resigned this epoch",
			resignSlot: epochStartSlot + 1,
			rejected:   true,
		},
		{
			name:       "pending member resigned last epoch",
			resignSlot: epochStartSlot - 1,
		},
		{
			name:       "seated member resigned with a pending re-election",
			seated:     true,
			resignSlot: epochStartSlot - 1,
			rejected:   true,
		},
	}
	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		for _, test := range tests {
			t.Run(era.name+"/"+test.name, func(t *testing.T) {
				t.Parallel()
				pparams := era.pparams(lcommon.ProtocolVersionPlomin)
				lv, db := committeeTestView(t, pparams)
				lv.epochStartSlot = epochStartSlot
				cold, coldKey := committeeTestVotingKey(0x61)
				if test.seated {
					seatCommitteeMembers(t, db, cold)
				} else {
					seatCommitteeMembers(t, db, committeeTestCredential(0x62))
				}
				storeCommitteeUpdateProposal(t, db, 0x63, cold, 10)
				seedCommitteeCredentialResignation(
					t,
					db,
					cold,
					1,
					test.resignSlot,
				)
				_, paymentKey := committeeTestVotingKey(0x64)

				err := committeeVotingValidate(
					t, era, lv, pparams, paymentKey,
					nil,
					[]lcommon.Certificate{authorizeHotCertificate(
						cold,
						committeeTestCredential(0x65),
					)},
					coldKey,
				)
				if test.rejected {
					var resigned conway.ResignedCommitteeMemberHotKeyError
					require.ErrorAs(t, err, &resigned)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

// GOVCERT admits a potential future member when any pending committee
// proposal names it among its new members (isPotentialFutureMember), so a
// newer pending proposal that would remove the credential does not cancel an
// older one that adds it.
func TestValidateTxCommitteeAuthorizationByMemberOfAnyPendingProposal(
	t *testing.T,
) {
	t.Parallel()

	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		t.Run(era.name, func(t *testing.T) {
			t.Parallel()
			pparams := era.pparams(lcommon.ProtocolVersionPlomin)
			lv, db := committeeTestView(t, pparams)
			seatCommitteeMembers(t, db, committeeTestCredential(0x71))
			cold, coldKey := committeeTestVotingKey(0x72)
			storeCommitteeUpdateProposal(t, db, 0x73, cold, 10)
			removal, err := lcommon.NewUpdateCommitteeGovAction(
				nil,
				[]lcommon.Credential{cold},
				nil,
				cbor.Rat{Rat: big.NewRat(2, 3)},
			)
			require.NoError(t, err)
			encoded, err := cbor.Encode(removal)
			require.NoError(t, err)
			require.NoError(
				t,
				db.SetGovernanceProposal(context.Background(), &models.GovernanceProposal{
					TxHash:        governanceTestHash(0x74),
					ActionType:    uint8(lcommon.GovActionTypeUpdateCommittee),
					ExpiresEpoch:  100,
					AnchorHash:    make([]byte, 32),
					ReturnAddress: make([]byte, 29),
					GovActionCbor: encoded,
					AddedSlot:     5,
				}, nil),
			)
			_, paymentKey := committeeTestVotingKey(0x75)

			err = committeeVotingValidate(
				t, era, lv, pparams, paymentKey,
				nil,
				[]lcommon.Certificate{authorizeHotCertificate(
					cold,
					committeeTestCredential(0x76),
				)},
				coldKey,
			)
			require.NoError(t, err)
		})
	}
}
