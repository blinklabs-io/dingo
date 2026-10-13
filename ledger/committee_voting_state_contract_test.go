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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// A committee hot credential is compared with its key/script tag on both
// the seated and the unseated authorization paths: a key-hash voter is not
// admitted by a script-hash authorization with the same bytes.
func TestValidateTxCommitteeHotCredentialTagByAuthorizationPath(t *testing.T) {
	t.Parallel()

	const epochStartSlot = 100
	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		for _, seated := range []bool{true, false} {
			for _, scriptAuthorization := range []bool{false, true} {
				name := era.name + "/pending"
				if seated {
					name = era.name + "/seated"
				}
				if scriptAuthorization {
					name += "/script-hash authorization"
				} else {
					name += "/key-hash authorization"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					pparams := era.pparams(lcommon.ProtocolVersionPlomin)
					lv, db := committeeTestView(t, pparams)
					lv.epochStartSlot = epochStartSlot
					cold := committeeTestCredential(0x61)
					voter, voterKey := committeeTestVotingKey(0x62)
					authorized := voter
					if scriptAuthorization {
						authorized = lcommon.Credential{
							CredType:   lcommon.CredentialTypeScriptHash,
							Credential: voter.Credential,
						}
					}
					if seated {
						seatCommitteeMembers(t, db, cold)
					} else {
						seatCommitteeMembers(t, db, committeeTestCredential(0x63))
						storeCommitteeUpdateProposal(t, db, 0x64, cold, 10)
					}
					seedCommitteeCredentialAuthorization(
						t, db, cold, authorized, 1, epochStartSlot,
					)

					err := committeeVotingValidate(
						t, era, lv, pparams, voterKey,
						lcommon.VotingProcedures{committeeVoter(voter): {}},
						nil,
					)
					if scriptAuthorization {
						requireUnknownCommitteeVoter(t, err)
					} else {
						require.NoError(t, err)
					}
				})
			}
		}
	}
}
