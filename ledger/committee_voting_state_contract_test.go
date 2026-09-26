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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// seatExpiredCommitteeMember seats a cold credential whose term ended before
// the view's epoch. cardano-ledger keeps it in committeeMembers until an
// enacted action removes it, so it is still elected.
func seatExpiredCommitteeMember(
	t *testing.T,
	db *database.Database,
	cold lcommon.Credential,
) {
	t.Helper()
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredentialTag: uint8(cold.CredType),
		ColdCredHash:      cold.Credential[:],
		ExpiresEpoch:      1,
	}}, nil))
}

// gouroboros common.CommitteeVotingState: CommitteeHotCredentialColdCredentials
// returns every cold credential currently authorizing the exact tagged hot
// credential and does not filter by enacted membership or expiry; only a
// resigned cold credential has no authorization to return. The current
// authorizations are the csCommitteeCreds entries that survive the epoch
// boundary (Conway EPOCH updateCommitteeState).
func TestLedgerViewCommitteeHotCredentialColdCredentialsReturnsEveryAuthorization(
	t *testing.T,
) {
	t.Parallel()

	const epochStartSlot = 100
	pparams := committeeVotingConway.pparams(lcommon.ProtocolVersionVanRossem)
	lv, db := committeeTestView(t, pparams)
	lv.pinCommitteeState(5, pparams)
	lv.epochStartSlot = epochStartSlot
	hot := committeeTestCredential(0x11)
	scriptHot := lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: hot.Credential,
	}

	seated := committeeTestCredential(0x21)
	expired := committeeTestCredential(0x22)
	resignedSeated := committeeTestCredential(0x23)
	movedAway := committeeTestCredential(0x24)
	scriptTwin := committeeTestCredential(0x25)
	seatCommitteeMembers(t, db, seated, resignedSeated, movedAway, scriptTwin)
	seatExpiredCommitteeMember(t, db, expired)
	seedCommitteeCredentialAuthorization(t, db, seated, hot, 1, 1)
	seedCommitteeCredentialAuthorization(t, db, expired, hot, 2, 1)
	seedCommitteeCredentialAuthorization(t, db, resignedSeated, hot, 3, 1)
	seedCommitteeCredentialResignation(t, db, resignedSeated, 4, 2)
	seedCommitteeCredentialAuthorization(t, db, movedAway, hot, 5, 1)
	seedCommitteeCredentialAuthorization(
		t, db, movedAway, committeeTestCredential(0x12), 6, 2,
	)
	seedCommitteeCredentialAuthorization(t, db, scriptTwin, scriptHot, 7, 1)

	pending := committeeTestCredential(0x31)
	pendingResigned := committeeTestCredential(0x32)
	pendingLastEpoch := committeeTestCredential(0x33)
	for i, cold := range []lcommon.Credential{
		pending, pendingResigned, pendingLastEpoch,
	} {
		storeCommitteeUpdateProposal(t, db, byte(0x41+i), cold, 10)
	}
	seedCommitteeCredentialAuthorization(
		t, db, pending, hot, 8, epochStartSlot,
	)
	seedCommitteeCredentialAuthorization(
		t, db, pendingResigned, hot, 9, epochStartSlot,
	)
	seedCommitteeCredentialResignation(
		t, db, pendingResigned, 10, epochStartSlot+1,
	)
	seedCommitteeCredentialAuthorization(
		t, db, pendingLastEpoch, hot, 11, epochStartSlot-1,
	)

	coldCredentials, err := lv.CommitteeHotCredentialColdCredentials(hot)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]lcommon.Credential{seated, expired, pending},
		coldCredentials,
	)
	coldCredentials, err = lv.CommitteeHotCredentialColdCredentials(scriptHot)
	require.NoError(t, err)
	require.Equal(t, []lcommon.Credential{scriptTwin}, coldCredentials)

	for _, cold := range []lcommon.Credential{seated, expired} {
		elected, err := lv.CommitteeCredentialIsElected(cold)
		require.NoError(t, err)
		require.True(t, elected)
	}
	elected, err := lv.CommitteeCredentialIsElected(pending)
	require.NoError(t, err)
	require.False(t, elected)
}

// Reference verdicts for one committee hot voter at PV9, PV10 and PV11:
// VotersDoNotExist unless a surviving csCommitteeCreds entry authorizes the
// hot credential (at every version), plus UnelectedCommitteeVoters from PV11
// unless that entry's cold credential is in the enacted committee. Expiry is
// not consulted by GOV.
func TestValidateTxCommitteeVoterVerdictsByProtocolVersion(t *testing.T) {
	t.Parallel()

	const epochStartSlot = 100
	type verdict int
	const (
		accept verdict = iota
		unknown
		unelected
	)
	versions := []struct {
		label string
		major uint
	}{
		{label: "PV9", major: lcommon.ProtocolVersionPlomin - 1},
		{label: "PV10", major: lcommon.ProtocolVersionPlomin},
		{label: "PV11", major: lcommon.ProtocolVersionVanRossem},
	}
	members := []struct {
		name string
		seed func(
			t *testing.T,
			db *database.Database,
			cold, hot lcommon.Credential,
		)
		verdicts [3]verdict
	}{
		{
			name: "seated",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, cold)
				seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
			},
			verdicts: [3]verdict{accept, accept, accept},
		},
		{
			name: "seated expired",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatExpiredCommitteeMember(t, db, cold)
				seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
			},
			verdicts: [3]verdict{accept, accept, accept},
		},
		{
			name: "seated resigned",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, cold)
				seedCommitteeCredentialAuthorization(t, db, cold, hot, 1, 1)
				seedCommitteeCredentialResignation(t, db, cold, 2, 2)
			},
			verdicts: [3]verdict{unknown, unknown, unelected},
		},
		{
			name: "pending authorized this epoch",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, committeeTestCredential(0x5f))
				storeCommitteeUpdateProposal(t, db, 0x5e, cold, 10)
				seedCommitteeCredentialAuthorization(
					t, db, cold, hot, 1, epochStartSlot,
				)
			},
			verdicts: [3]verdict{accept, accept, unelected},
		},
		{
			name: "pending authorized last epoch",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, committeeTestCredential(0x5f))
				storeCommitteeUpdateProposal(t, db, 0x5e, cold, 10)
				seedCommitteeCredentialAuthorization(
					t, db, cold, hot, 1, epochStartSlot-1,
				)
			},
			verdicts: [3]verdict{unknown, unknown, unelected},
		},
		{
			name: "pending resigned",
			seed: func(t *testing.T, db *database.Database, cold, hot lcommon.Credential) {
				seatCommitteeMembers(t, db, committeeTestCredential(0x5f))
				storeCommitteeUpdateProposal(t, db, 0x5e, cold, 10)
				seedCommitteeCredentialAuthorization(
					t, db, cold, hot, 1, epochStartSlot,
				)
				seedCommitteeCredentialResignation(
					t, db, cold, 2, epochStartSlot+1,
				)
			},
			verdicts: [3]verdict{unknown, unknown, unelected},
		},
	}
	for _, era := range []committeeVotingEra{
		committeeVotingConway,
		committeeVotingDijkstra,
	} {
		for _, member := range members {
			for i, version := range versions {
				name := fmt.Sprintf(
					"%s/%s/%s",
					era.name,
					member.name,
					version.label,
				)
				want := member.verdicts[i]
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					pparams := era.pparams(version.major)
					lv, db := committeeTestView(t, pparams)
					lv.pinCommitteeState(5, pparams)
					lv.epochStartSlot = epochStartSlot
					cold := committeeTestCredential(0x51)
					hot, hotKey := committeeTestVotingKey(0x52)
					member.seed(t, db, cold, hot)

					err := committeeVotingValidate(
						t, era, lv, pparams, hotKey,
						lcommon.VotingProcedures{committeeVoter(hot): {}},
						nil,
					)
					switch want {
					case accept:
						require.NoError(t, err)
					case unknown:
						requireUnknownCommitteeVoter(t, err)
					case unelected:
						var unelectedErr conway.UnelectedCommitteeVoterError
						require.ErrorAs(t, err, &unelectedErr)
					}
				})
			}
		}
	}
}

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
