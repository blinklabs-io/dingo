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

package eras

import (
	"errors"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

type committeeVotingTestState struct {
	*mockLedgerState
	available      bool
	knownCold      map[string]*lcommon.CommitteeMember
	authorizations map[string][]lcommon.Credential
	elected        map[string]bool
}

func (s *committeeVotingTestState) CommitteeStateAvailable() (bool, error) {
	return s.available, nil
}

func (s *committeeVotingTestState) CommitteeCredentialMember(
	credential lcommon.Credential,
) (*lcommon.CommitteeMember, error) {
	return s.knownCold[taggedCommitteeCredentialKey(credential)], nil
}

func (*committeeVotingTestState) CommitteeHotCredentialMember(
	lcommon.Credential,
) (*lcommon.CommitteeMember, error) {
	return nil, nil
}

func (s *committeeVotingTestState) CommitteeHotCredentialColdCredentials(
	hot lcommon.Credential,
) ([]lcommon.Credential, error) {
	return s.authorizations[taggedCommitteeCredentialKey(hot)], nil
}

func (s *committeeVotingTestState) CommitteeCredentialIsElected(
	cold lcommon.Credential,
) (bool, error) {
	return s.elected[taggedCommitteeCredentialKey(cold)], nil
}

func TestValidateTxConwayPV11UnknownVotersRequireElectedCommittee(t *testing.T) {
	var coldHash, hotHash lcommon.Blake2b224
	coldHash[0] = 0x11
	hotHash[0] = 0x22
	cold := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: coldHash,
	}
	hot := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hotHash,
	}
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hotHash),
	}
	state := &committeeVotingTestState{
		mockLedgerState: newMockLedgerState(),
		available:       true,
		authorizations: map[string][]lcommon.Credential{
			taggedCommitteeCredentialKey(hot): {cold},
		},
		elected: map[string]bool{
			taggedCommitteeCredentialKey(cold): false,
		},
	}
	tx := &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
		},
	}

	validate := func(major uint) error {
		return ValidateTxConway(tx, 0, state, &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{Major: major},
		})
	}

	// Protocol version 10 recognizes an authorized hot key without requiring
	// its cold credential to be in the enacted committee.
	var unknown conway.UnknownVoterError
	require.False(t, errors.As(
		validate(lcommon.ProtocolVersionPlomin),
		&unknown,
	))

	// Protocol version 11 reports VotersDoNotExist for the same authorization
	// when its cold credential has no elected committee term.
	err := validate(lcommon.ProtocolVersionVanRossem)
	require.ErrorAs(t, err, &unknown)

	// An elected cold credential keeps its hot voter valid at PV11.
	state.elected[taggedCommitteeCredentialKey(cold)] = true
	require.False(t, errors.As(
		validate(lcommon.ProtocolVersionVanRossem),
		&unknown,
	))
}

func TestValidateTxConwayPV11UnknownVoterRequiresCommitteeState(t *testing.T) {
	var hotHash lcommon.Blake2b224
	hotHash[0] = 0x33
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hotHash),
	}
	state := &committeeVotingTestState{
		mockLedgerState: newMockLedgerState(),
		available:       false,
	}
	tx := &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
		},
	}
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionVanRossem,
		},
	}

	var unavailable conway.CommitteeStateUnavailableError
	err := ValidateTxConway(tx, 0, state, pp)
	require.ErrorAs(t, err, &unavailable)
	var unknown conway.UnknownVoterError
	require.False(t, errors.As(err, &unknown))
}
