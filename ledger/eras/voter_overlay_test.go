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
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

type committeeVotingTestState struct {
	*mockLedgerState
	authorizations map[string][]lcommon.Credential
}

func (*committeeVotingTestState) CommitteeStateAvailable() (bool, error) {
	return true, nil
}

func (*committeeVotingTestState) CommitteeCredentialMember(
	lcommon.Credential,
) (*lcommon.CommitteeMember, error) {
	return nil, nil
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

func (*committeeVotingTestState) CommitteeCredentialIsElected(
	lcommon.Credential,
) (bool, error) {
	return true, nil
}

func TestValidateUnknownVotersSeesPriorCommitteeAuthorizationInBlock(
	t *testing.T,
) {
	t.Parallel()

	var coldHash, hotHash lcommon.Blake2b224
	coldHash[0] = 0x45
	hotHash[0] = 0x67
	cold := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: coldHash,
	}
	hot := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hotHash,
	}
	base := &committeeVotingTestState{
		mockLedgerState: newMockLedgerState(),
		authorizations:  map[string][]lcommon.Credential{},
	}
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionVanRossem,
		},
	}
	pending := lcommon.NewBlockLedgerState(base)
	predecessor := &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxCertificates: []lcommon.CertificateWrapper{{
				Type: uint(lcommon.CertificateTypeAuthCommitteeHot),
				Certificate: &lcommon.AuthCommitteeHotCertificate{
					CertType:       uint(lcommon.CertificateTypeAuthCommitteeHot),
					ColdCredential: cold,
					HotCredential:  hot,
				},
			}},
		},
	}
	require.NoError(t, pending.ApplyTransaction(predecessor, pp))

	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hotHash),
	}
	tx := &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
		},
	}
	require.NoError(t, validateUnknownVoters(tx, 0, pending, pp))
}
