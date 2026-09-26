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

package conformance

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/stretchr/testify/require"
)

// gouroboros common.CommitteeVotingState: CommitteeHotCredentialColdCredentials
// returns every cold credential currently authorizing the hot credential,
// seated or not, and omits only resigned ones.
func TestCommitteeHotCredentialColdCredentialsIncludesUnseatedAuthorization(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	hot := testHash28(0x81)
	seatedCold := testHash28(0x82)
	require.NoError(t, m.LoadInitialState(
		&conformance.ParsedInitialState{
			CurrentEpoch:     5,
			CommitteeMembers: map[common.Blake2b224]uint64{seatedCold: 999},
			HotKeyAuthorizations: map[common.Blake2b224]common.Blake2b224{
				seatedCold: hot,
			},
		},
		&conway.ConwayProtocolParameters{},
	))
	keyCredential := func(hash common.Blake2b224) common.Credential {
		return common.Credential{
			CredType:   common.CredentialTypeAddrKeyHash,
			Credential: hash,
		}
	}
	pendingCold := testHash28(0x83)
	resignedCold := testHash28(0x84)
	persist := func(seed string, slot uint64, certs ...common.Certificate) {
		tx, err := syntheticTransaction(seed, certs)
		require.NoError(t, err)
		require.NoError(t, m.db.SetTransactionMetadataOnly(
			tx,
			ocommon.Point{Slot: slot, Hash: syntheticBlockHash(slot)},
			0,
			map[int]uint64{},
			nil,
		))
	}
	persist("pending-auth", 10,
		&common.AuthCommitteeHotCertificate{
			CertType:       uint(common.CertificateTypeAuthCommitteeHot),
			ColdCredential: keyCredential(pendingCold),
			HotCredential:  keyCredential(hot),
		},
		&common.AuthCommitteeHotCertificate{
			CertType:       uint(common.CertificateTypeAuthCommitteeHot),
			ColdCredential: keyCredential(resignedCold),
			HotCredential:  keyCredential(hot),
		},
	)
	persist("pending-resign", 11,
		&common.ResignCommitteeColdCertificate{
			CertType:       uint(common.CertificateTypeResignCommitteeCold),
			ColdCredential: keyCredential(resignedCold),
		},
	)

	provider := NewDingoStateProvider(m)
	coldCredentials, err := provider.CommitteeHotCredentialColdCredentials(
		keyCredential(hot),
	)
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]common.Credential{
			keyCredential(seatedCold),
			keyCredential(pendingCold),
		},
		coldCredentials,
	)
	elected, err := provider.CommitteeCredentialIsElected(
		keyCredential(pendingCold),
	)
	require.NoError(t, err)
	require.False(t, elected)
	coldCredentials, err = provider.CommitteeHotCredentialColdCredentials(
		common.Credential{
			CredType:   common.CredentialTypeScriptHash,
			Credential: hot,
		},
	)
	require.NoError(t, err)
	require.Empty(t, coldCredentials)
}
