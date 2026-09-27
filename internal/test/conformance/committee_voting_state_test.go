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
	"bytes"
	"database/sql"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
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

// Stored committee credentials of the wrong length are corrupt state: the
// harness's committee lookups must fail rather than truncate them into a
// 28-byte credential that matches a real hot key or cold credential.
func TestCommitteeVotingStateRejectsMalformedStoredHashes(t *testing.T) {
	hot := testHash28(0x91)
	cold := testHash28(0x92)
	keyCredential := func(hash common.Blake2b224) common.Credential {
		return common.Credential{
			CredType:   common.CredentialTypeAddrKeyHash,
			Credential: hash,
		}
	}
	overlong := func(hash common.Blake2b224) []byte {
		return append(append([]byte(nil), hash[:]...), 0xff)
	}
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, raw *sql.DB)
		lookup func(p *DingoStateProvider) error
	}{
		{
			name: "authorized hot credential",
			mutate: func(t *testing.T, raw *sql.DB) {
				_, err := raw.Exec(
					`UPDATE auth_committee_hot SET host_credential = ?`,
					overlong(hot),
				)
				require.NoError(t, err)
			},
			lookup: func(p *DingoStateProvider) error {
				_, err := p.CommitteeHotCredentialMember(keyCredential(hot))
				if err != nil {
					return err
				}
				_, err = p.CommitteeHotCredentialColdCredentials(
					keyCredential(hot),
				)
				return err
			},
		},
		{
			name: "seated cold credential",
			mutate: func(t *testing.T, raw *sql.DB) {
				_, err := raw.Exec(
					`UPDATE committee_member SET cold_cred_hash = ?`,
					overlong(cold),
				)
				require.NoError(t, err)
			},
			lookup: func(p *DingoStateProvider) error {
				_, err := p.CommitteeCredentialIsElected(keyCredential(cold))
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewDingoStateManager()
			require.NoError(t, err)
			defer func() { require.NoError(t, m.Close()) }()
			require.NoError(t, m.LoadInitialState(
				&conformance.ParsedInitialState{
					CurrentEpoch:     5,
					CommitteeMembers: map[common.Blake2b224]uint64{cold: 999},
					HotKeyAuthorizations: map[common.Blake2b224]common.Blake2b224{
						cold: hot,
					},
				},
				&conway.ConwayProtocolParameters{},
			))
			raw, err := dbtest.RawSQLiteMetadata(t, m.db)
			require.NoError(t, err)
			tc.mutate(t, raw)

			require.ErrorContains(
				t,
				tc.lookup(NewDingoStateProvider(m)),
				"invalid blake2b-224 hash",
			)
		})
	}
}

// The harness mirrors LedgerView's committee windows: an unseated
// credential's authorization lasts until the next epoch boundary, as
// cardano-ledger's EPOCH updateCommitteeState drops it there.
func TestCommitteeHotCredentialMemberDropsUnseatedAuthorizationAtBoundary(
	t *testing.T,
) {
	m, err := NewDingoStateManager()
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()

	seatedCold := testHash28(0xa1)
	require.NoError(t, m.LoadInitialState(
		&conformance.ParsedInitialState{
			CurrentEpoch:     5,
			CommitteeMembers: map[common.Blake2b224]uint64{seatedCold: 999},
		},
		&conway.ConwayProtocolParameters{},
	))
	pending := common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa2),
	}
	hot := common.Credential{
		CredType:   common.CredentialTypeAddrKeyHash,
		Credential: testHash28(0xa3),
	}
	action, err := common.NewUpdateCommitteeGovAction(
		nil,
		nil,
		map[*common.Credential]uint64{&pending: 999},
		cbor.Rat{Rat: big.NewRat(2, 3)},
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	require.NoError(t, m.db.SetGovernanceProposal(
		&models.GovernanceProposal{
			TxHash:        testHash32(0xa4),
			ActionType:    uint8(common.GovActionTypeUpdateCommittee),
			ExpiresEpoch:  1000,
			GovActionCbor: encoded,
			AnchorHash:    testHash32(0xa5),
			ReturnAddress: bytes.Repeat([]byte{0xa6}, 29),
		},
		nil,
	))
	tx, err := syntheticTransaction(
		"pending-authorization",
		[]common.Certificate{&common.AuthCommitteeHotCertificate{
			CertType:       uint(common.CertificateTypeAuthCommitteeHot),
			ColdCredential: pending,
			HotCredential:  hot,
		}},
	)
	require.NoError(t, err)
	require.NoError(t, m.ApplyTransaction(tx, 10))

	provider := NewDingoStateProvider(m)
	member, err := provider.CommitteeHotCredentialMember(hot)
	require.NoError(t, err)
	require.NotNil(t, member, "the authorization holds for its own epoch")

	require.NoError(t, m.ProcessEpochBoundary(6))
	member, err = provider.CommitteeHotCredentialMember(hot)
	require.NoError(t, err)
	require.Nil(t, member, "the boundary drops an unseated authorization")
	coldMember, err := provider.CommitteeCredentialMember(pending)
	require.NoError(t, err)
	require.NotNil(t, coldMember, "the credential is still a potential member")
	require.Nil(t, coldMember.HotKey)
}
