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

package mithril

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateVerificationMaterial(t *testing.T) {
	g1Hex := testBLSG1Hex(t)
	g2Hex := testBLSG2Hex(t)
	err := ValidateVerificationMaterial(&VerificationMaterial{
		CertificateChain: &CertificateChainVerificationResult{
			LeafCertificate: &Certificate{
				Hash:                     "leaf",
				Epoch:                    270,
				AggregateVerificationKey: g2Hex,
				MultiSignature:           g1Hex,
				Metadata: CertificateMetadata{
					Signers: []StakeDistributionParty{
						{PartyID: "pool1abc", Stake: 42},
					},
				},
			},
		},
		MithrilStakeDistribution: &MithrilStakeDistribution{
			Epoch:           270,
			CertificateHash: "leaf",
			Signers: []MithrilStakeDistributionParty{
				{PartyID: "pool1abc", Stake: 42, VerificationKey: g2Hex},
			},
		},
		CardanoStakeDistribution: &CardanoStakeDistribution{
			Epoch:           270,
			CertificateHash: "leaf",
		},
	})
	require.NoError(t, err)
}

func TestValidateVerificationMaterialIgnoresStakeMismatch(t *testing.T) {
	g1Hex := testBLSG1Hex(t)
	g2Hex := testBLSG2Hex(t)
	err := ValidateVerificationMaterial(&VerificationMaterial{
		CertificateChain: &CertificateChainVerificationResult{
			LeafCertificate: &Certificate{
				Hash:                     "leaf",
				Epoch:                    270,
				AggregateVerificationKey: g2Hex,
				MultiSignature:           g1Hex,
				Metadata: CertificateMetadata{
					Signers: []StakeDistributionParty{
						{PartyID: "pool1abc", Stake: 42},
					},
				},
			},
		},
		MithrilStakeDistribution: &MithrilStakeDistribution{
			Epoch:           270,
			CertificateHash: "leaf",
			Signers: []MithrilStakeDistributionParty{
				{PartyID: "pool1abc", Stake: 99, VerificationKey: g2Hex},
			},
		},
		CardanoStakeDistribution: &CardanoStakeDistribution{
			Epoch:           270,
			CertificateHash: "leaf",
		},
	})
	require.NoError(t, err)
}

func TestValidateVerificationMaterialInvalidSignerKey(t *testing.T) {
	g1Hex := testBLSG1Hex(t)
	g2Hex := testBLSG2Hex(t)
	err := ValidateVerificationMaterial(&VerificationMaterial{
		CertificateChain: &CertificateChainVerificationResult{
			LeafCertificate: &Certificate{
				Hash:                     "leaf",
				Epoch:                    270,
				AggregateVerificationKey: g2Hex,
				MultiSignature:           g1Hex,
				Metadata: CertificateMetadata{
					Signers: []StakeDistributionParty{
						{PartyID: "pool1abc", Stake: 42},
					},
				},
			},
		},
		MithrilStakeDistribution: &MithrilStakeDistribution{
			Epoch:           270,
			CertificateHash: "leaf",
			Signers: []MithrilStakeDistributionParty{
				{PartyID: "pool1abc", Stake: 42, VerificationKey: "%%%"},
			},
		},
		CardanoStakeDistribution: &CardanoStakeDistribution{
			Epoch:           270,
			CertificateHash: "leaf",
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "verification key is invalid")
}

func TestValidateVerificationMaterialCardanoDistributionMismatch(t *testing.T) {
	g1Hex := testBLSG1Hex(t)
	g2Hex := testBLSG2Hex(t)
	err := ValidateVerificationMaterial(&VerificationMaterial{
		CertificateChain: &CertificateChainVerificationResult{
			LeafCertificate: &Certificate{
				Hash:                     "leaf",
				Epoch:                    270,
				AggregateVerificationKey: g2Hex,
				MultiSignature:           g1Hex,
				Metadata: CertificateMetadata{
					Signers: []StakeDistributionParty{
						{PartyID: "pool1abc", Stake: 42},
					},
				},
			},
		},
		MithrilStakeDistribution: &MithrilStakeDistribution{
			Epoch:           270,
			CertificateHash: "leaf",
			Signers: []MithrilStakeDistributionParty{
				{PartyID: "pool1abc", Stake: 42, VerificationKey: g2Hex},
			},
		},
		CardanoStakeDistribution: &CardanoStakeDistribution{
			Epoch:           271,
			CertificateHash: "leaf",
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cardano stake distribution epoch mismatch")
}
