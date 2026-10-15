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
	t.Parallel()
	g1Hex := testBLSG1Hex(t)
	g2Hex := testBLSG2Hex(t)
	for _, tt := range []struct {
		name         string
		signerStake  uint64
		signerKey    string
		cardanoEpoch uint64
		wantError    string
	}{
		{"matching distributions", 42, g2Hex, 270, ""},
		{"stake mismatch is ignored", 99, g2Hex, 270, ""},
		{"invalid signer key", 42, "%%%", 270, "verification key is invalid"},
		{
			"cardano epoch mismatch", 42, g2Hex, 271,
			"cardano stake distribution epoch mismatch",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
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
						{
							PartyID:         "pool1abc",
							Stake:           tt.signerStake,
							VerificationKey: tt.signerKey,
						},
					},
				},
				CardanoStakeDistribution: &CardanoStakeDistribution{
					Epoch:           tt.cardanoEpoch,
					CertificateHash: "leaf",
				},
			})
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
