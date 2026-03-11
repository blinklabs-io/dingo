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
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func signedEntityTypeFromRaw(t *testing.T, raw string) SignedEntityType {
	t.Helper()
	var ret SignedEntityType
	require.NoError(t, json.Unmarshal([]byte(raw), &ret))
	return ret
}

func TestBuildVerificationMaterial(t *testing.T) {
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/artifact/mithril-stake-distributions":
			_ = json.NewEncoder(w).Encode([]MithrilStakeDistributionListItem{
				{Hash: "msd123", Epoch: 270, CertificateHash: "msd-cert"},
				{Hash: "msd999", Epoch: 270, CertificateHash: "other"},
			})
		case "/artifact/mithril-stake-distribution/msd123":
			_ = json.NewEncoder(w).Encode(MithrilStakeDistribution{
				Hash: "msd123", Epoch: 270, CertificateHash: "msd-cert",
			})
		case "/artifact/cardano-stake-distributions":
			_ = json.NewEncoder(w).Encode([]CardanoStakeDistributionListItem{
				{Hash: "csd123", Epoch: 270, CertificateHash: "csd-cert"},
				{Hash: "csd999", Epoch: 270, CertificateHash: "other"},
			})
		case "/artifact/cardano-stake-distribution/csd123":
			_ = json.NewEncoder(w).Encode(CardanoStakeDistribution{
				Hash: "csd123", Epoch: 270, CertificateHash: "csd-cert",
			})
		default:
			http.NotFound(w, r)
		}
	})

	material, err := BuildVerificationMaterial(
		context.Background(),
		NewClient(server.URL),
		&CertificateChainVerificationResult{
			LeafCertificate: &Certificate{
				Hash:  "leaf",
				Epoch: 270,
			},
			Certificates: []*Certificate{
				{
					Hash:  "msd-cert",
					Epoch: 270,
					SignedEntityType: signedEntityTypeFromRaw(
						t,
						`{"MithrilStakeDistribution":270}`,
					),
				},
				{
					Hash:  "csd-cert",
					Epoch: 270,
					SignedEntityType: signedEntityTypeFromRaw(
						t,
						`{"CardanoStakeDistribution":270}`,
					),
				},
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, material)
	require.NotNil(t, material.MithrilStakeDistribution)
	require.NotNil(t, material.CardanoStakeDistribution)
	require.Equal(t, "msd-cert", material.MithrilStakeDistribution.CertificateHash)
	require.Equal(t, "csd-cert", material.CardanoStakeDistribution.CertificateHash)
}

func TestBuildVerificationMaterialRejectsMismatchedEpochFallback(t *testing.T) {
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/artifact/mithril-stake-distributions":
			_ = json.NewEncoder(w).Encode([]MithrilStakeDistributionListItem{
				{Hash: "msd123", Epoch: 270, CertificateHash: "other"},
			})
		case "/artifact/cardano-stake-distributions":
			_ = json.NewEncoder(w).Encode([]CardanoStakeDistributionListItem{
				{Hash: "csd123", Epoch: 270, CertificateHash: "other"},
			})
		default:
			http.NotFound(w, r)
		}
	})

	_, err := BuildVerificationMaterial(
		context.Background(),
		NewClient(server.URL),
		&CertificateChainVerificationResult{
			LeafCertificate: &Certificate{
				Hash:  "leaf",
				Epoch: 270,
			},
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "certificate hash mismatch")
}

func TestBuildVerificationMaterialUsesSupportingCertificatesFromChain(t *testing.T) {
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/artifact/mithril-stake-distributions":
			_ = json.NewEncoder(w).Encode([]MithrilStakeDistributionListItem{
				{Hash: "msd123", Epoch: 270, CertificateHash: "msd-cert"},
			})
		case "/artifact/mithril-stake-distribution/msd123":
			_ = json.NewEncoder(w).Encode(MithrilStakeDistribution{
				Hash: "msd123", Epoch: 270, CertificateHash: "msd-cert",
			})
		case "/artifact/cardano-stake-distributions":
			_ = json.NewEncoder(w).Encode([]CardanoStakeDistributionListItem{
				{Hash: "csd123", Epoch: 269, CertificateHash: "csd-cert"},
			})
		case "/artifact/cardano-stake-distribution/csd123":
			_ = json.NewEncoder(w).Encode(CardanoStakeDistribution{
				Hash: "csd123", Epoch: 269, CertificateHash: "csd-cert",
			})
		default:
			http.NotFound(w, r)
		}
	})

	material, err := BuildVerificationMaterial(
		context.Background(),
		NewClient(server.URL),
		&CertificateChainVerificationResult{
			LeafCertificate: &Certificate{
				Hash:  "leaf",
				Epoch: 270,
				SignedEntityType: signedEntityTypeFromRaw(
					t,
					`{"CardanoImmutableFilesFull":{"epoch":270,"immutable_file_number":1}}`,
				),
			},
			Certificates: []*Certificate{
				{
					Hash:  "msd-cert",
					Epoch: 270,
					SignedEntityType: signedEntityTypeFromRaw(
						t,
						`{"MithrilStakeDistribution":270}`,
					),
				},
				{
					Hash:  "csd-cert",
					Epoch: 269,
					SignedEntityType: signedEntityTypeFromRaw(
						t,
						`{"CardanoStakeDistribution":269}`,
					),
				},
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, "msd-cert", material.MithrilCertificate.Hash)
	require.Equal(t, "csd-cert", material.CardanoCertificate.Hash)
	require.Equal(t, "msd-cert", material.MithrilStakeDistribution.CertificateHash)
	require.Equal(t, "csd-cert", material.CardanoStakeDistribution.CertificateHash)
}
