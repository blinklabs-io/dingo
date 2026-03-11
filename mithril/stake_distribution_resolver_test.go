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

func TestResolveStakeDistributionForCertificateMithril(t *testing.T) {
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/artifact/mithril-stake-distributions":
			_ = json.NewEncoder(w).Encode([]MithrilStakeDistributionListItem{
				{
					Hash:            "msd123",
					CertificateHash: "cert123",
					Epoch:           270,
				},
			})
		case "/artifact/mithril-stake-distribution/msd123":
			_ = json.NewEncoder(w).Encode(MithrilStakeDistribution{
				Hash:            "msd123",
				CertificateHash: "cert123",
				Epoch:           270,
			})
		default:
			http.NotFound(w, r)
		}
	})

	result, err := ResolveStakeDistributionForCertificate(
		context.Background(),
		NewClient(server.URL),
		&CertificateChainVerificationResult{
			SignedEntityKind: signedEntityTypeMithrilStakeDistribution,
			LeafCertificate: &Certificate{
				Hash:  "cert123",
				Epoch: 270,
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, signedEntityTypeMithrilStakeDistribution, result.Kind)
	require.NotNil(t, result.MithrilStakeDistribution)
}

func TestResolveStakeDistributionForCertificateCardano(t *testing.T) {
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/artifact/cardano-stake-distributions":
			_ = json.NewEncoder(w).Encode([]CardanoStakeDistributionListItem{
				{
					Hash:            "csd123",
					CertificateHash: "cert123",
					Epoch:           270,
				},
			})
		case "/artifact/cardano-stake-distribution/csd123":
			_ = json.NewEncoder(w).Encode(CardanoStakeDistribution{
				Hash:            "csd123",
				CertificateHash: "cert123",
				Epoch:           270,
			})
		default:
			http.NotFound(w, r)
		}
	})

	result, err := ResolveStakeDistributionForCertificate(
		context.Background(),
		NewClient(server.URL),
		&CertificateChainVerificationResult{
			SignedEntityKind: signedEntityTypeCardanoStakeDistribution,
			LeafCertificate: &Certificate{
				Hash:  "cert123",
				Epoch: 270,
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, signedEntityTypeCardanoStakeDistribution, result.Kind)
	require.NotNil(t, result.CardanoStakeDistribution)
}

// TestResolveStakeDistributionEpochMatchDoesNotOverrideCertHash verifies that
// an artifact whose epoch matches the leaf certificate but whose certificate
// hash differs is NOT returned. Before the fix, the `||` condition on
// item.Epoch == leaf.Epoch would incorrectly bind an unverified artifact.
func TestResolveStakeDistributionEpochMatchDoesNotOverrideCertHash(t *testing.T) {
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/artifact/mithril-stake-distributions":
			// Artifact exists for epoch 270 but signed by a DIFFERENT cert.
			_ = json.NewEncoder(w).Encode([]MithrilStakeDistributionListItem{
				{
					Hash:            "msd-wrong",
					CertificateHash: "cert-OTHER",
					Epoch:           270,
				},
			})
		case "/artifact/cardano-stake-distributions":
			_ = json.NewEncoder(w).Encode([]CardanoStakeDistributionListItem{
				{
					Hash:            "csd-wrong",
					CertificateHash: "cert-OTHER",
					Epoch:           270,
				},
			})
		default:
			http.NotFound(w, r)
		}
	})

	// Mithril variant: same epoch, different cert hash -> must fail.
	_, err := ResolveStakeDistributionForCertificate(
		context.Background(),
		NewClient(server.URL),
		&CertificateChainVerificationResult{
			SignedEntityKind: signedEntityTypeMithrilStakeDistribution,
			LeafCertificate: &Certificate{
				Hash:  "cert-EXPECTED",
				Epoch: 270,
			},
		},
	)
	require.Error(t, err)
	require.Contains(
		t,
		err.Error(),
		"mithril stake distribution certificate hash mismatch",
	)

	// Cardano variant: same epoch, different cert hash -> must fail.
	_, err = ResolveStakeDistributionForCertificate(
		context.Background(),
		NewClient(server.URL),
		&CertificateChainVerificationResult{
			SignedEntityKind: signedEntityTypeCardanoStakeDistribution,
			LeafCertificate: &Certificate{
				Hash:  "cert-EXPECTED",
				Epoch: 270,
			},
		},
	)
	require.Error(t, err)
	require.Contains(
		t,
		err.Error(),
		"cardano stake distribution certificate hash mismatch",
	)
}

// TestResolveStakeDistributionFallbackMithril exercises the fallback path
// where the leaf certificate is NOT a stake distribution entity but a
// supporting certificate in the chain IS.
func TestResolveStakeDistributionFallbackMithril(t *testing.T) {
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/artifact/mithril-stake-distributions":
			_ = json.NewEncoder(w).Encode([]MithrilStakeDistributionListItem{
				{
					Hash:            "msd-support",
					CertificateHash: "support-cert",
					Epoch:           270,
				},
			})
		case "/artifact/mithril-stake-distribution/msd-support":
			_ = json.NewEncoder(w).Encode(MithrilStakeDistribution{
				Hash:            "msd-support",
				CertificateHash: "support-cert",
				Epoch:           270,
			})
		default:
			http.NotFound(w, r)
		}
	})

	// Leaf is a snapshot cert; supporting cert in chain is the MSD cert.
	supportCert := &Certificate{
		Hash:  "support-cert",
		Epoch: 270,
		SignedEntityType: SignedEntityType{
			raw: json.RawMessage(
				`{"MithrilStakeDistribution":270}`,
			),
		},
	}

	result, err := ResolveStakeDistributionForCertificate(
		context.Background(),
		NewClient(server.URL),
		&CertificateChainVerificationResult{
			SignedEntityKind: signedEntityTypeCardanoImmutableFilesFull,
			LeafCertificate: &Certificate{
				Hash:  "leaf-cert",
				Epoch: 270,
			},
			Certificates: []*Certificate{supportCert},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, signedEntityTypeMithrilStakeDistribution, result.Kind)
	require.NotNil(t, result.MithrilStakeDistribution)
	require.Equal(t, "msd-support", result.MithrilStakeDistribution.Hash)
}

// TestResolveStakeDistributionFallbackCardano exercises the fallback path
// for Cardano stake distributions.
func TestResolveStakeDistributionFallbackCardano(t *testing.T) {
	server := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/artifact/mithril-stake-distributions":
			_ = json.NewEncoder(w).Encode(
				[]MithrilStakeDistributionListItem{},
			)
		case "/artifact/cardano-stake-distributions":
			_ = json.NewEncoder(w).Encode([]CardanoStakeDistributionListItem{
				{
					Hash:            "csd-support",
					CertificateHash: "support-cert",
					Epoch:           270,
				},
			})
		case "/artifact/cardano-stake-distribution/csd-support":
			_ = json.NewEncoder(w).Encode(CardanoStakeDistribution{
				Hash:            "csd-support",
				CertificateHash: "support-cert",
				Epoch:           270,
			})
		default:
			http.NotFound(w, r)
		}
	})

	supportCert := &Certificate{
		Hash:  "support-cert",
		Epoch: 270,
		SignedEntityType: SignedEntityType{
			raw: json.RawMessage(
				`{"CardanoStakeDistribution":270}`,
			),
		},
	}

	result, err := ResolveStakeDistributionForCertificate(
		context.Background(),
		NewClient(server.URL),
		&CertificateChainVerificationResult{
			SignedEntityKind: signedEntityTypeCardanoImmutableFilesFull,
			LeafCertificate: &Certificate{
				Hash:  "leaf-cert",
				Epoch: 270,
			},
			Certificates: []*Certificate{supportCert},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, signedEntityTypeCardanoStakeDistribution, result.Kind)
	require.NotNil(t, result.CardanoStakeDistribution)
	require.Equal(t, "csd-support", result.CardanoStakeDistribution.Hash)
}
