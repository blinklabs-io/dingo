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
	"errors"
	"fmt"
)

// ResolvedStakeDistribution bundles the stake-distribution artifact selected
// for a certificate verification flow.
type ResolvedStakeDistribution struct {
	Kind                     string
	MithrilStakeDistribution *MithrilStakeDistribution
	CardanoStakeDistribution *CardanoStakeDistribution
}

// ResolveStakeDistributionForCertificate locates the stake-distribution
// artifact referenced by a verified certificate chain result.
func ResolveStakeDistributionForCertificate(
	ctx context.Context,
	client *Client,
	verification *CertificateChainVerificationResult,
) (*ResolvedStakeDistribution, error) {
	if client == nil {
		return nil, errors.New("mithril client is nil")
	}
	if verification == nil || verification.LeafCertificate == nil {
		return nil, errors.New("certificate verification result is incomplete")
	}

	// Try direct match first based on signed entity kind.
	switch verification.SignedEntityKind {
	case signedEntityTypeMithrilStakeDistribution:
		artifact, err := resolveMithrilStakeDistribution(
			ctx, client, verification.LeafCertificate, true,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"resolving Mithril stake distribution for leaf certificate: %w",
				err,
			)
		}
		return &ResolvedStakeDistribution{
			Kind:                     signedEntityTypeMithrilStakeDistribution,
			MithrilStakeDistribution: artifact,
		}, nil
	case signedEntityTypeCardanoStakeDistribution:
		artifact, err := resolveCardanoStakeDistribution(
			ctx, client, verification.LeafCertificate, true,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"resolving Cardano stake distribution for leaf certificate: %w",
				err,
			)
		}
		return &ResolvedStakeDistribution{
			Kind:                     signedEntityTypeCardanoStakeDistribution,
			CardanoStakeDistribution: artifact,
		}, nil
	}

	// For non-stake-distribution entity kinds (e.g., CardanoImmutableFilesFull),
	// scan the certificate chain for supporting stake distribution certificates.
	mithrilCert := certificateForSignedEntityKind(
		verification,
		signedEntityTypeMithrilStakeDistribution,
	)
	if mithrilCert != nil {
		artifact, err := resolveMithrilStakeDistribution(
			ctx, client, mithrilCert, true,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"resolving Mithril stake distribution for chain certificate: %w",
				err,
			)
		}
		return &ResolvedStakeDistribution{
			Kind:                     signedEntityTypeMithrilStakeDistribution,
			MithrilStakeDistribution: artifact,
		}, nil
	}
	cardanoCert := certificateForSignedEntityKind(
		verification,
		signedEntityTypeCardanoStakeDistribution,
	)
	if cardanoCert != nil {
		artifact, err := resolveCardanoStakeDistribution(
			ctx, client, cardanoCert, true,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"resolving Cardano stake distribution for chain certificate: %w",
				err,
			)
		}
		return &ResolvedStakeDistribution{
			Kind:                     signedEntityTypeCardanoStakeDistribution,
			CardanoStakeDistribution: artifact,
		}, nil
	}

	return nil, fmt.Errorf(
		"no stake distribution artifact found for signed entity kind %q",
		verification.SignedEntityKind,
	)
}

// resolveMithrilStakeDistribution looks up the Mithril stake distribution
// matching the given certificate. It first matches by exact certificate hash,
// then optionally falls back to epoch-based matching.
func resolveMithrilStakeDistribution(
	ctx context.Context,
	client *Client,
	leaf *Certificate,
	epochFallback bool,
) (*MithrilStakeDistribution, error) {
	list, err := client.ListMithrilStakeDistributions(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"listing Mithril stake distributions: %w", err,
		)
	}
	var epochMatchHash, epochMatchCertHash string
	for _, item := range list {
		if item.CertificateHash != "" && item.CertificateHash == leaf.Hash {
			dist, err := client.GetMithrilStakeDistribution(ctx, item.Hash)
			if err != nil {
				return nil, fmt.Errorf(
					"getting Mithril stake distribution %s: %w",
					item.Hash, err,
				)
			}
			return dist, nil
		}
		if epochFallback && item.Epoch == leaf.Epoch && epochMatchHash == "" {
			epochMatchHash = item.Hash
			epochMatchCertHash = item.CertificateHash
		}
	}
	if epochMatchHash != "" {
		if epochMatchCertHash != "" &&
			epochMatchCertHash != leaf.Hash {
			return nil, fmt.Errorf(
				"mithril stake distribution certificate hash mismatch: distribution=%s certificate=%s",
				epochMatchCertHash,
				leaf.Hash,
			)
		}
		dist, err := client.GetMithrilStakeDistribution(ctx, epochMatchHash)
		if err != nil {
			return nil, fmt.Errorf(
				"getting Mithril stake distribution %s: %w",
				epochMatchHash, err,
			)
		}
		return dist, nil
	}
	return nil, fmt.Errorf(
		"no Mithril stake distribution found for certificate %s at epoch %d",
		leaf.Hash,
		leaf.Epoch,
	)
}

// resolveCardanoStakeDistribution looks up the Cardano stake distribution
// matching the given certificate. It first matches by exact certificate hash,
// then optionally falls back to epoch-based matching.
func resolveCardanoStakeDistribution(
	ctx context.Context,
	client *Client,
	leaf *Certificate,
	epochFallback bool,
) (*CardanoStakeDistribution, error) {
	list, err := client.ListCardanoStakeDistributions(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"listing Cardano stake distributions: %w", err,
		)
	}
	var epochMatchHash, epochMatchCertHash string
	for _, item := range list {
		if item.CertificateHash != "" && item.CertificateHash == leaf.Hash {
			dist, err := client.GetCardanoStakeDistribution(ctx, item.Hash)
			if err != nil {
				return nil, fmt.Errorf(
					"getting Cardano stake distribution %s: %w",
					item.Hash, err,
				)
			}
			return dist, nil
		}
		if epochFallback && item.Epoch == leaf.Epoch && epochMatchHash == "" {
			epochMatchHash = item.Hash
			epochMatchCertHash = item.CertificateHash
		}
	}
	if epochMatchHash != "" {
		if epochMatchCertHash != "" &&
			epochMatchCertHash != leaf.Hash {
			return nil, fmt.Errorf(
				"cardano stake distribution certificate hash mismatch: distribution=%s certificate=%s",
				epochMatchCertHash,
				leaf.Hash,
			)
		}
		dist, err := client.GetCardanoStakeDistribution(ctx, epochMatchHash)
		if err != nil {
			return nil, fmt.Errorf(
				"getting Cardano stake distribution %s: %w",
				epochMatchHash, err,
			)
		}
		return dist, nil
	}
	return nil, fmt.Errorf(
		"no Cardano stake distribution found for certificate %s at epoch %d",
		leaf.Hash,
		leaf.Epoch,
	)
}
