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
	"strings"
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
	certificate := verification.LeafCertificate
	kind := verification.SignedEntityKind
	source := "leaf"
	if kind != signedEntityTypeMithrilStakeDistribution &&
		kind != signedEntityTypeCardanoStakeDistribution {
		source = "chain"
		certificate = certificateForSignedEntityKind(verification,
			signedEntityTypeMithrilStakeDistribution)
		kind = signedEntityTypeMithrilStakeDistribution
		if certificate == nil {
			certificate = certificateForSignedEntityKind(verification,
				signedEntityTypeCardanoStakeDistribution)
			kind = signedEntityTypeCardanoStakeDistribution
		}
		if certificate == nil {
			return nil, fmt.Errorf(
				"no stake distribution artifact found for signed entity kind %q",
				verification.SignedEntityKind,
			)
		}
	}
	result := &ResolvedStakeDistribution{Kind: kind}
	switch kind {
	case signedEntityTypeMithrilStakeDistribution:
		artifact, err := resolveMithrilStakeDistribution(
			ctx,
			client,
			certificate,
			true,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"resolving Mithril stake distribution for %s certificate: %w",
				source,
				err,
			)
		}
		result.MithrilStakeDistribution = artifact
	case signedEntityTypeCardanoStakeDistribution:
		artifact, err := resolveCardanoStakeDistribution(
			ctx,
			client,
			certificate,
			true,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"resolving Cardano stake distribution for %s certificate: %w",
				source,
				err,
			)
		}
		result.CardanoStakeDistribution = artifact
	}
	return result, nil
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
		return nil, fmt.Errorf("listing Mithril stake distributions: %w", err)
	}
	hash, err := stakeDistributionHash(
		list, leaf, epochFallback, "Mithril",
		func(item MithrilStakeDistributionListItem) (string, string, uint64) {
			return item.Hash, item.CertificateHash, item.Epoch
		},
	)
	if err != nil {
		return nil, err
	}
	distribution, err := client.GetMithrilStakeDistribution(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf(
			"getting Mithril stake distribution %s: %w",
			hash,
			err,
		)
	}
	return distribution, nil
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
		return nil, fmt.Errorf("listing Cardano stake distributions: %w", err)
	}
	hash, err := stakeDistributionHash(
		list, leaf, epochFallback, "Cardano",
		func(item CardanoStakeDistributionListItem) (string, string, uint64) {
			return item.Hash, item.CertificateHash, item.Epoch
		},
	)
	if err != nil {
		return nil, err
	}
	distribution, err := client.GetCardanoStakeDistribution(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf(
			"getting Cardano stake distribution %s: %w",
			hash,
			err,
		)
	}
	return distribution, nil
}

func stakeDistributionHash[T any](
	list []T,
	certificate *Certificate,
	epochFallback bool,
	kind string,
	reference func(T) (hash, certificateHash string, epoch uint64),
) (string, error) {
	var epochHash, epochCertificate string
	for _, entry := range list {
		hash, certificateHash, epoch := reference(entry)
		if certificateHash != "" && certificateHash == certificate.Hash {
			return hash, nil
		}
		if epochFallback && epoch == certificate.Epoch && epochHash == "" {
			epochHash = hash
			epochCertificate = certificateHash
		}
	}
	if epochHash == "" {
		return "", fmt.Errorf(
			"no %s stake distribution found for certificate %s at epoch %d",
			kind, certificate.Hash, certificate.Epoch,
		)
	}
	if epochCertificate != "" && epochCertificate != certificate.Hash {
		return "", fmt.Errorf(
			"%s stake distribution certificate hash mismatch: distribution=%s certificate=%s",
			strings.ToLower(kind),
			epochCertificate,
			certificate.Hash,
		)
	}
	return epochHash, nil
}
