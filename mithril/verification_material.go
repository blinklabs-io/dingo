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

// VerificationMaterial bundles the non-cryptographic inputs needed for full
// Mithril STM certificate verification.
type VerificationMaterial struct {
	CertificateChain         *CertificateChainVerificationResult
	MithrilCertificate       *Certificate
	CardanoCertificate       *Certificate
	MithrilStakeDistribution *MithrilStakeDistribution
	CardanoStakeDistribution *CardanoStakeDistribution
}

// BuildVerificationMaterial assembles the current verification inputs for a
// certificate chain. This does not perform aggregate signature verification; it
// only prepares the inputs required for that future step.
func BuildVerificationMaterial(
	ctx context.Context,
	client *Client,
	verification *CertificateChainVerificationResult,
) (*VerificationMaterial, error) {
	if client == nil {
		return nil, errors.New("mithril client is nil")
	}
	if verification == nil || verification.LeafCertificate == nil {
		return nil, errors.New("certificate verification result is incomplete")
	}

	material := &VerificationMaterial{
		CertificateChain: verification,
	}
	mithrilCert := certificateForSignedEntityKind(
		verification,
		signedEntityTypeMithrilStakeDistribution,
	)
	if mithrilCert == nil {
		mithrilCert = verification.LeafCertificate
	}
	material.MithrilCertificate = mithrilCert
	mithrilDist, err := resolveMithrilStakeDistribution(
		ctx,
		client,
		mithrilCert,
		true,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"resolving Mithril stake distribution for cert %s: %w",
			mithrilCert.Hash,
			err,
		)
	}
	material.MithrilStakeDistribution = mithrilDist

	cardanoCert := certificateForSignedEntityKind(
		verification,
		signedEntityTypeCardanoStakeDistribution,
	)
	if cardanoCert != nil {
		material.CardanoCertificate = cardanoCert
		cardanoDist, err := resolveCardanoStakeDistribution(
			ctx,
			client,
			cardanoCert,
			true,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"resolving Cardano stake distribution for cert %s: %w",
				cardanoCert.Hash,
				err,
			)
		}
		material.CardanoStakeDistribution = cardanoDist
	}
	return material, nil
}

func certificateForSignedEntityKind(
	verification *CertificateChainVerificationResult,
	kind string,
) *Certificate {
	if verification == nil {
		return nil
	}
	if verification.LeafCertificate != nil {
		if k, err := verification.LeafCertificate.SignedEntityType.Kind(); err == nil && k == kind {
			return verification.LeafCertificate
		}
	}
	for _, cert := range verification.Certificates {
		if cert == nil {
			continue
		}
		if k, err := cert.SignedEntityType.Kind(); err == nil && k == kind {
			return cert
		}
	}
	return nil
}
