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
	"errors"
	"fmt"
)

// ValidateVerificationMaterial checks that the certificate's active signer
// metadata is consistent with the certified Mithril stake distribution used for
// the same epoch.
func ValidateVerificationMaterial(material *VerificationMaterial) error {
	if material == nil {
		return errors.New("verification material is nil")
	}
	if material.CertificateChain == nil || material.CertificateChain.LeafCertificate == nil {
		return errors.New("certificate chain is incomplete")
	}
	if material.MithrilStakeDistribution == nil {
		return errors.New("mithril stake distribution is missing")
	}
	leaf := material.CertificateChain.LeafCertificate
	mithrilCert := material.MithrilCertificate
	if mithrilCert == nil {
		mithrilCert = leaf
	}
	cardanoCert := material.CardanoCertificate
	if cardanoCert == nil {
		cardanoCert = leaf
	}
	if material.MithrilStakeDistribution.Epoch != mithrilCert.Epoch {
		return fmt.Errorf(
			"mithril stake distribution epoch mismatch: certificate=%d distribution=%d",
			mithrilCert.Epoch,
			material.MithrilStakeDistribution.Epoch,
		)
	}
	if material.MithrilStakeDistribution.CertificateHash != "" &&
		material.MithrilStakeDistribution.CertificateHash != mithrilCert.Hash {
		return fmt.Errorf(
			"mithril stake distribution certificate hash mismatch: certificate=%s distribution=%s",
			mithrilCert.Hash,
			material.MithrilStakeDistribution.CertificateHash,
		)
	}
	if material.CardanoStakeDistribution != nil {
		if material.CardanoStakeDistribution.Epoch != cardanoCert.Epoch {
			return fmt.Errorf(
				"cardano stake distribution epoch mismatch: certificate=%d distribution=%d",
				cardanoCert.Epoch,
				material.CardanoStakeDistribution.Epoch,
			)
		}
		if material.CardanoStakeDistribution.CertificateHash != "" &&
			material.CardanoStakeDistribution.CertificateHash != cardanoCert.Hash {
			return fmt.Errorf(
				"cardano stake distribution certificate hash mismatch: certificate=%s distribution=%s",
				cardanoCert.Hash,
				material.CardanoStakeDistribution.CertificateHash,
			)
		}
	}
	if !mithrilCert.IsGenesis() {
		if err := validateAggregateVerificationKeyEncoding(
			mithrilCert.AggregateVerificationKey,
		); err != nil {
			return fmt.Errorf(
				"aggregate verification key is invalid: %w",
				err,
			)
		}
		if err := validateMultiSignatureEncoding(
			mithrilCert.MultiSignature,
		); err != nil {
			return fmt.Errorf(
				"multi-signature is invalid: %w",
				err,
			)
		}
	}
	for _, signer := range material.MithrilStakeDistribution.Signers {
		if err := validateSignerVerificationKeyEncoding(
			signer.VerificationKey,
		); err != nil {
			return fmt.Errorf(
				"signer %s verification key is invalid: %w",
				signer.PartyID,
				err,
			)
		}
	}
	return nil
}

func validateAggregateVerificationKeyEncoding(encoded string) error {
	if _, err := parseSTMAggregateVerificationKey(encoded); err == nil {
		return nil
	}
	if _, err := decodeBLSG2Point(encoded); err == nil {
		return nil
	}
	return errors.New("unsupported aggregate verification key encoding")
}

func validateMultiSignatureEncoding(encoded string) error {
	if _, err := parseSTMAggregateSignature(encoded); err == nil {
		return nil
	}
	if _, err := decodeBLSG1Point(encoded); err == nil {
		return nil
	}
	return errors.New("unsupported multi-signature encoding")
}

func validateSignerVerificationKeyEncoding(encoded string) error {
	if _, err := parseSTMSignerVerificationKey(encoded); err == nil {
		return nil
	}
	if _, err := decodeBLSG2Point(encoded); err == nil {
		return nil
	}
	return errors.New("unsupported signer verification key encoding")
}
