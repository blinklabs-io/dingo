// Copyright 2024 Blink Labs Software
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

package eras

import (
	"errors"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// praosVRFNonceValue computes the nonce contribution from a Praos
// (Babbage/Conway) block's VRF output. In Praos, the VRF output
// is NOT used raw — it goes through domain separation and double
// hashing matching the Haskell vrfNonceValue function:
//
//	hashVRF SVRFNonce cert = blake2b_256("N" || rawVrfOutput)
//	vrfNonceValue = blake2b_256(hashVRF result)
//
// This differs from TPraos (Shelley–Alonzo) which uses the raw
// 64-byte NonceVrf output directly via coerce.
//
// Ref: Ouroboros.Consensus.Protocol.Praos.VRF (vrfNonceValue, hashVRF)
func praosVRFNonceValue(vrfOutput []byte) []byte {
	// Step 1: range extension with domain separator "N"
	tagged := make([]byte, 1+len(vrfOutput))
	tagged[0] = 'N'
	copy(tagged[1:], vrfOutput)
	rangeExtended := lcommon.Blake2b256Hash(tagged)
	// Step 2: nonce generation
	nonceValue := lcommon.Blake2b256Hash(rangeExtended.Bytes())
	return nonceValue.Bytes()
}

var ErrIncompatibleProtocolParams = errors.New("pparams are not expected type")

type EraDesc struct {
	DecodePParamsFunc       func([]byte) (lcommon.ProtocolParameters, error)
	DecodePParamsUpdateFunc func([]byte) (any, error)
	PParamsUpdateFunc       func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error)
	HardForkFunc            func(*cardano.CardanoNodeConfig, lcommon.ProtocolParameters) (lcommon.ProtocolParameters, error)
	EpochLengthFunc         func(*cardano.CardanoNodeConfig) (uint, uint, error)
	CalculateEtaVFunc       func(*cardano.CardanoNodeConfig, []byte, ledger.Block) ([]byte, error)
	CertDepositFunc         func(lcommon.Certificate, lcommon.ProtocolParameters) (uint64, error)
	ValidateTxFunc          func(lcommon.Transaction, uint64, lcommon.LedgerState, lcommon.ProtocolParameters) error
	EvaluateTxFunc          func(lcommon.Transaction, lcommon.LedgerState, lcommon.ProtocolParameters) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error)
	Name                    string
	Id                      uint
	// MinMajorVersion and MaxMajorVersion are the inclusive protocol-major
	// version range covered by this era. Adjacent eras must meet without
	// gap or overlap (cur.MinMajorVersion == prev.MaxMajorVersion + 1).
	MinMajorVersion uint
	MaxMajorVersion uint
}

var Eras = []EraDesc{
	ByronEraDesc,
	ShelleyEraDesc,
	AllegraEraDesc,
	MaryEraDesc,
	AlonzoEraDesc,
	BabbageEraDesc,
	ConwayEraDesc,
}

// EraForVersion returns the era descriptor whose [MinMajorVersion,
// MaxMajorVersion] range contains majorVersion. Returns (nil, false) if
// no era covers the given version.
func EraForVersion(majorVersion uint) (*EraDesc, bool) {
	for i := range Eras {
		if majorVersion >= Eras[i].MinMajorVersion &&
			majorVersion <= Eras[i].MaxMajorVersion {
			return &Eras[i], true
		}
	}
	return nil, false
}

// GetEraById returns the era descriptor for the given era ID.
// Returns nil if the era ID is not found.
func GetEraById(eraId uint) *EraDesc {
	for i := range Eras {
		if Eras[i].Id == eraId {
			return &Eras[i]
		}
	}
	return nil
}

// IsCompatibleEra checks if a transaction era is valid
// for the current ledger era. Cardano allows transactions
// from the current era and the immediately previous era
// (era - 1) after a hard fork. The previous era is
// determined by the ordering of the Eras slice, not by
// numeric ID arithmetic.
//
// Returns true if txEraId matches the ledgerEraId or if
// txEraId is the era immediately preceding ledgerEraId
// in the known era sequence. Returns false for unknown
// era IDs that don't match, future eras, or eras more
// than one step back.
func IsCompatibleEra(txEraId, ledgerEraId uint) bool {
	if txEraId == ledgerEraId {
		return true
	}
	// Find the ledger era's index in the Eras slice
	ledgerIdx := -1
	for i := range Eras {
		if Eras[i].Id == ledgerEraId {
			ledgerIdx = i
			break
		}
	}
	// Unknown ledger era: only exact match is valid
	// (already handled above)
	if ledgerIdx < 0 {
		return false
	}
	// Check if txEraId is the immediately previous era
	if ledgerIdx > 0 && Eras[ledgerIdx-1].Id == txEraId {
		return true
	}
	return false
}
