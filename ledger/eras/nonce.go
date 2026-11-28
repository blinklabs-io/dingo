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

package eras

import (
	"fmt"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/vrf"
)

// NonceFromBytes returns the ordinary nonce held in the 32 bytes b. Dingo
// carries nonces as byte slices, and a 32-byte slice is always an ordinary
// nonce here: a caller with a neutral nonce passes lcommon.Nonce{Type:
// lcommon.NonceTypeNeutral} to gOuroboros itself.
func NonceFromBytes(b []byte) (lcommon.Nonce, error) {
	if len(b) != lcommon.Blake2b256Size {
		return lcommon.Nonce{}, fmt.Errorf(
			"invalid nonce length: %d, expected %d",
			len(b),
			lcommon.Blake2b256Size,
		)
	}
	nonce := lcommon.Nonce{Type: lcommon.NonceTypeNonce}
	copy(nonce.Value[:], b)
	return nonce, nil
}

// CombineNonceBytes folds the 32-byte nonce contribution into the 32-byte
// rolling nonce prev, as the ⭒ operator does for two ordinary nonces.
func CombineNonceBytes(prev, contribution []byte) ([]byte, error) {
	b, err := NonceFromBytes(contribution)
	if err != nil {
		return nil, fmt.Errorf("nonce contribution: %w", err)
	}
	a, err := NonceFromBytes(prev)
	if err != nil {
		return nil, fmt.Errorf("previous block nonce: %w", err)
	}
	combined, err := lcommon.CombineNonces(a, b)
	if err != nil {
		return nil, err
	}
	return combined.Value[:], nil
}

// VRFInputPraos returns the Praos VRF input for slot and the 32-byte epoch
// nonce eta0.
func VRFInputPraos(slot uint64, eta0 []byte) ([]byte, error) {
	nonce, err := NonceFromBytes(eta0)
	if err != nil {
		return nil, fmt.Errorf("epoch nonce: %w", err)
	}
	return vrf.MkInputVrfFromNonce(slot, nonce)
}

// VRFSeedTPraos returns the TPraos VRF input for slot and the 32-byte epoch
// nonce eta0, XORed with seedConstant (vrf.SeedL or vrf.SeedEta).
func VRFSeedTPraos(
	slot uint64,
	eta0 []byte,
	seedConstant []byte,
) ([]byte, error) {
	nonce, err := NonceFromBytes(eta0)
	if err != nil {
		return nil, fmt.Errorf("epoch nonce: %w", err)
	}
	return vrf.MkSeedTPraosFromNonce(slot, nonce, seedConstant)
}
