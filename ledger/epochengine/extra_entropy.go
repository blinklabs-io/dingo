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

package epochengine

import (
	"fmt"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

// EraHasExtraEntropy reports whether an era folds the extraEntropy protocol
// parameter into its epoch nonce.
//
// Only the TPraos eras do. cardano-ledger's TICKN rule computes
//
//	epochNonce = candidateNonce ⭒ lastEpochBlockNonce ⭒ extraEntropy
//
// (Cardano.Protocol.TPraos.Rules.Tickn.tickTransition), while the Praos
// implementation used from Babbage onwards drops the third term entirely
// (Ouroboros.Consensus.Protocol.Praos.tickChainDepState computes
// praosStateCandidateNonce ⭒ praosStateLastEpochBlockNonce). That is why
// Babbage and later protocol parameters carry no extraEntropy field at all,
// and why cardano-ledger's Babbage ledger view sets lvExtraEntropy to an
// error thunk rather than a value.
func EraHasExtraEntropy(eraId uint) bool {
	switch eraId {
	case shelley.EraIdShelley,
		allegra.EraIdAllegra,
		mary.EraIdMary,
		alonzo.EraIdAlonzo:
		return true
	default:
		return false
	}
}

// ExtraEntropyFromPParams returns the 32-byte extraEntropy protocol parameter,
// or nil when it is neutral or the era has no such parameter. Nil is the
// NeutralNonce, the identity of the nonce ⭒ operator, so a nil return leaves
// the epoch nonce unchanged. A typed-nil parameter set reads as neutral rather
// than panicking, since a caller can hold one from a failed clone.
func ExtraEntropyFromPParams(pparams lcommon.ProtocolParameters) []byte {
	switch pp := pparams.(type) {
	case *shelley.ShelleyProtocolParameters:
		// Also covers Allegra, whose parameter type is a gouroboros alias for
		// this one; see the compile-time check in protocol_version.go.
		if pp == nil {
			return nil
		}
		return TPraosExtraEntropy(pp.ProtocolMajor, pp.ExtraEntropy)
	case *mary.MaryProtocolParameters:
		if pp == nil {
			return nil
		}
		return TPraosExtraEntropy(pp.ProtocolMajor, pp.ExtraEntropy)
	case *alonzo.AlonzoProtocolParameters:
		if pp == nil {
			return nil
		}
		return TPraosExtraEntropy(pp.ProtocolMajor, pp.ExtraEntropy)
	default:
		return nil
	}
}

// TPraosExtraEntropy drops the term once the parameter set's own protocol
// version has left TPraos. The concrete type cannot decide this on its own at
// a hard-fork boundary out of Alonzo: the parameters the boundary enacts are
// still Alonzo-typed while their protocol version is already Babbage's, and
// the first Praos epoch is ticked by Praos, which takes no extraEntropy term.
func TPraosExtraEntropy(protocolMajor uint, nonce lcommon.Nonce) []byte {
	if protocolMajor >= babbage.MinProtocolVersionBabbage {
		return nil
	}
	return ExtraEntropyNonceBytes(nonce)
}

func ExtraEntropyNonceBytes(nonce lcommon.Nonce) []byte {
	if nonce.Type != lcommon.NonceTypeNonce {
		return nil
	}
	out := make([]byte, len(nonce.Value))
	copy(out, nonce.Value[:])
	return out
}

// AssembleEpochNonce computes eta0 for a new epoch:
//
//	eta0 = candidateNonce ⭒ labForEta ⭒ extraEntropy
//
// labForEta is the carried lastEpochBlockNonce and extraEntropy the protocol
// parameter, either of which may be empty for NeutralNonce, the identity of
// ⭒. The operator is left-associative, so the extra entropy is mixed into the
// already-combined candidate and lab.
func AssembleEpochNonce(
	candidateNonce []byte,
	labForEta []byte,
	extraEntropy []byte,
) ([]byte, error) {
	if len(candidateNonce) != lcommon.Blake2b256Size {
		return nil, fmt.Errorf(
			"epoch nonce requires a 32-byte candidate nonce, got %d",
			len(candidateNonce),
		)
	}
	if len(labForEta) == 0 {
		if len(extraEntropy) == 0 {
			return cloneNonce(candidateNonce), nil
		}
		// candidateNonce ⭒ NeutralNonce ⭒ extraEntropy collapses to
		// candidateNonce ⭒ extraEntropy. An all-zero candidate is Dingo's
		// wire form of NeutralNonce, and NeutralNonce <> x = x
		// (cardano-ledger BaseTypes), so it is the identity here.
		if IsNeutralNonce(candidateNonce) {
			return cloneNonce(extraEntropy), nil
		}
		// Nonce(a) ⭒ Nonce(b) = Nonce(blake2b_256(a || b))
		buf := make([]byte, 0, len(candidateNonce)+len(extraEntropy))
		buf = append(buf, candidateNonce...)
		buf = append(buf, extraEntropy...)
		mixed := lcommon.Blake2b256Hash(buf)
		return mixed.Bytes(), nil
	}
	if len(labForEta) != lcommon.Blake2b256Size ||
		len(extraEntropy) != 0 && len(extraEntropy) != lcommon.Blake2b256Size {
		return nil, fmt.Errorf(
			"invalid epoch nonce inputs: lab=%d, extraEntropy=%d, expected "+
				"32 and 0 or 32",
			len(labForEta),
			len(extraEntropy),
		)
	}
	candidate, err := eras.NonceFromBytes(candidateNonce)
	if err != nil {
		return nil, err
	}
	lab, err := eras.NonceFromBytes(labForEta)
	if err != nil {
		return nil, err
	}
	extra := lcommon.Nonce{Type: lcommon.NonceTypeNeutral}
	if len(extraEntropy) != 0 {
		if extra, err = eras.NonceFromBytes(extraEntropy); err != nil {
			return nil, err
		}
	}
	result, err := lcommon.EpochNonceFromNonces(candidate, lab, extra)
	if err != nil {
		return nil, err
	}
	return result.Value[:], nil
}

// IsNeutralNonce reports whether nonce is the NeutralNonce, the identity of
// the ⭒ operator. cardano-ledger encodes it as the absence of a value; the
// all-zero 32 bytes is its wire form here.
func IsNeutralNonce(nonce []byte) bool {
	for _, b := range nonce {
		if b != 0 {
			return false
		}
	}
	return true
}
