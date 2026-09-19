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

package ledger

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

// eraHasExtraEntropy reports whether an era folds the extraEntropy protocol
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
func eraHasExtraEntropy(eraId uint) bool {
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

// extraEntropyFromPParams returns the 32-byte extraEntropy protocol parameter,
// or nil when it is neutral or the era has no such parameter. Nil is the
// NeutralNonce, the identity of the nonce ⭒ operator, so a nil return leaves
// the epoch nonce unchanged. A typed-nil parameter set reads as neutral rather
// than panicking, since a caller can hold one from a failed clone.
func extraEntropyFromPParams(pparams lcommon.ProtocolParameters) []byte {
	switch pp := pparams.(type) {
	case *shelley.ShelleyProtocolParameters:
		// Also covers Allegra, whose parameter type is a gouroboros alias for
		// this one; see the compile-time check in protocol_version.go.
		if pp == nil {
			return nil
		}
		return tpraosExtraEntropy(pp.ProtocolMajor, pp.ExtraEntropy)
	case *mary.MaryProtocolParameters:
		if pp == nil {
			return nil
		}
		return tpraosExtraEntropy(pp.ProtocolMajor, pp.ExtraEntropy)
	case *alonzo.AlonzoProtocolParameters:
		if pp == nil {
			return nil
		}
		return tpraosExtraEntropy(pp.ProtocolMajor, pp.ExtraEntropy)
	default:
		return nil
	}
}

// tpraosExtraEntropy drops the term once the parameter set's own protocol
// version has left TPraos. The concrete type cannot decide this on its own at
// a hard-fork boundary out of Alonzo: the parameters the boundary enacts are
// still Alonzo-typed while their protocol version is already Babbage's, and
// the first Praos epoch is ticked by Praos, which takes no extraEntropy term.
func tpraosExtraEntropy(protocolMajor uint, nonce lcommon.Nonce) []byte {
	if protocolMajor >= babbage.MinProtocolVersionBabbage {
		return nil
	}
	return extraEntropyNonceBytes(nonce)
}

func extraEntropyNonceBytes(nonce lcommon.Nonce) []byte {
	if nonce.Type != lcommon.NonceTypeNonce {
		return nil
	}
	out := make([]byte, len(nonce.Value))
	copy(out, nonce.Value[:])
	return out
}

// epochExtraEntropyBase returns the recorded protocol parameters that govern
// epochId in era eraId, or nil when the era has no extraEntropy parameter or
// no parameters are on record.
//
// The stored row is preferred over the in-memory current parameters because
// the epoch cache can already run ahead of the applied epoch, which would make
// the in-memory value belong to an older epoch than the one being computed.
func (ls *LedgerState) epochExtraEntropyBase(
	epochId uint64,
	eraId uint,
) (lcommon.ProtocolParameters, eras.EraDesc, bool, error) {
	var era eras.EraDesc
	if !eraHasExtraEntropy(eraId) || ls.db == nil {
		return nil, era, false, nil
	}
	eraDesc, ok := ls.eraById(eraId)
	if !ok || eraDesc == nil {
		return nil, era, false, nil
	}
	era = *eraDesc
	var base lcommon.ProtocolParameters
	if era.DecodePParamsFunc != nil {
		stored, err := ls.db.GetPParams(
			epochId, era.Id, era.DecodePParamsFunc, nil,
		)
		if err != nil {
			return nil, era, false, fmt.Errorf(
				"get protocol parameters for epoch %d: %w", epochId, err,
			)
		}
		base = stored
	}
	if base == nil {
		snapshot := ls.loadConsensusSnapshot()
		if snapshot == nil || snapshot.currentEra.Id != era.Id {
			return nil, era, false, nil
		}
		base = snapshot.currentPParams
	}
	if base == nil {
		return nil, era, false, nil
	}
	return base, era, true, nil
}

// recordedExtraEntropyForEpoch resolves the extraEntropy of an epoch whose
// protocol parameters are already enacted and on record. It performs no
// forecast, so it must not be used for an epoch the rollover has not reached.
func (ls *LedgerState) recordedExtraEntropyForEpoch(
	epochId uint64,
	eraId uint,
) ([]byte, error) {
	base, _, ok, err := ls.epochExtraEntropyBase(epochId, eraId)
	if err != nil || !ok {
		return nil, err
	}
	return extraEntropyFromPParams(base), nil
}

// forecastExtraEntropyForEpoch resolves the extraEntropy that belongs in the
// nonce of epochId for a path that runs before the rollover has enacted that
// epoch's protocol parameters.
//
// It mirrors cardano-ledger's TICKF forecast, which is what supplies TICKN
// with its extraEntropy: the recorded parameters are advanced by the pending
// genesis-key update that the boundary will enact into epochId.
//
// A forecast reaching more than one epoch past the last enacted row sees only
// the update submitted in epochId-1, so an extraEntropy set by an intervening,
// not-yet-enacted update would be missed. That window closes as soon as the
// rollover enacts and persists the intervening epoch's parameters, and the
// rollover itself never forecasts.
func (ls *LedgerState) forecastExtraEntropyForEpoch(
	epochId uint64,
	eraId uint,
) ([]byte, error) {
	base, era, ok, err := ls.epochExtraEntropyBase(epochId, eraId)
	if err != nil || !ok {
		return nil, err
	}
	forecast, err := ls.forecastPendingPParamUpdate(era, epochId, base)
	if err != nil {
		return nil, fmt.Errorf(
			"forecast protocol parameters for epoch %d: %w", epochId, err,
		)
	}
	if forecast != nil {
		base = forecast
	}
	return extraEntropyFromPParams(base), nil
}

// assembleEpochNonce computes eta0 for a new epoch:
//
//	eta0 = candidateNonce ⭒ labForEta ⭒ extraEntropy
//
// labForEta is the carried lastEpochBlockNonce and extraEntropy the protocol
// parameter, either of which may be empty for NeutralNonce, the identity of
// ⭒. The operator is left-associative, so the extra entropy is mixed into the
// already-combined candidate and lab.
func assembleEpochNonce(
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
		// candidateNonce ⭒ extraEntropy. CalculateEpochNonce cannot express
		// that, since it requires a 32-byte lab; CalculateRollingNonce is
		// gouroboros' implementation of the bare ⭒ operator.
		result, err := lcommon.CalculateRollingNonce(
			candidateNonce, extraEntropy,
		)
		if err != nil {
			return nil, err
		}
		return result.Bytes(), nil
	}
	result, err := lcommon.CalculateEpochNonce(
		candidateNonce,
		labForEta,
		extraEntropy,
	)
	if err != nil {
		return nil, err
	}
	return result.Bytes(), nil
}
