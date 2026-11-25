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

	"github.com/blinklabs-io/dingo/ledger/epochengine"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

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
	if !epochengine.EraHasExtraEntropy(eraId) || ls.db == nil {
		return nil, era, false, nil
	}
	eraDesc, ok := ls.eraById(eraId)
	if !ok || eraDesc == nil {
		return nil, era, false, nil
	}
	era = *eraDesc
	var base lcommon.ProtocolParameters
	if era.DecodePParamsFunc != nil {
		stored, err := ls.ledgerDB().GetPParams(
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
	return epochengine.ExtraEntropyFromPParams(base), nil
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
	return epochengine.ExtraEntropyFromPParams(base), nil
}
