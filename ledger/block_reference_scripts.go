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
	"errors"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// pv10ReferenceScriptState applies Conway's pre-PV11 restricted-map behavior
// only to aggregate block reference-script accounting. A missing reference
// input contributes zero to that accounting; transaction validation still
// resolves it through the original state and owns validity.
type pv10ReferenceScriptState struct {
	state lcommon.UtxoState
}

func (s pv10ReferenceScriptState) UtxoById(
	input lcommon.TransactionInput,
) (lcommon.Utxo, error) {
	utxo, err := s.state.UtxoById(input)
	if errors.Is(err, database.ErrUtxoNotFound) {
		return lcommon.Utxo{}, nil
	}
	return utxo, err
}

// referenceScriptParams selects the parameters for the aggregate check of a
// block. A block of the era immediately before the ledger's current era is
// judged under the previous era's parameters, the same predecessor rule
// resolveValidationEra applies to its transactions, so the aggregate and
// per-transaction checks cannot pick different parameters for one block. The
// ledger's own era transition has already fixed currentEra; the block
// contributes only its wire era, and a block further behind is unreachable.
func referenceScriptParams(
	block ledger.Block,
	currentEra eras.EraDesc,
	pparams, prevEraPParams lcommon.ProtocolParameters,
) lcommon.ProtocolParameters {
	if uint(block.Era().Id)+1 == currentEra.Id && prevEraPParams != nil {
		return prevEraPParams
	}
	return pparams
}

// validateBlockReferenceScripts checks the aggregate against the UTxO state
// before this block's own transactions. The era helpers apply protocol-version
// rules for preceding outputs and include phase-2-invalid transactions.
func validateBlockReferenceScripts(
	block ledger.Block,
	pp lcommon.ProtocolParameters,
	state lcommon.UtxoState,
) error {
	switch b := block.(type) {
	case *conway.ConwayBlock:
		conwayPParams, ok := pp.(*conway.ConwayProtocolParameters)
		if !ok || conwayPParams == nil {
			return errors.New("pparams are not expected type")
		}
		if state != nil &&
			conwayPParams.ProtocolVersion.Major <= lcommon.ProtocolVersionPlomin {
			state = pv10ReferenceScriptState{state: state}
		}
		return conway.ValidateRefScriptSizePerBlock(b, pp, state)
	case *dijkstra.DijkstraBlock:
		// The upstream rule accepts both parameter shapes, but dereferences
		// either pointer. Reject typed nils before delegating.
		switch p := pp.(type) {
		case *dijkstra.DijkstraProtocolParameters:
			if p == nil {
				return errors.New("pparams are not expected type")
			}
		case *conway.ConwayProtocolParameters:
			if p == nil {
				return errors.New("pparams are not expected type")
			}
		}
		return dijkstra.ValidateRefScriptSizePerBlock(b, pp, state)
	default:
		return nil
	}
}
