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
	"math/big"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// LoadPersistedProtocolParameters decodes the era-owned CBOR row and restores
// Dijkstra's genesis-only stake thresholds, which are intentionally absent
// from the on-chain protocol-parameter CBOR representation.
func (e *Engine) LoadPersistedProtocolParameters(
	epoch uint64,
	era eras.EraDesc,
	txn ledgerdb.Txn,
) (lcommon.ProtocolParameters, error) {
	return e.loadPersistedProtocolParametersWith(
		epoch, era, era.DecodePParamsFunc, txn,
	)
}

func (e *Engine) loadPersistedProtocolParametersWith(
	epoch uint64,
	era eras.EraDesc,
	decode func([]byte) (lcommon.ProtocolParameters, error),
	txn ledgerdb.Txn,
) (lcommon.ProtocolParameters, error) {
	if decode == nil {
		return nil, nil
	}
	pparams, err := e.ledgerDB().GetPParams(
		epoch,
		era.Id,
		decode,
		txn,
	)
	if err != nil || pparams == nil || era.Id != eras.DijkstraEraDesc.Id {
		return pparams, err
	}
	dijkstraPParams, ok := pparams.(*dijkstra.DijkstraProtocolParameters)
	if !ok || dijkstraPParams == nil {
		return nil, fmt.Errorf(
			"persisted Dijkstra pparams decoded as %T",
			pparams,
		)
	}
	if e.config.CardanoNodeConfig != nil {
		genesis := e.config.CardanoNodeConfig.DijkstraGenesis()
		if genesis != nil {
			if dijkstraPParams.CommitteeStakeCoverage == nil {
				dijkstraPParams.CommitteeStakeCoverage = cloneGenesisRat(
					genesis.CommitteeStakeCoverage,
				)
			}
			if dijkstraPParams.QuorumStakeThreshold == nil {
				dijkstraPParams.QuorumStakeThreshold = cloneGenesisRat(
					genesis.QuorumStakeThreshold,
				)
			}
		}
	}
	if err := dijkstraPParams.ValidateLeiosCommitteeParameters(); err != nil {
		return nil, fmt.Errorf("validate persisted Dijkstra pparams: %w", err)
	}
	return dijkstraPParams, nil
}

func cloneGenesisRat(value *lcommon.GenesisRat) *cbor.Rat {
	if value == nil || value.Rat == nil {
		return nil
	}
	return &cbor.Rat{Rat: new(big.Rat).Set(value.Rat)}
}
