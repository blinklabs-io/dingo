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

package governance

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// TranslateRatifiedGovActions rewrites ratified-but-not-yet-enacted governance
// actions that must survive an era boundary in the target era's CBOR shape.
func TranslateRatifiedGovActions(
	db *database.Database,
	txn *database.Txn,
	fromEraId uint,
	toEraId uint,
) error {
	if fromEraId != conway.EraIdConway ||
		toEraId != gdijkstra.EraIdDijkstra {
		return nil
	}
	proposals, err := db.GetRatifiedGovernanceProposals(txn)
	if err != nil {
		return err
	}
	for _, proposal := range proposals {
		if lcommon.GovActionType(proposal.ActionType) !=
			lcommon.GovActionTypeParameterChange {
			continue
		}
		translated, err := translateConwayParamChangeToDijkstra(
			proposal.GovActionCbor,
		)
		if err != nil {
			return fmt.Errorf(
				"translate ratified parameter-change proposal %x#%d: %w",
				proposal.TxHash,
				proposal.ActionIndex,
				err,
			)
		}
		proposal.GovActionCbor = translated
		if err := db.SetGovernanceProposal(proposal, txn); err != nil {
			return fmt.Errorf(
				"persist translated governance proposal %x#%d: %w",
				proposal.TxHash,
				proposal.ActionIndex,
				err,
			)
		}
	}
	return nil
}

func translateConwayParamChangeToDijkstra(data []byte) ([]byte, error) {
	var action conway.ConwayParameterChangeGovAction
	if _, err := cbor.Decode(data, &action); err != nil {
		return nil, err
	}
	translated := gdijkstra.DijkstraParameterChangeGovAction{
		Type:        action.Type,
		ActionId:    action.ActionId,
		ParamUpdate: conwayPParamUpdateToDijkstra(action.ParamUpdate),
		PolicyHash:  action.PolicyHash,
	}
	return cbor.Encode(&translated)
}

func conwayPParamUpdateToDijkstra(
	update conway.ConwayProtocolParameterUpdate,
) gdijkstra.DijkstraProtocolParameterUpdate {
	return gdijkstra.DijkstraProtocolParameterUpdate{
		MinFeeA:                    update.MinFeeA,
		MinFeeB:                    update.MinFeeB,
		MaxBlockBodySize:           update.MaxBlockBodySize,
		MaxTxSize:                  update.MaxTxSize,
		MaxBlockHeaderSize:         update.MaxBlockHeaderSize,
		KeyDeposit:                 update.KeyDeposit,
		PoolDeposit:                update.PoolDeposit,
		MaxEpoch:                   update.MaxEpoch,
		NOpt:                       update.NOpt,
		A0:                         update.A0,
		Rho:                        update.Rho,
		Tau:                        update.Tau,
		ProtocolVersion:            update.ProtocolVersion,
		MinPoolCost:                update.MinPoolCost,
		AdaPerUtxoByte:             update.AdaPerUtxoByte,
		CostModels:                 update.CostModels,
		ExecutionCosts:             update.ExecutionCosts,
		MaxTxExUnits:               update.MaxTxExUnits,
		MaxBlockExUnits:            update.MaxBlockExUnits,
		MaxValueSize:               update.MaxValueSize,
		CollateralPercentage:       update.CollateralPercentage,
		MaxCollateralInputs:        update.MaxCollateralInputs,
		PoolVotingThresholds:       update.PoolVotingThresholds,
		DRepVotingThresholds:       update.DRepVotingThresholds,
		MinCommitteeSize:           update.MinCommitteeSize,
		CommitteeTermLimit:         update.CommitteeTermLimit,
		GovActionValidityPeriod:    update.GovActionValidityPeriod,
		GovActionDeposit:           update.GovActionDeposit,
		DRepDeposit:                update.DRepDeposit,
		DRepInactivityPeriod:       update.DRepInactivityPeriod,
		MinFeeRefScriptCostPerByte: update.MinFeeRefScriptCostPerByte,
	}
}
