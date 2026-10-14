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
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ForecastHardForkInitiations returns pparams with the protocol version the
// ENACT step of the boundary into newEpoch will leave, applying only the
// HardForkInitiation proposals it enacts. It performs no writes, so a caller
// can learn the era a boundary will enter before the boundary runs.
//
// The proposals are those marked ratified and not yet enacted, which ENACT
// reads, and those a boundary has decided but not yet written, which pending
// carries: ENACT runs after the previous boundary's RATIFY marks are written.
// A caller must read pending before this call so that a decision written in
// between is seen by one of the two. A proposal ENACT would return to pending
// because its preflight fails is skipped, as ProcessEpoch skips it.
//
// pparams is returned unchanged when no HardForkInitiation is enacted.
func ForecastHardForkInitiations(
	ctx context.Context,
	db *database.Database,
	txn *database.Txn,
	newEpoch uint64,
	boundarySlot uint64,
	pparams lcommon.ProtocolParameters,
	updateFn func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error),
	pending []*models.GovernanceProposal,
) (lcommon.ProtocolParameters, error) {
	if db == nil {
		return nil, errors.New("nil governance forecast database")
	}
	conwayPParams, err := conwayGovernanceProtocolParameters(pparams)
	if err != nil {
		return nil, err
	}
	if conwayPParams == nil {
		return pparams, nil
	}
	replayed, err := db.GetEnactedGovernanceProposalsAt(
		ctx, newEpoch, boundarySlot, txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get boundary-enacted proposals: %w", err)
	}
	ratified, err := db.GetRatifiedGovernanceProposals(ctx, txn)
	if err != nil {
		return nil, fmt.Errorf("get ratified proposals: %w", err)
	}
	running := pparams
	enact := func(proposal *models.GovernanceProposal, replay bool) error {
		if lcommon.GovActionType(proposal.ActionType) !=
			lcommon.GovActionTypeHardForkInitiation {
			return nil
		}
		if !replay {
			if _, err := ratificationEnactmentPrecondition(
				running, updateFn, proposal, math.MaxUint64,
			); err != nil {
				return nil //nolint:nilerr // ENACT returns such a proposal to pending
			}
		}
		action, err := decodeGovActionForPParams(
			proposal.GovActionCbor, proposal.ActionType, running,
		)
		if err != nil {
			return fmt.Errorf("decode gov action: %w", err)
		}
		hardFork, ok := action.(*lcommon.HardForkInitiationGovAction)
		if !ok {
			return fmt.Errorf("unexpected gov action type %T", action)
		}
		running, err = setProtocolVersion(
			running,
			hardFork.ProtocolVersion.Major,
			hardFork.ProtocolVersion.Minor,
		)
		return err
	}
	for _, proposal := range orderParameterChangeChains(replayed) {
		if err := enact(proposal, true); err != nil {
			return nil, err
		}
	}
	queue := append(
		append([]*models.GovernanceProposal{}, ratified...),
		pending...,
	)
	for _, proposal := range orderParameterChangeChains(queue) {
		if err := enact(proposal, false); err != nil {
			return nil, err
		}
	}
	return running, nil
}
