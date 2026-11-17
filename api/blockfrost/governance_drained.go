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

package blockfrost

import (
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// governanceDrained reports whether the metadata store no longer holds the
// governance family's rows, so the reads that query them directly would
// answer empty.
func (a *NodeAdapter) governanceDrained() bool {
	// A split node reads the IndexerDB, which block replay fills with the
	// whole governance history whatever the ledger drained.
	if a.indexer != nil {
		return false
	}
	return a.ledgerState.DrainedFamilies().Has(nodesettings.FamilyGovernance) ||
		a.ledgerState.Database().FamilyDrained(nodesettings.FamilyGovernance)
}

// refuseDrainedGovernance is the error of a read that needs the proposal
// listing order or the per-cast vote history, which the live UTxO set does
// not keep.
func (a *NodeAdapter) refuseDrainedGovernance(what string) error {
	if !a.governanceDrained() {
		return nil
	}
	return fmt.Errorf(
		"%s of a drained governance family: %w", what, ErrHistoryNotRetained,
	)
}

// drainedProposalByRef reads one proposal through the ledger, which serves
// it from the live UTxO set.
func (a *NodeAdapter) drainedProposalByRef(
	ctx context.Context,
	ref GovActionRef,
) (*models.GovernanceProposal, error) {
	proposal, err := a.ledgerState.LedgerDB().GetGovernanceProposal(
		ctx, ref.TxHash, ref.Index, nil,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"proposal %x#%d: %w", ref.TxHash, ref.Index, err,
		)
	}
	return proposal, nil
}
