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

package conformance

import (
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/ouroboros-mock/conformance"
)

// LedgerRulesMetadata contains the fixture state that is not represented in
// ouroboros-mock's shared ParsedInitialState projection.
type LedgerRulesMetadata struct {
	Treasury                      *uint64
	Reserves                      *uint64
	DormantDRepEpochs             *uint64
	GovActionCbor                 map[string][]byte
	UnelectedCommitteeCredentials []common.Credential
}

func committeeCredentialKey(credential common.Credential) string {
	//nolint:gosec // credential tags are bounded ledger enums.
	tag := uint8(credential.CredType)
	return string([]byte{tag}) +
		string(credential.Credential[:])
}

// LoadLedgerRulesInitialState seeds the common parsed state through Dingo's
// real database-backed conformance provider, then imports the additional
// governance state carried by the node-diversity ledger-rule fixtures.
func (m *DingoStateManager) LoadLedgerRulesInitialState(
	state *conformance.ParsedInitialState,
	pp common.ProtocolParameters,
	metadata LedgerRulesMetadata,
) error {
	if err := m.LoadInitialState(state, pp); err != nil {
		return err
	}
	m.unelectedCommitteeMembers = make(
		map[string]struct{},
		len(metadata.UnelectedCommitteeCredentials),
	)
	for _, credential := range metadata.UnelectedCommitteeCredentials {
		m.unelectedCommitteeMembers[committeeCredentialKey(credential)] = struct{}{}
	}

	txn := m.db.Transaction(context.Background(), true)
	defer txn.Release()

	if metadata.Treasury != nil || metadata.Reserves != nil {
		var treasury, reserves uint64
		if metadata.Treasury != nil {
			treasury = *metadata.Treasury
		}
		if metadata.Reserves != nil {
			reserves = *metadata.Reserves
		}
		if err := m.db.Metadata().SetNetworkState(
			treasury,
			reserves,
			0,
			txn.Metadata(),
		); err != nil {
			return fmt.Errorf("seed protocol pots: %w", err)
		}
	}
	if metadata.DormantDRepEpochs != nil {
		if err := m.db.SetImportedDormantDRepEpochs(
			context.Background(),
			*metadata.DormantDRepEpochs,
			txn,
		); err != nil {
			return fmt.Errorf("seed dormant DRep epochs: %w", err)
		}
	}

	roots := []struct {
		id         *string
		actionType uint8
	}{
		{
			state.ProposalRoots.ProtocolParameters,
			uint8(common.GovActionTypeParameterChange),
		},
		{
			state.ProposalRoots.HardFork,
			uint8(common.GovActionTypeHardForkInitiation),
		},
		{
			state.ProposalRoots.ConstitutionalCommittee,
			uint8(common.GovActionTypeUpdateCommittee),
		},
		{
			state.ProposalRoots.Constitution,
			uint8(common.GovActionTypeNewConstitution),
		},
	}
	for _, root := range roots {
		if root.id == nil {
			continue
		}
		proposal := models.GovernanceProposal{
			TxHash:      parseProposalTxHash(*root.id),
			ActionIndex: parseProposalActionIdx(*root.id),
			ActionType:  root.actionType,
		}
		if info, ok := state.Proposals[*root.id]; ok {
			proposal = m.proposalToModel(*root.id, info)
		}
		enactedEpoch := state.CurrentEpoch
		enactedSlot := uint64(0)
		proposal.EnactedEpoch = &enactedEpoch
		proposal.EnactedSlot = &enactedSlot
		if err := m.db.SetGovernanceProposal(
			context.Background(),
			&proposal,
			txn,
		); err != nil {
			return fmt.Errorf("seed proposal root %s: %w", *root.id, err)
		}
	}
	for id, actionCbor := range metadata.GovActionCbor {
		proposal, err := m.db.GetGovernanceProposal(
			context.Background(),
			parseProposalTxHash(id),
			parseProposalActionIdx(id),
			txn,
		)
		if err != nil {
			return fmt.Errorf(
				"load proposal %s for action contents: %w",
				id,
				err,
			)
		}
		proposal.GovActionCbor = append([]byte(nil), actionCbor...)
		if err := m.db.SetGovernanceProposal(
			context.Background(),
			proposal,
			txn,
		); err != nil {
			return fmt.Errorf("seed proposal %s action contents: %w", id, err)
		}
	}

	return txn.Commit()
}
