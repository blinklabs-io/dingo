package governance

import (
	"bytes"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ResolveCommitteeProposal resolves the applicable pending UpdateCommittee
// proposal for a cold credential. Only proposals extending the current
// committee-purpose root participate; among competing siblings the newest
// proposal wins. A credential in the winning proposal's removal set is absent.
func ResolveCommitteeProposal(
	proposals []*models.GovernanceProposal,
	root *models.GovernanceProposal,
	coldCredential lcommon.Credential,
	pparams lcommon.ProtocolParameters,
) (*lcommon.CommitteeMember, uint64, error) {
	var selected *models.GovernanceProposal
	var selectedAction *lcommon.UpdateCommitteeGovAction
	for _, proposal := range proposals {
		if proposal == nil || lcommon.GovActionType(proposal.ActionType) != lcommon.GovActionTypeUpdateCommittee ||
			!committeeProposalInLineage(proposals, proposal, root, nil) {
			continue
		}
		action, err := DecodeGovActionForPParams(
			proposal.GovActionCbor, proposal.ActionType, pparams,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("decode committee proposal: %w", err)
		}
		update, ok := action.(*lcommon.UpdateCommitteeGovAction)
		if !ok {
			return nil, 0, fmt.Errorf("unexpected committee action %T", action)
		}
		if !committeeActionMentionsCredential(update, coldCredential) {
			continue
		}
		if selected == nil || proposal.AddedSlot > selected.AddedSlot ||
			(proposal.AddedSlot == selected.AddedSlot && proposal.ID > selected.ID) {
			selected, selectedAction = proposal, update
		}
	}
	if selected == nil {
		// Some imported histories do not carry a reconstructable enacted root.
		// Preserve their rootless pending proposals, while still selecting only
		// the newest proposal rather than the storage order's oldest match.
		for _, proposal := range proposals {
			if proposal == nil || lcommon.GovActionType(proposal.ActionType) != lcommon.GovActionTypeUpdateCommittee {
				continue
			}
			action, err := DecodeGovActionForPParams(
				proposal.GovActionCbor, proposal.ActionType, pparams,
			)
			if err != nil {
				return nil, 0, fmt.Errorf("decode committee proposal: %w", err)
			}
			update, ok := action.(*lcommon.UpdateCommitteeGovAction)
			if !ok {
				return nil, 0, fmt.Errorf("unexpected committee action %T", action)
			}
			if !committeeActionMentionsCredential(update, coldCredential) {
				continue
			}
			if selected == nil || proposal.AddedSlot > selected.AddedSlot ||
				(proposal.AddedSlot == selected.AddedSlot && proposal.ID > selected.ID) {
				selected, selectedAction = proposal, update
			}
		}
	}
	if selected == nil {
		return nil, 0, nil
	}
	for _, credential := range selectedAction.Credentials {
		if credential.CredType == coldCredential.CredType &&
			credential.Credential == coldCredential.Credential {
			return nil, 0, nil
		}
	}
	for credential, expiry := range selectedAction.CredEpochs {
		if credential != nil && credential.CredType == coldCredential.CredType &&
			credential.Credential == coldCredential.Credential {
			return &lcommon.CommitteeMember{
				ColdKey: coldCredential.Credential, ExpiryEpoch: uint64(expiry),
			}, selected.AddedSlot, nil
		}
	}
	return nil, 0, nil
}

func committeeActionMentionsCredential(
	action *lcommon.UpdateCommitteeGovAction,
	coldCredential lcommon.Credential,
) bool {
	if action == nil {
		return false
	}
	for _, credential := range action.Credentials {
		if credential.CredType == coldCredential.CredType &&
			credential.Credential == coldCredential.Credential {
			return true
		}
	}
	for credential := range action.CredEpochs {
		if credential != nil && credential.CredType == coldCredential.CredType &&
			credential.Credential == coldCredential.Credential {
			return true
		}
	}
	return false
}

func committeeProposalExtends(
	proposal, root *models.GovernanceProposal,
) bool {
	if root == nil {
		return proposal.ParentTxHash == nil && proposal.ParentActionIdx == nil
	}
	return bytes.Equal(proposal.ParentTxHash, root.TxHash) &&
		proposal.ParentActionIdx != nil && *proposal.ParentActionIdx == root.ActionIndex
}

// committeeProposalInLineage reports whether proposal is a pending descendant
// of root. Pending proposals may be chained through other pending proposals;
// checking only the immediate parent can select an obsolete branch after a
// re-election. The visited set bounds malformed cyclic proposal data.
func committeeProposalInLineage(
	proposals []*models.GovernanceProposal,
	proposal, root *models.GovernanceProposal,
	visited map[uint]struct{},
) bool {
	if proposal == nil {
		return false
	}
	if root == nil {
		return proposal.ParentTxHash == nil && proposal.ParentActionIdx == nil
	}
	if visited == nil {
		visited = make(map[uint]struct{})
	}
	if _, ok := visited[proposal.ID]; ok {
		return false
	}
	visited[proposal.ID] = struct{}{}
	if committeeProposalExtends(proposal, root) {
		return true
	}
	if proposal.ParentTxHash == nil || proposal.ParentActionIdx == nil {
		return false
	}
	for _, parent := range proposals {
		if parent == nil || parent.ID == proposal.ID ||
			!bytes.Equal(parent.TxHash, proposal.ParentTxHash) ||
			parent.ActionIndex != *proposal.ParentActionIdx {
			continue
		}
		return committeeProposalInLineage(proposals, parent, root, visited)
	}
	return false
}
