package governance

import (
	"bytes"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// ResolveCommitteeProposal resolves a cold credential as a potential future
// committee member: one that a pending UpdateCommittee proposal extending the
// current committee-purpose root would add. cardano-ledger's GOVCERT accepts a
// certificate from such a credential when any pending committee proposal names
// it among its new members (isPotentialFutureMember), whatever other proposals
// do with it, so a proposal removing the credential does not cancel another
// that adds it. Among the proposals that add it, the newest supplies the
// returned expiry epoch and term start slot.
func ResolveCommitteeProposal(
	proposals []*models.GovernanceProposal,
	root *models.GovernanceProposal,
	coldCredential lcommon.Credential,
	pparams lcommon.ProtocolParameters,
) (*lcommon.CommitteeMember, uint64, error) {
	var (
		selected *models.GovernanceProposal
		expiry   uint64
	)
	consider := func(
		proposal *models.GovernanceProposal,
		inLineage bool,
	) error {
		if proposal == nil ||
			lcommon.GovActionType(
				proposal.ActionType,
			) != lcommon.GovActionTypeUpdateCommittee ||
			!inLineage {
			return nil
		}
		action, err := DecodeGovActionForPParams(
			proposal.GovActionCbor, proposal.ActionType, pparams,
		)
		if err != nil {
			return fmt.Errorf("decode committee proposal: %w", err)
		}
		update, ok := action.(*lcommon.UpdateCommitteeGovAction)
		if !ok {
			return fmt.Errorf("unexpected committee action %T", action)
		}
		proposedExpiry, adds := committeeActionAddsCredential(
			update,
			coldCredential,
		)
		if !adds {
			return nil
		}
		if selected == nil || proposal.AddedSlot > selected.AddedSlot ||
			(proposal.AddedSlot == selected.AddedSlot && proposal.ID > selected.ID) {
			selected, expiry = proposal, proposedExpiry
		}
		return nil
	}
	for _, proposal := range proposals {
		if err := consider(
			proposal,
			proposal != nil &&
				committeeProposalInLineage(proposals, proposal, root, nil),
		); err != nil {
			return nil, 0, err
		}
	}
	if selected == nil && root == nil {
		// Some imported histories do not carry a reconstructable enacted root.
		// Preserve their rootless pending proposals. With a root present, a
		// proposal outside its lineage cannot enact, so the fallback must not
		// reach for one.
		for _, proposal := range proposals {
			if err := consider(proposal, true); err != nil {
				return nil, 0, err
			}
		}
	}
	if selected == nil {
		return nil, 0, nil
	}
	return &lcommon.CommitteeMember{
		ColdKey:     coldCredential.Credential,
		ExpiryEpoch: expiry,
	}, selected.AddedSlot, nil
}

// committeeActionAddsCredential reports whether an UpdateCommittee action names
// the exact tagged cold credential among its new members, with its expiry.
func committeeActionAddsCredential(
	action *lcommon.UpdateCommitteeGovAction,
	coldCredential lcommon.Credential,
) (uint64, bool) {
	if action == nil {
		return 0, false
	}
	for credential, expiry := range action.CredEpochs {
		if credential != nil &&
			credential.CredType == coldCredential.CredType &&
			credential.Credential == coldCredential.Credential {
			return expiry, true
		}
	}
	return 0, false
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
