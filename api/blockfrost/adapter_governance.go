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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/governance"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// governanceTypeName maps a governance action type to its Blockfrost name.
func governanceTypeName(actionType uint8) string {
	switch lcommon.GovActionType(actionType) {
	case lcommon.GovActionTypeParameterChange:
		return "parameter_change"
	case lcommon.GovActionTypeHardForkInitiation:
		return "hard_fork_initiation"
	case lcommon.GovActionTypeTreasuryWithdrawal:
		return "treasury_withdrawals"
	case lcommon.GovActionTypeNoConfidence:
		return "no_confidence"
	case lcommon.GovActionTypeUpdateCommittee:
		return "new_committee"
	case lcommon.GovActionTypeNewConstitution:
		return "new_constitution"
	case lcommon.GovActionTypeInfo:
		return "info_action"
	default:
		return "info_action"
	}
}

// govActionIDString renders the CIP-129 governance action identifier.
func govActionIDString(txHash []byte, index uint32) (string, error) {
	id, err := lcommon.NewGovActionId(txHash, index)
	if err != nil {
		return "", fmt.Errorf("governance action id: %w", err)
	}
	return id.String(), nil
}

func voteName(vote uint8) string {
	switch vote {
	case models.VoteYes:
		return "yes"
	case models.VoteNo:
		return "no"
	default:
		return "abstain"
	}
}

func proposalSummary(
	proposal *models.GovernanceProposal,
) (GovernanceProposalInfo, error) {
	id, err := govActionIDString(proposal.TxHash, proposal.ActionIndex)
	if err != nil {
		return GovernanceProposalInfo{}, err
	}
	return GovernanceProposalInfo{
		ID:             id,
		TxHash:         hex.EncodeToString(proposal.TxHash),
		CertIndex:      int(proposal.ActionIndex),
		GovernanceType: governanceTypeName(proposal.ActionType),
	}, nil
}

// GovernanceProposals returns the paginated proposal list.
func (a *NodeAdapter) GovernanceProposals(
	ctx context.Context,
	params PaginationParams,
) ([]GovernanceProposalInfo, int, error) {
	offset, ok := paginationOffset(params)
	if !ok {
		return []GovernanceProposalInfo{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	rows, total, err := db.Metadata().GetGovernanceProposalsPage(
		params.Count, offset, params.Order, txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get governance proposals: %w", err)
	}
	ret := make([]GovernanceProposalInfo, 0, len(rows))
	for _, row := range rows {
		info, err := proposalSummary(row)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, info)
	}
	return ret, total, nil
}

func (a *NodeAdapter) proposalByRef(
	ctx context.Context,
	ref GovActionRef,
	txn dbtypes.Txn,
) (*models.GovernanceProposal, error) {
	proposal, err := a.ledgerState.Database().Metadata().GetGovernanceProposal(
		ref.TxHash, ref.Index, txn,
	)
	if err != nil {
		return nil, fmt.Errorf("get governance proposal: %w", err)
	}
	if proposal == nil {
		return nil, fmt.Errorf(
			"proposal %x#%d: %w", ref.TxHash, ref.Index,
			models.ErrGovernanceProposalNotFound,
		)
	}
	_ = ctx
	return proposal, nil
}

// decodedProposalAction decodes the stored governance action of a proposal.
func (a *NodeAdapter) decodedProposalAction(
	proposal *models.GovernanceProposal,
) (lcommon.GovAction, error) {
	action, err := governance.DecodeGovActionForPParams(
		proposal.GovActionCbor,
		proposal.ActionType,
		a.ledgerState.GetCurrentPParams(),
	)
	if err != nil {
		return nil, fmt.Errorf("decode governance action: %w", err)
	}
	return action, nil
}

func optionalActionID(id *lcommon.GovActionId) any {
	if id == nil {
		return nil
	}
	return id.String()
}

// governanceDescription renders a governance action as the tagged JSON
// object the Blockfrost description field carries.
func (a *NodeAdapter) governanceDescription(
	action lcommon.GovAction,
	proposal *models.GovernanceProposal,
) (json.RawMessage, error) {
	var tagged struct {
		Tag      string `json:"tag"`
		Contents any    `json:"contents,omitempty"`
	}
	switch act := action.(type) {
	case *conway.ConwayParameterChangeGovAction,
		*dijkstra.DijkstraParameterChangeGovAction:
		updateCbor, previous, policy := parameterChangeParts(action)
		params, err := proposalParameters(updateCbor, proposal.ProposedEpoch)
		if err != nil {
			return nil, err
		}
		tagged.Tag = "ParameterChange"
		tagged.Contents = []any{
			optionalActionID(previous), params, optionalHex(policy),
		}
	case *lcommon.HardForkInitiationGovAction:
		tagged.Tag = "HardForkInitiation"
		tagged.Contents = []any{
			optionalActionID(act.ActionId),
			map[string]uint{
				"major": act.ProtocolVersion.Major,
				"minor": act.ProtocolVersion.Minor,
			},
		}
	case *lcommon.TreasuryWithdrawalGovAction:
		withdrawals := treasuryWithdrawals(act)
		amounts := make(map[string]uint64, len(withdrawals))
		for _, w := range withdrawals {
			amount, err := strconv.ParseUint(w.Amount, 10, 64)
			if err != nil {
				return nil, err
			}
			amounts[w.StakeAddress] = amount
		}
		tagged.Tag = "TreasuryWithdrawals"
		tagged.Contents = []any{amounts, optionalHex(act.PolicyHash)}
	case *lcommon.NoConfidenceGovAction:
		tagged.Tag = "NoConfidence"
		tagged.Contents = []any{optionalActionID(act.ActionId)}
	case *lcommon.UpdateCommitteeGovAction:
		removed := make([]string, 0, len(act.Credentials))
		for _, credential := range act.Credentials {
			removed = append(removed, committeeColdID(
				credential.Credential[:],
				credential.CredType == uint(lcommon.CredentialTypeScriptHash),
			))
		}
		slices.Sort(removed)
		added := make(map[string]uint64, len(act.CredEpochs))
		for credential, epoch := range act.CredEpochs {
			if credential == nil {
				continue
			}
			added[committeeColdID(
				credential.Credential[:],
				credential.CredType == uint(lcommon.CredentialTypeScriptHash),
			)] = epoch
		}
		quorum := map[string]int64{}
		if act.Quorum.Rat != nil {
			quorum["numerator"] = act.Quorum.Num().Int64()
			quorum["denominator"] = act.Quorum.Denom().Int64()
		}
		tagged.Tag = "UpdateCommittee"
		tagged.Contents = []any{
			optionalActionID(act.ActionId), removed, added, quorum,
		}
	case *lcommon.NewConstitutionGovAction:
		tagged.Tag = "NewConstitution"
		tagged.Contents = []any{
			optionalActionID(act.ActionId),
			map[string]any{
				"anchor": map[string]string{
					"url":  act.Constitution.Anchor.Url,
					"hash": hex.EncodeToString(act.Constitution.Anchor.DataHash[:]),
				},
				"script": optionalHex(act.Constitution.ScriptHash),
			},
		}
	case *lcommon.InfoGovAction:
		tagged.Tag = "InfoAction"
	default:
		// Dijkstra parameter changes and any future action type keep their
		// tag without contents.
		tagged.Tag = governanceActionTag(proposal.ActionType)
	}
	encoded, err := json.Marshal(tagged)
	if err != nil {
		return nil, fmt.Errorf("encode governance description: %w", err)
	}
	return encoded, nil
}

// parameterChangeParts returns the stored parameter update, previous action
// and policy hash of a parameter change action of either era; the update is
// nil for any other action.
func parameterChangeParts(
	action lcommon.GovAction,
) ([]byte, *lcommon.GovActionId, []byte) {
	switch act := action.(type) {
	case *conway.ConwayParameterChangeGovAction:
		return act.ParamUpdate.Cbor(), act.ActionId, act.PolicyHash
	case *dijkstra.DijkstraParameterChangeGovAction:
		return act.ParamUpdate.Cbor(), act.ActionId, act.PolicyHash
	default:
		return nil, nil, nil
	}
}

func governanceActionTag(actionType uint8) string {
	switch lcommon.GovActionType(actionType) {
	case lcommon.GovActionTypeParameterChange:
		return "ParameterChange"
	case lcommon.GovActionTypeHardForkInitiation:
		return "HardForkInitiation"
	case lcommon.GovActionTypeTreasuryWithdrawal:
		return "TreasuryWithdrawals"
	case lcommon.GovActionTypeNoConfidence:
		return "NoConfidence"
	case lcommon.GovActionTypeUpdateCommittee:
		return "UpdateCommittee"
	case lcommon.GovActionTypeNewConstitution:
		return "NewConstitution"
	case lcommon.GovActionTypeInfo:
		return "InfoAction"
	default:
		return "InfoAction"
	}
}

func optionalHex(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return hex.EncodeToString(b)
}

func treasuryWithdrawals(
	action *lcommon.TreasuryWithdrawalGovAction,
) []GovernanceWithdrawalInfo {
	ret := make([]GovernanceWithdrawalInfo, 0, len(action.Withdrawals))
	for address, amount := range action.Withdrawals {
		if address == nil {
			continue
		}
		ret = append(ret, GovernanceWithdrawalInfo{
			StakeAddress: address.String(),
			Amount:       strconv.FormatUint(amount, 10),
		})
	}
	// The wire order of a CBOR map is not preserved on decode, so order by
	// address for a stable listing.
	slices.SortFunc(ret, func(x, y GovernanceWithdrawalInfo) int {
		switch {
		case x.StakeAddress < y.StakeAddress:
			return -1
		case x.StakeAddress > y.StakeAddress:
			return 1
		default:
			return 0
		}
	})
	return ret
}

// GovernanceProposal returns a proposal and its lifecycle.
func (a *NodeAdapter) GovernanceProposal(
	ctx context.Context,
	ref GovActionRef,
) (GovernanceProposalDetailInfo, error) {
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	proposal, err := a.proposalByRef(ctx, ref, txn.Metadata())
	if err != nil {
		return GovernanceProposalDetailInfo{}, err
	}
	summary, err := proposalSummary(proposal)
	if err != nil {
		return GovernanceProposalDetailInfo{}, err
	}
	action, err := a.decodedProposalAction(proposal)
	if err != nil {
		return GovernanceProposalDetailInfo{}, err
	}
	description, err := a.governanceDescription(action, proposal)
	if err != nil {
		return GovernanceProposalDetailInfo{}, err
	}
	returnAddress, err := lcommon.NewAddressFromBytes(proposal.ReturnAddress)
	if err != nil {
		return GovernanceProposalDetailInfo{}, fmt.Errorf(
			"decode proposal return address: %w", err,
		)
	}
	return GovernanceProposalDetailInfo{
		GovernanceProposalInfo: summary,
		Description:            description,
		Deposit:                strconv.FormatUint(proposal.Deposit, 10),
		ReturnAddress:          returnAddress.String(),
		RatifiedEpoch:          proposal.RatifiedEpoch,
		EnactedEpoch:           proposal.EnactedEpoch,
		DroppedEpoch:           proposal.DroppedEpoch,
		ExpiredEpoch:           proposal.ExpiredEpoch,
		Expiration:             proposal.ExpiresEpoch,
	}, nil
}

// GovernanceProposalParameters returns the parameter update of a parameter
// change proposal. Any other proposal type has none and reports
// models.ErrGovernanceProposalNotFound.
func (a *NodeAdapter) GovernanceProposalParameters(
	ctx context.Context,
	ref GovActionRef,
) (GovernanceProposalParametersInfo, error) {
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	proposal, err := a.proposalByRef(ctx, ref, txn.Metadata())
	if err != nil {
		return GovernanceProposalParametersInfo{}, err
	}
	if lcommon.GovActionType(proposal.ActionType) !=
		lcommon.GovActionTypeParameterChange {
		return GovernanceProposalParametersInfo{}, fmt.Errorf(
			"proposal %x#%d has no parameters: %w",
			ref.TxHash, ref.Index, models.ErrGovernanceProposalNotFound,
		)
	}
	summary, err := proposalSummary(proposal)
	if err != nil {
		return GovernanceProposalParametersInfo{}, err
	}
	action, err := a.decodedProposalAction(proposal)
	if err != nil {
		return GovernanceProposalParametersInfo{}, err
	}
	updateCbor, _, _ := parameterChangeParts(action)
	if updateCbor == nil {
		return GovernanceProposalParametersInfo{}, fmt.Errorf(
			"unsupported parameter change action %T", action,
		)
	}
	params, err := proposalParameters(updateCbor, proposal.ProposedEpoch)
	if err != nil {
		return GovernanceProposalParametersInfo{}, err
	}
	return GovernanceProposalParametersInfo{
		ID:         summary.ID,
		TxHash:     summary.TxHash,
		CertIndex:  summary.CertIndex,
		Parameters: params,
	}, nil
}

// GovernanceProposalWithdrawals returns the treasury withdrawals of a
// proposal; any other proposal type has none.
func (a *NodeAdapter) GovernanceProposalWithdrawals(
	ctx context.Context,
	ref GovActionRef,
	params PaginationParams,
) ([]GovernanceWithdrawalInfo, int, error) {
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	proposal, err := a.proposalByRef(ctx, ref, txn.Metadata())
	if err != nil {
		return nil, 0, err
	}
	if lcommon.GovActionType(proposal.ActionType) !=
		lcommon.GovActionTypeTreasuryWithdrawal {
		return []GovernanceWithdrawalInfo{}, 0, nil
	}
	action, err := a.decodedProposalAction(proposal)
	if err != nil {
		return nil, 0, err
	}
	withdrawal, ok := action.(*lcommon.TreasuryWithdrawalGovAction)
	if !ok {
		return nil, 0, fmt.Errorf("unexpected treasury action %T", action)
	}
	all := treasuryWithdrawals(withdrawal)
	if params.Order == PaginationOrderDesc {
		slices.Reverse(all)
	}
	total := len(all)
	offset, ok := paginationOffset(params)
	if !ok || offset >= total {
		return []GovernanceWithdrawalInfo{}, total, nil
	}
	return all[offset:min(offset+params.Count, total)], total, nil
}

// GovernanceProposalMetadata returns the off-chain document behind a
// proposal's anchor. It reports models.ErrGovernanceProposalNotFound when
// the document has not been fetched.
func (a *NodeAdapter) GovernanceProposalMetadata(
	ctx context.Context,
	ref GovActionRef,
) (GovernanceProposalMetadataInfo, error) {
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	proposal, err := a.proposalByRef(ctx, ref, txn.Metadata())
	if err != nil {
		return GovernanceProposalMetadataInfo{}, err
	}
	summary, err := proposalSummary(proposal)
	if err != nil {
		return GovernanceProposalMetadataInfo{}, err
	}
	doc, err := db.Metadata().GetOffchainMetadata(
		models.OffchainMetadataSourceGovernanceProposal,
		proposal.AnchorURL, proposal.AnchorHash, txn.Metadata(),
	)
	if err != nil {
		return GovernanceProposalMetadataInfo{}, fmt.Errorf(
			"get proposal metadata: %w", err,
		)
	}
	if doc == nil ||
		doc.Status != models.OffchainMetadataStatusFetched ||
		len(doc.Content) == 0 {
		return GovernanceProposalMetadataInfo{}, fmt.Errorf(
			"metadata of proposal %x#%d: %w",
			ref.TxHash, ref.Index, models.ErrGovernanceProposalNotFound,
		)
	}
	ret := GovernanceProposalMetadataInfo{
		ID:        summary.ID,
		TxHash:    summary.TxHash,
		CertIndex: summary.CertIndex,
		URL:       proposal.AnchorURL,
		Hash:      hex.EncodeToString(proposal.AnchorHash),
		Bytes:     "\\x" + hex.EncodeToString(doc.Content),
	}
	if json.Valid(doc.Content) {
		ret.JSONMetadata = json.RawMessage(doc.Content)
	}
	return ret, nil
}

// castTransaction is the transaction that cast a vote, with the position of
// the vote among its voting procedures and the block holding it.
type castTransaction struct {
	txHash      string
	certIndex   int
	blockHeight uint64
}

// slotBlock is a decoded block cached while resolving a page of votes.
type slotBlock struct {
	number uint64
	txs    []lcommon.Transaction
}

// loadSlotBlock reads and decodes the block at slot, reporting
// ErrHistoryNotRetained for a slot the chain store holds no block for.
func (a *NodeAdapter) loadSlotBlock(
	ctx context.Context,
	slot uint64,
) (*slotBlock, error) {
	if a.slotBlocks != nil {
		return a.slotBlocks(ctx, slot)
	}
	stored, err := database.BlockBySlot(ctx, a.ledgerState.Database(), slot)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, fmt.Errorf(
				"block at slot %d: %w",
				slot,
				ErrHistoryNotRetained,
			)
		}
		return nil, fmt.Errorf("get block at slot %d: %w", slot, err)
	}
	txs, err := a.ledgerState.BlockTransactions(ctx, stored)
	if err != nil {
		return nil, fmt.Errorf("get transactions for block at slot %d: %w", slot, err)
	}
	return &slotBlock{number: stored.Number, txs: txs}, nil
}

func lcommonVoterType(voterType uint8, credentialTag uint8) (uint8, bool) {
	switch voterType {
	case models.VoterTypeCC:
		if credentialTag == 1 {
			return lcommon.VoterTypeConstitutionalCommitteeHotScriptHash, true
		}
		return lcommon.VoterTypeConstitutionalCommitteeHotKeyHash, true
	case models.VoterTypeDRep:
		if credentialTag == 1 {
			return lcommon.VoterTypeDRepScriptHash, true
		}
		return lcommon.VoterTypeDRepKeyHash, true
	case models.VoterTypeSPO:
		return lcommon.VoterTypeStakingPoolKeyHash, true
	default:
		return 0, false
	}
}

// voteProcedureIndex is the position of a voting procedure among the
// voter's own procedures in the transaction, in the ledger map's order of
// governance action id (transaction id, then index). This is db-sync's
// voting_procedure.index, which Blockfrost serves as cert_index: it numbers
// each voter's actions from 0, so other voters in the transaction do not
// shift it.
func voteProcedureIndex(
	procedures lcommon.VotingProcedures,
	voter lcommon.Voter,
	action lcommon.GovActionId,
) (int, bool) {
	for v, votes := range procedures {
		if v == nil || v.Type != voter.Type || v.Hash != voter.Hash {
			continue
		}
		actions := make([]*lcommon.GovActionId, 0, len(votes))
		for id := range votes {
			if id != nil {
				actions = append(actions, id)
			}
		}
		slices.SortFunc(actions, func(x, y *lcommon.GovActionId) int {
			if c := bytes.Compare(
				x.TransactionId[:], y.TransactionId[:],
			); c != 0 {
				return c
			}
			return int(x.GovActionIdx) - int(y.GovActionIdx)
		})
		for index, id := range actions {
			if id.Equal(action) {
				return index, true
			}
		}
	}
	return 0, false
}

// castTransactions locates the transaction behind each cast. A cast recorded
// with its position needs only its block's height. One recorded before the
// history kept positions is matched to the block's casts by
// legacyCastTransaction. A cast whose block is not held, or that matches no
// cast in it, reports ErrHistoryNotRetained rather than a made-up transaction.
func (a *NodeAdapter) castTransactions(
	ctx context.Context,
	rows []models.GovernanceVoteRow,
) ([]castTransaction, error) {
	blocks := make(map[uint64]*slotBlock, len(rows))
	ret := make([]castTransaction, 0, len(rows))
	for _, row := range rows {
		block, ok := blocks[row.CastSlot]
		if !ok {
			var err error
			block, err = a.loadSlotBlock(ctx, row.CastSlot)
			if err != nil {
				return nil, err
			}
			blocks[row.CastSlot] = block
		}
		if len(row.CastTxHash) > 0 {
			ret = append(ret, castTransaction{
				txHash:      hex.EncodeToString(row.CastTxHash),
				certIndex:   int(row.CastCertIndex),
				blockHeight: block.number,
			})
			continue
		}
		cast, err := legacyCastTransaction(row, block)
		if err != nil {
			return nil, err
		}
		ret = append(ret, cast)
	}
	return ret, nil
}

// blockCast is one valid voting procedure in a block by the voter of a
// history entry, on its proposal.
type blockCast struct {
	txHash     string
	certIndex  int
	vote       uint8
	anchorURL  string
	anchorHash []byte
}

// legacyCastTransaction matches a history entry recorded without its position
// to a cast in its block. Such history comes in two kinds.
//
// Transition history recorded a cast only when it changed the vote's value or
// anchor, so the block's casts by the voter on the proposal fall into runs of
// one value and only a run's first cast was recorded. The first run goes
// unrecorded when it repeats the value from before the block, so the vote's
// entries at the slot are the first casts of its last CastSlotCount runs, in
// order; a vote's earliest entries have no earlier value, so they record every
// run.
//
// Migration v22 instead seeded one entry per vote that predated it, copying
// the vote's state after its last cast, so a seed stands for the block's final
// cast. A seed is the vote's earliest entry and the only one at its slot; it
// is told apart from transition history when its block holds more than one
// run, or when it sits after the slot the vote was first cast in. A single
// run of repeated casts in the vote's first block fits both kinds, the first
// cast and the last, and is reported as not retained rather than guessed.
//
// The value the entry recorded must match the cast it lands on.
func legacyCastTransaction(
	row models.GovernanceVoteRow,
	block *slotBlock,
) (castTransaction, error) {
	notRetained := fmt.Errorf(
		"transaction casting vote %d at slot %d: %w",
		row.Vote.ID, row.CastSlot, ErrHistoryNotRetained,
	)
	voterType, ok := lcommonVoterType(
		row.Vote.VoterType, row.Vote.VoterCredentialTag,
	)
	if !ok || len(row.Vote.VoterCredential) != 28 {
		return castTransaction{}, fmt.Errorf(
			"vote %d has an invalid voter", row.Vote.ID,
		)
	}
	voter := lcommon.Voter{Type: voterType}
	copy(voter.Hash[:], row.Vote.VoterCredential)
	action, err := lcommon.NewGovActionId(
		row.ProposalTxHash, row.ProposalActionIndex,
	)
	if err != nil {
		return castTransaction{}, err
	}
	casts := make([]blockCast, 0, len(block.txs))
	// runStarts holds the index in casts of each run's first cast.
	runStarts := make([]int, 0, len(block.txs))
	for _, tx := range block.txs {
		if !tx.IsValid() {
			continue
		}
		procedures := tx.VotingProcedures()
		index, matched := voteProcedureIndex(procedures, voter, action)
		if !matched {
			continue
		}
		cast := blockCast{
			txHash:    hex.EncodeToString(tx.Hash().Bytes()),
			certIndex: index,
		}
		if procedure, ok := voterProcedure(procedures, voter, action); ok {
			cast.vote = procedure.Vote
			if procedure.Anchor != nil {
				cast.anchorURL = procedure.Anchor.Url
				cast.anchorHash = procedure.Anchor.DataHash[:]
			}
		}
		if len(casts) == 0 || !sameCastValue(casts[len(casts)-1], cast) {
			runStarts = append(runStarts, len(casts))
		}
		casts = append(casts, cast)
	}
	count, runs := row.CastSlotCount, len(runStarts)
	if runs == 0 || count < 1 || row.CastSlotRank < 0 ||
		row.CastSlotRank >= count {
		return castTransaction{}, notRetained
	}
	var cast blockCast
	single := row.CastSlotEarliest && count == 1
	switch {
	case single && (runs > 1 || row.CastSlot > row.Vote.AddedSlot):
		cast = casts[len(casts)-1]
	case single && len(casts) > 1:
		return castTransaction{}, notRetained
	case count == runs, count == runs-1 && !row.CastSlotEarliest:
		cast = casts[runStarts[runs-count+row.CastSlotRank]]
	default:
		return castTransaction{}, notRetained
	}
	recorded := blockCast{
		vote:       row.Vote.Vote,
		anchorURL:  row.Vote.AnchorURL,
		anchorHash: row.Vote.AnchorHash,
	}
	if !sameCastValue(cast, recorded) {
		return castTransaction{}, notRetained
	}
	return castTransaction{
		txHash:      cast.txHash,
		certIndex:   cast.certIndex,
		blockHeight: block.number,
	}, nil
}

func sameCastValue(a, b blockCast) bool {
	return a.vote == b.vote && a.anchorURL == b.anchorURL &&
		bytes.Equal(a.anchorHash, b.anchorHash)
}

// voterProcedure returns the voter's procedure on action in procedures.
func voterProcedure(
	procedures lcommon.VotingProcedures,
	voter lcommon.Voter,
	action lcommon.GovActionId,
) (lcommon.VotingProcedure, bool) {
	for v, votes := range procedures {
		if v == nil || v.Type != voter.Type || v.Hash != voter.Hash {
			continue
		}
		for id, procedure := range votes {
			if id != nil && id.Equal(action) {
				return procedure, true
			}
		}
	}
	return lcommon.VotingProcedure{}, false
}

func voterRole(voterType uint8) string {
	switch voterType {
	case models.VoterTypeCC:
		return "constitutional_committee"
	case models.VoterTypeDRep:
		return "drep"
	default:
		return "spo"
	}
}

func voterID(row models.GovernanceVoteRow) (string, error) {
	voterType, ok := lcommonVoterType(
		row.Vote.VoterType, row.Vote.VoterCredentialTag,
	)
	if !ok || len(row.Vote.VoterCredential) != 28 {
		return "", fmt.Errorf("vote %d has an invalid voter", row.Vote.ID)
	}
	voter := lcommon.Voter{Type: voterType}
	copy(voter.Hash[:], row.Vote.VoterCredential)
	return voter.String(), nil
}

// votesPage reads one page of votes and locates their casting transactions.
// The page's read transaction is released before the blocks are loaded:
// each block load takes its own read transaction, and holding a pooled
// connection across them lets concurrent requests exhaust the read pool.
func (a *NodeAdapter) votesPage(
	ctx context.Context,
	filter models.GovernanceVoteFilter,
	params PaginationParams,
) ([]models.GovernanceVoteRow, []castTransaction, int, error) {
	offset, ok := paginationOffset(params)
	if !ok {
		return []models.GovernanceVoteRow{}, []castTransaction{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	rows, total, err := db.Metadata().GetGovernanceVotesPage(
		filter, params.Count, offset, params.Order, txn.Metadata(),
	)
	txn.Release()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("get governance votes: %w", err)
	}
	casts, err := a.castTransactions(ctx, rows)
	if err != nil {
		return nil, nil, 0, err
	}
	return rows, casts, total, nil
}

// GovernanceProposalVotes returns the votes cast on a proposal.
func (a *NodeAdapter) GovernanceProposalVotes(
	ctx context.Context,
	ref GovActionRef,
	params PaginationParams,
) ([]GovernanceProposalVoteInfo, int, error) {
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	proposal, err := a.proposalByRef(ctx, ref, txn.Metadata())
	txn.Release()
	if err != nil {
		return nil, 0, err
	}
	rows, casts, total, err := a.votesPage(
		ctx, models.GovernanceVoteFilter{ProposalID: &proposal.ID}, params,
	)
	if err != nil {
		return nil, 0, err
	}
	ret := make([]GovernanceProposalVoteInfo, 0, len(rows))
	for i, row := range rows {
		voter, err := voterID(row)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, GovernanceProposalVoteInfo{
			TxHash:    casts[i].txHash,
			CertIndex: casts[i].certIndex,
			VoterRole: voterRole(row.Vote.VoterType),
			Voter:     voter,
			Vote:      voteName(row.Vote.Vote),
			Counted:   row.Counted,
		})
	}
	return ret, total, nil
}
