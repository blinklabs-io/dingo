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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/governance"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// resolveDRep finds the tag of a DRep credential the node holds. A legacy
// hash-only identifier may name either a key or a script DRep, and the key
// form is tried first.
func (a *NodeAdapter) resolveDRep(
	ctx context.Context,
	credential DRepCredential,
) (uint8, *models.Drep, error) {
	if credential.Predefined != nil {
		return 0, nil, nil
	}
	tags := []uint8{0, 1}
	if credential.CredentialTagKnown {
		tags = []uint8{0}
		if credential.HasScript {
			tags = []uint8{1}
		}
	}
	db := a.ledgerState.Database()
	for _, tag := range tags {
		drep, err := db.GetDrepByCredential(
			ctx, tag, credential.Hash, true, nil,
		)
		if err == nil && drep != nil {
			return tag, drep, nil
		}
		if err != nil && !errors.Is(err, models.ErrDrepNotFound) {
			return 0, nil, fmt.Errorf("get drep %x: %w", credential.Hash, err)
		}
	}
	return 0, nil, fmt.Errorf("drep %x: %w", credential.Hash, ErrDRepNotFound)
}

// DRepDelegators returns the registered delegators of a DRep with their
// live stake.
func (a *NodeAdapter) DRepDelegators(
	ctx context.Context,
	credential DRepCredential,
	params PaginationParams,
) ([]DRepDelegatorInfo, int, error) {
	tag, _, err := a.resolveDRep(ctx, credential)
	if err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []DRepDelegatorInfo{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	rows, total, err := db.Metadata().GetDRepDelegatorStakes(
		tag, credential.Hash, credential.Predefined,
		params.Count, offset, params.Order, txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get drep delegators: %w", err)
	}
	ret := make([]DRepDelegatorInfo, 0, len(rows))
	for _, row := range rows {
		address, err := a.stakeAddressFromTag(row.CredentialTag, row.StakingKey)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, DRepDelegatorInfo{
			Address: address,
			Amount:  strconv.FormatUint(row.Stake, 10),
		})
	}
	return ret, total, nil
}

// DRepMetadata returns the off-chain document behind a DRep's current
// anchor. A DRep with no anchor reports ErrDRepNotFound.
func (a *NodeAdapter) DRepMetadata(
	ctx context.Context,
	credential DRepCredential,
) (DRepAnchorInfo, error) {
	tag, drep, err := a.resolveDRep(ctx, credential)
	if err != nil {
		return DRepAnchorInfo{}, err
	}
	if drep == nil || drep.AnchorURL == "" {
		return DRepAnchorInfo{}, fmt.Errorf(
			"drep %s has no anchor: %w", credential.ID, ErrDRepNotFound,
		)
	}
	hexID := hex.EncodeToString(credential.Hash)
	if credential.CredentialTagKnown {
		hexID = hex.EncodeToString(
			append([]byte{cip129DRepHeader(tag == 1)}, credential.Hash...),
		)
	}
	ret := DRepAnchorInfo{
		DRepID: credential.ID,
		Hex:    hexID,
		URL:    drep.AnchorURL,
		Hash:   hex.EncodeToString(drep.AnchorHash),
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	doc, err := db.Metadata().GetOffchainMetadata(
		models.OffchainMetadataSourceDrep,
		drep.AnchorURL, drep.AnchorHash, txn.Metadata(),
	)
	if err != nil {
		return DRepAnchorInfo{}, fmt.Errorf("get drep metadata: %w", err)
	}
	switch {
	case doc == nil || doc.Status == models.OffchainMetadataStatusPending:
	case doc.Status == models.OffchainMetadataStatusFailed:
		ret.Error = offchainFetchError(
			"Drep", drep.AnchorURL, drep.AnchorHash, doc,
		)
	case doc.Status == models.OffchainMetadataStatusFetched:
		encoded := "\\x" + hex.EncodeToString(doc.Content)
		ret.Bytes = &encoded
		if json.Valid(doc.Content) {
			ret.JSONMetadata = json.RawMessage(doc.Content)
		}
	}
	return ret, nil
}

// DRepUpdates returns a DRep's registration, update and deregistration
// certificates.
func (a *NodeAdapter) DRepUpdates(
	ctx context.Context,
	credential DRepCredential,
	params PaginationParams,
) ([]DRepUpdateInfo, int, error) {
	tag, _, err := a.resolveDRep(ctx, credential)
	if err != nil {
		return nil, 0, err
	}
	offset, ok := paginationOffset(params)
	if !ok {
		return []DRepUpdateInfo{}, 0, nil
	}
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	rows, total, err := db.Metadata().GetDRepUpdates(
		tag, credential.Hash, params.Count, offset, params.Order,
		txn.Metadata(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get drep updates: %w", err)
	}
	ret := make([]DRepUpdateInfo, 0, len(rows))
	for _, row := range rows {
		info := DRepUpdateInfo{
			TxHash:    hex.EncodeToString(row.TxHash),
			CertIndex: int(row.CertIndex),
			Action:    row.Action,
		}
		if row.Deposit != nil {
			deposit := strconv.FormatUint(*row.Deposit, 10)
			info.Deposit = &deposit
		}
		ret = append(ret, info)
	}
	return ret, total, nil
}

// DRepVotes returns the votes a DRep cast.
func (a *NodeAdapter) DRepVotes(
	ctx context.Context,
	credential DRepCredential,
	params PaginationParams,
) ([]DRepVoteInfo, int, error) {
	tag, _, err := a.resolveDRep(ctx, credential)
	if err != nil {
		return nil, 0, err
	}
	if credential.Predefined != nil {
		return []DRepVoteInfo{}, 0, nil
	}
	voterType := uint8(models.VoterTypeDRep)
	rows, casts, total, err := a.votesPage(ctx, models.GovernanceVoteFilter{
		VoterType: &voterType,
		VoterCredentials: []models.CommitteeCredential{
			{CredentialTag: tag, Credential: credential.Hash},
		},
	}, params)
	if err != nil {
		return nil, 0, err
	}
	ret := make([]DRepVoteInfo, 0, len(rows))
	for i, row := range rows {
		proposalID, err := govActionIDString(
			row.ProposalTxHash, row.ProposalActionIndex,
		)
		if err != nil {
			return nil, 0, err
		}
		ret = append(ret, DRepVoteInfo{
			TxHash:            casts[i].txHash,
			CertIndex:         casts[i].certIndex,
			ProposalID:        proposalID,
			ProposalTxHash:    hex.EncodeToString(row.ProposalTxHash),
			ProposalCertIndex: int(row.ProposalActionIndex),
			Vote:              voteName(row.Vote.Vote),
		})
	}
	return ret, total, nil
}

// PoolVotes returns the votes a pool cast.
func (a *NodeAdapter) PoolVotes(
	ctx context.Context,
	poolID string,
	params PaginationParams,
) ([]PoolVoteInfo, int, error) {
	poolKeyHash, _, err := a.knownPool(ctx, poolID)
	if err != nil {
		return nil, 0, err
	}
	voterType := uint8(models.VoterTypeSPO)
	rows, casts, total, err := a.votesPage(ctx, models.GovernanceVoteFilter{
		VoterType: &voterType,
		VoterCredentials: []models.CommitteeCredential{
			{CredentialTag: 0, Credential: poolKeyHash},
		},
	}, params)
	if err != nil {
		return nil, 0, err
	}
	ret := make([]PoolVoteInfo, 0, len(rows))
	for i, row := range rows {
		ret = append(ret, PoolVoteInfo{
			TxHash:    casts[i].txHash,
			CertIndex: casts[i].certIndex,
			Vote:      voteName(row.Vote.Vote),
		})
	}
	return ret, total, nil
}

// committeeCredentialHeader is the CIP-129 header byte of a committee
// credential: hot credentials use 0x02 and 0x03, cold ones 0x12 and 0x13.
func committeeCredentialHeader(cold bool, hasScript bool) byte {
	header := byte(0x02)
	if cold {
		header = 0x12
	}
	if hasScript {
		header++
	}
	return header
}

func committeeCredentialHex(cold bool, hash []byte, hasScript bool) string {
	return hex.EncodeToString(
		append([]byte{committeeCredentialHeader(cold, hasScript)}, hash...),
	)
}

func committeeCredentialID(
	hrp string,
	cold bool,
	hash []byte,
	hasScript bool,
) string {
	id, err := bech32EncodeData(
		hrp,
		append([]byte{committeeCredentialHeader(cold, hasScript)}, hash...),
	)
	if err != nil {
		// Only an out-of-range group size can fail, which a 29-byte payload
		// cannot hit.
		return ""
	}
	return id
}

func committeeColdID(hash []byte, hasScript bool) string {
	return committeeCredentialID("cc_cold", true, hash, hasScript)
}

// GovernanceCommittee returns the constitutional committee.
func (a *NodeAdapter) GovernanceCommittee(
	ctx context.Context,
) (CommitteeInfo, error) {
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	meta := db.Metadata()

	stored, err := db.GetCommitteeMembers(ctx, txn)
	if err != nil {
		return CommitteeInfo{}, err
	}
	// Several rows for one credential are its successive terms, and only the
	// latest is seated.
	latest := make(map[string]*models.CommitteeMember, len(stored))
	var order []string
	for _, member := range stored {
		if member == nil {
			continue
		}
		key := models.CommitteeCredential{
			CredentialTag: member.ColdCredentialTag,
			Credential:    member.ColdCredHash,
		}.Key()
		held, ok := latest[key]
		if !ok {
			order = append(order, key)
		}
		if !ok || member.TermStartSlot > held.TermStartSlot ||
			(member.TermStartSlot == held.TermStartSlot &&
				member.AddedSlot > held.AddedSlot) ||
			(member.TermStartSlot == held.TermStartSlot &&
				member.AddedSlot == held.AddedSlot && member.ID > held.ID) {
			latest[key] = member
		}
	}
	credentials := make([]models.CommitteeCredential, 0, len(order))
	for _, key := range order {
		member, ok := latest[key]
		if !ok || member == nil {
			continue
		}
		credentials = append(credentials, models.CommitteeCredential{
			CredentialTag: member.ColdCredentialTag,
			Credential:    member.ColdCredHash,
			TermStartSlot: member.TermStartSlot,
		})
	}
	resigned, err := db.GetResignedCommitteeMembers(ctx, credentials, txn)
	if err != nil {
		return CommitteeInfo{}, err
	}

	ret := CommitteeInfo{Members: make([]CommitteeMemberInfo, 0, len(order))}
	for _, key := range order {
		member, ok := latest[key]
		if !ok || member == nil {
			continue
		}
		cold := member.ColdCredHash
		coldScript := member.ColdCredentialTag == 1
		info := CommitteeMemberInfo{
			ColdID:          committeeColdID(cold, coldScript),
			ColdHex:         committeeCredentialHex(true, cold, coldScript),
			ColdHasScript:   coldScript,
			Status:          "not_authorized",
			ExpirationEpoch: member.ExpiresEpoch,
		}
		if resigned[key] {
			info.Status = "resigned"
		} else {
			auth, err := db.GetCommitteeMember(
				ctx, member.ColdCredentialTag, cold, member.TermStartSlot, txn,
			)
			if err != nil && !errors.Is(err, models.ErrCommitteeMemberNotFound) {
				return CommitteeInfo{}, err
			}
			if auth != nil {
				hotScript := auth.HotCredentialTag == 1
				hotID := committeeCredentialID("cc_hot", false, auth.HotCredential, hotScript)
				hotHex := committeeCredentialHex(false, auth.HotCredential, hotScript)
				info.HotID, info.HotHex, info.HotHasScript = &hotID, &hotHex, &hotScript
				info.Status = "authorized"
			}
		}
		ret.Members = append(ret.Members, info)
	}

	if len(ret.Members) == 0 {
		everSeated, err := db.GetCommitteeMembersIncludeDeleted(ctx, txn)
		if err != nil {
			return CommitteeInfo{}, err
		}
		ret.IsDissolved = len(everSeated) > 0
	}

	quorum, err := governance.EffectiveCommitteeQuorum(
		ctx, db, txn, a.ledgerState.CardanoNodeConfig().ConwayGenesis(),
	)
	if err != nil {
		return CommitteeInfo{}, fmt.Errorf("get committee quorum: %w", err)
	}
	ret.QuorumNum = quorum.Num().Uint64()
	ret.QuorumDenom = quorum.Denom().Uint64()

	enacted, err := meta.GetLastEnactedGovernanceProposal(
		[]uint8{
			uint8(lcommon.GovActionTypeNoConfidence),
			uint8(lcommon.GovActionTypeUpdateCommittee),
		},
		txn.Metadata(),
	)
	if err != nil {
		return CommitteeInfo{}, fmt.Errorf("get last committee action: %w", err)
	}
	if enacted != nil {
		id, err := govActionIDString(enacted.TxHash, enacted.ActionIndex)
		if err != nil {
			return CommitteeInfo{}, err
		}
		txHash := hex.EncodeToString(enacted.TxHash)
		index := int(enacted.ActionIndex)
		ret.GovActionID, ret.ProposalTxHash, ret.ProposalIndex = &id, &txHash, &index
	}
	return ret, nil
}

// checkCommitteeVotesAttributed reports ErrHistoryNotRetained while a stored
// committee vote has no authorization mapping its hot credential to a cold
// one: a cold-credential list would silently omit it.
func (a *NodeAdapter) checkCommitteeVotesAttributed(ctx context.Context) error {
	db := a.ledgerState.Database()
	txn := db.Transaction(ctx, false)
	defer txn.Release()
	unattributed, err := db.Metadata().HasUnattributedCommitteeVotes(
		txn.Metadata(),
	)
	if err != nil {
		return fmt.Errorf("check committee vote attribution: %w", err)
	}
	if unattributed {
		return fmt.Errorf(
			"committee hot-key authorizations: %w", ErrHistoryNotRetained,
		)
	}
	return nil
}

// CommitteeVotes returns the votes cast by committee members. A cold
// credential selects the votes of every hot credential it has authorized; a
// hot one selects only its own.
func (a *NodeAdapter) CommitteeVotes(
	ctx context.Context,
	credential *CommitteeCredentialRef,
	cold bool,
	params PaginationParams,
) ([]CommitteeVoteInfo, int, error) {
	voterType := uint8(models.VoterTypeCC)
	filter := models.GovernanceVoteFilter{VoterType: &voterType}
	if credential != nil {
		tag := uint8(0)
		if credential.HasScript {
			tag = 1
		}
		selected := models.CommitteeCredential{
			CredentialTag: tag, Credential: credential.Hash,
		}
		if cold {
			if err := a.checkCommitteeVotesAttributed(ctx); err != nil {
				return nil, 0, err
			}
			filter.AuthorizedBy = &selected
		} else {
			filter.VoterCredentials = []models.CommitteeCredential{selected}
		}
	}
	rows, casts, total, err := a.votesPage(ctx, filter, params)
	if err != nil {
		return nil, 0, err
	}
	ret := make([]CommitteeVoteInfo, 0, len(rows))
	for i, row := range rows {
		proposalID, err := govActionIDString(
			row.ProposalTxHash, row.ProposalActionIndex,
		)
		if err != nil {
			return nil, 0, err
		}
		hotID := committeeCredentialID(
			"cc_hot", false, row.Vote.VoterCredential,
			row.Vote.VoterCredentialTag == 1,
		)
		// Committee votes are the only vote list that carries a block time,
		// so the conversion runs here rather than in castTransactions.
		blockTime, err := a.ledgerState.SlotToTime(row.CastSlot)
		if err != nil {
			return nil, 0, fmt.Errorf(
				"get block time for slot %d: %w", row.CastSlot, err,
			)
		}
		info := CommitteeVoteInfo{
			TxHash:         casts[i].txHash,
			VoterHotID:     hotID,
			ProposalID:     proposalID,
			ProposalTxHash: hex.EncodeToString(row.ProposalTxHash),
			ProposalIndex:  int(row.ProposalActionIndex),
			GovernanceType: governanceTypeName(row.ProposalActionType),
			Vote:           voteName(row.Vote.Vote),
			BlockHeight:    casts[i].blockHeight,
			BlockTime:      blockTime.Unix(),
		}
		if row.Vote.AnchorURL != "" {
			url := row.Vote.AnchorURL
			hash := hex.EncodeToString(row.Vote.AnchorHash)
			info.MetadataURL, info.MetadataHash = &url, &hash
		}
		ret = append(ret, info)
	}
	return ret, total, nil
}
