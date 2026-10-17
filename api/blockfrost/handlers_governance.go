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
	"net/http"
	"strconv"
	"strings"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/btcsuite/btcd/btcutil/bech32"
)

// writeInvalidIdentifier answers a malformed path identifier.
func writeInvalidIdentifier(w http.ResponseWriter, what string) {
	writeError(w, http.StatusBadRequest, "Bad Request", "Invalid "+what+".")
}

// govActionRefFromParts parses the transaction hash and certificate index
// path form of a governance action reference.
func govActionRefFromParts(txHash, index string) (GovActionRef, bool) {
	hash, err := hex.DecodeString(txHash)
	if err != nil || len(hash) != 32 {
		return GovActionRef{}, false
	}
	parsed, err := strconv.ParseUint(index, 10, 32)
	if err != nil {
		return GovActionRef{}, false
	}
	return GovActionRef{TxHash: hash, Index: uint32(parsed)}, true
}

// govActionRefFromID parses a CIP-129 governance action identifier.
func govActionRefFromID(id string) (GovActionRef, bool) {
	var parsed lcommon.GovActionId
	if err := parsed.UnmarshalText([]byte(id)); err != nil {
		return GovActionRef{}, false
	}
	return GovActionRef{
		TxHash: append([]byte(nil), parsed.TransactionId[:]...),
		Index:  parsed.GovActionIdx,
	}, true
}

// proposalSubresources are the path words that name what follows a
// governance action identifier. They cannot be certificate indexes, which is
// what tells the identifier form from the transaction hash form.
var proposalSubresources = map[string]bool{
	"parameters": true, "withdrawals": true, "votes": true, "metadata": true,
}

// handleProposalByID handles GET /api/v0/governance/proposals/{first}, the
// governance action identifier form of the proposal.
func (b *Blockfrost) handleProposalByID(
	w http.ResponseWriter,
	r *http.Request,
) {
	ref, ok := govActionRefFromID(r.PathValue("first"))
	if !ok {
		writeInvalidIdentifier(w, "governance action id")
		return
	}
	b.serveProposalPart(w, r, ref, "")
}

// handleProposalTwoSegments handles
// GET /api/v0/governance/proposals/{first}/{second}: either a proposal by
// transaction hash and certificate index, or a part of a proposal by
// governance action identifier. One route serves both forms so they share
// the identifier parsing and its error path.
func (b *Blockfrost) handleProposalTwoSegments(
	w http.ResponseWriter,
	r *http.Request,
) {
	first, second := r.PathValue("first"), r.PathValue("second")
	if proposalSubresources[second] {
		ref, ok := govActionRefFromID(first)
		if !ok {
			writeInvalidIdentifier(w, "governance action id")
			return
		}
		b.serveProposalPart(w, r, ref, second)
		return
	}
	ref, ok := govActionRefFromParts(first, second)
	if !ok {
		writeInvalidIdentifier(w, "transaction hash or certificate index")
		return
	}
	b.serveProposalPart(w, r, ref, "")
}

// handleProposalThreeSegments handles
// GET /api/v0/governance/proposals/{tx_hash}/{cert_index}/{part}.
func (b *Blockfrost) handleProposalThreeSegments(
	w http.ResponseWriter,
	r *http.Request,
) {
	part := r.PathValue("third")
	if !proposalSubresources[part] {
		b.handleNotFound(w, r)
		return
	}
	ref, ok := govActionRefFromParts(
		r.PathValue("first"),
		r.PathValue("second"),
	)
	if !ok {
		writeInvalidIdentifier(w, "transaction hash or certificate index")
		return
	}
	b.serveProposalPart(w, r, ref, part)
}

// serveProposalPart serves a proposal or one of its parts: "" is the
// proposal itself.
func (b *Blockfrost) serveProposalPart(
	w http.ResponseWriter,
	r *http.Request,
	ref GovActionRef,
	part string,
) {
	switch part {
	case "":
		info, err := b.node.GovernanceProposal(r.Context(), ref)
		if err != nil {
			b.writeHistoryError(w, err, "failed to retrieve proposal")
			return
		}
		writeJSON(w, http.StatusOK, GovernanceProposalResponse{
			ID:                    info.ID,
			TxHash:                info.TxHash,
			CertIndex:             info.CertIndex,
			GovernanceType:        info.GovernanceType,
			GovernanceDescription: info.Description,
			Deposit:               info.Deposit,
			ReturnAddress:         info.ReturnAddress,
			RatifiedEpoch:         info.RatifiedEpoch,
			EnactedEpoch:          info.EnactedEpoch,
			DroppedEpoch:          info.DroppedEpoch,
			ExpiredEpoch:          info.ExpiredEpoch,
			Expiration:            info.Expiration,
		})
	case "parameters":
		info, err := b.node.GovernanceProposalParameters(r.Context(), ref)
		if err != nil {
			b.writeHistoryError(
				w,
				err,
				"failed to retrieve proposal parameters",
			)
			return
		}
		writeJSON(w, http.StatusOK, GovernanceProposalParametersResponse(info))
	case "withdrawals":
		servePage(
			b,
			w,
			r,
			func(ctx context.Context, params PaginationParams) ([]GovernanceWithdrawalInfo, int, error) {
				return b.node.GovernanceProposalWithdrawals(ctx, ref, params)
			},
			func(item GovernanceWithdrawalInfo) GovernanceWithdrawalResponse {
				return GovernanceWithdrawalResponse(item)
			},
			"failed to retrieve proposal withdrawals",
		)
	case "votes":
		servePage(
			b,
			w,
			r,
			func(ctx context.Context, params PaginationParams) ([]GovernanceProposalVoteInfo, int, error) {
				return b.node.GovernanceProposalVotes(ctx, ref, params)
			},
			func(item GovernanceProposalVoteInfo) GovernanceProposalVoteResponse {
				return GovernanceProposalVoteResponse(item)
			},
			"failed to retrieve proposal votes",
		)
	case "metadata":
		info, err := b.node.GovernanceProposalMetadata(r.Context(), ref)
		if err != nil {
			b.writeHistoryError(w, err, "failed to retrieve proposal metadata")
			return
		}
		writeJSON(w, http.StatusOK, GovernanceProposalMetadataResponse(info))
	}
}

// handleProposals handles GET /api/v0/governance/proposals.
func (b *Blockfrost) handleProposals(
	w http.ResponseWriter,
	r *http.Request,
) {
	servePage(b, w, r,
		b.node.GovernanceProposals,
		func(item GovernanceProposalInfo) GovernanceProposalListResponse {
			return GovernanceProposalListResponse(item)
		},
		"failed to retrieve proposals",
	)
}

// drepOrWriteError parses the {drep_id} path value.
func drepOrWriteError(
	w http.ResponseWriter,
	r *http.Request,
) (DRepCredential, bool) {
	credential, err := parseDRepIdentifier(r.PathValue("drep_id"))
	if err != nil {
		writeInvalidIdentifier(w, "DRep identifier")
		return DRepCredential{}, false
	}
	return credential, true
}

// handleDRepDelegators handles
// GET /api/v0/governance/dreps/{drep_id}/delegators.
func (b *Blockfrost) handleDRepDelegators(
	w http.ResponseWriter,
	r *http.Request,
) {
	credential, ok := drepOrWriteError(w, r)
	if !ok {
		return
	}
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]DRepDelegatorInfo, int, error) {
			return b.node.DRepDelegators(ctx, credential, params)
		},
		func(item DRepDelegatorInfo) DRepDelegatorResponse {
			return DRepDelegatorResponse(item)
		},
		"failed to retrieve DRep delegators",
	)
}

// handleDRepMetadata handles
// GET /api/v0/governance/dreps/{drep_id}/metadata.
func (b *Blockfrost) handleDRepMetadata(
	w http.ResponseWriter,
	r *http.Request,
) {
	credential, ok := drepOrWriteError(w, r)
	if !ok {
		return
	}
	info, err := b.node.DRepMetadata(r.Context(), credential)
	if err != nil {
		b.writeHistoryError(w, err, "failed to retrieve DRep metadata")
		return
	}
	resp := DRepAnchorResponse{
		DRepID:       info.DRepID,
		Hex:          info.Hex,
		URL:          info.URL,
		Hash:         info.Hash,
		JSONMetadata: info.JSONMetadata,
		Bytes:        info.Bytes,
	}
	if info.Error != nil {
		respErr := OffchainFetchErrorResponse(*info.Error)
		resp.Error = &respErr
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleDRepUpdates handles
// GET /api/v0/governance/dreps/{drep_id}/updates.
func (b *Blockfrost) handleDRepUpdates(
	w http.ResponseWriter,
	r *http.Request,
) {
	credential, ok := drepOrWriteError(w, r)
	if !ok {
		return
	}
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]DRepUpdateInfo, int, error) {
			return b.node.DRepUpdates(ctx, credential, params)
		},
		func(item DRepUpdateInfo) DRepUpdateResponse {
			return DRepUpdateResponse(item)
		},
		"failed to retrieve DRep updates",
	)
}

// handleDRepVotes handles GET /api/v0/governance/dreps/{drep_id}/votes.
func (b *Blockfrost) handleDRepVotes(
	w http.ResponseWriter,
	r *http.Request,
) {
	credential, ok := drepOrWriteError(w, r)
	if !ok {
		return
	}
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]DRepVoteInfo, int, error) {
			return b.node.DRepVotes(ctx, credential, params)
		},
		func(item DRepVoteInfo) DRepVoteResponse {
			return DRepVoteResponse(item)
		},
		"failed to retrieve DRep votes",
	)
}

// handlePoolVotes handles GET /api/v0/pools/{pool_id}/votes.
func (b *Blockfrost) handlePoolVotes(
	w http.ResponseWriter,
	r *http.Request,
) {
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]PoolVoteInfo, int, error) {
			return b.node.PoolVotes(ctx, r.PathValue("pool_id"), params)
		},
		func(item PoolVoteInfo) PoolVoteResponse {
			return PoolVoteResponse(item)
		},
		"failed to retrieve pool votes",
	)
}

// handleCommittee handles GET /api/v0/governance/committee.
func (b *Blockfrost) handleCommittee(
	w http.ResponseWriter,
	r *http.Request,
) {
	info, err := b.node.GovernanceCommittee(r.Context())
	if err != nil {
		b.writeHistoryError(w, err, "failed to retrieve committee")
		return
	}
	members := make([]CommitteeMemberResponse, 0, len(info.Members))
	for _, member := range info.Members {
		members = append(members, CommitteeMemberResponse{
			CCColdID:        member.ColdID,
			CCColdHex:       member.ColdHex,
			CCColdHasScript: member.ColdHasScript,
			CCHotID:         member.HotID,
			CCHotHex:        member.HotHex,
			CCHotHasScript:  member.HotHasScript,
			Status:          member.Status,
			ExpirationEpoch: member.ExpirationEpoch,
		})
	}
	writeJSON(w, http.StatusOK, CommitteeResponse{
		GovActionID:    info.GovActionID,
		ProposalTxHash: info.ProposalTxHash,
		ProposalIndex:  info.ProposalIndex,
		IsDissolved:    info.IsDissolved,
		Quorum: CommitteeQuorumResponse{
			Numerator:   info.QuorumNum,
			Denominator: info.QuorumDenom,
		},
		Members: members,
	})
}

func committeeVoteResponse(item CommitteeVoteInfo) CommitteeVoteResponse {
	return CommitteeVoteResponse(item)
}

// handleCommitteeVotes handles GET /api/v0/governance/committee/votes.
func (b *Blockfrost) handleCommitteeVotes(
	w http.ResponseWriter,
	r *http.Request,
) {
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]CommitteeVoteInfo, int, error) {
			return b.node.CommitteeVotes(ctx, nil, false, params)
		},
		committeeVoteResponse,
		"failed to retrieve committee votes",
	)
}

// parseCommitteeCredential parses a CIP-129 committee credential
// identifier, reporting whether it names a cold credential.
func parseCommitteeCredential(id string) (CommitteeCredentialRef, bool, bool) {
	hrp, data, err := bech32.Decode(id)
	if err != nil {
		return CommitteeCredentialRef{}, false, false
	}
	cold := false
	switch strings.ToLower(hrp) {
	case "cc_hot":
	case "cc_cold":
		cold = true
	default:
		return CommitteeCredentialRef{}, false, false
	}
	payload, err := bech32.ConvertBits(data, 5, 8, false)
	if err != nil || len(payload) != 29 {
		return CommitteeCredentialRef{}, false, false
	}
	header := payload[0]
	if header != committeeCredentialHeader(cold, false) &&
		header != committeeCredentialHeader(cold, true) {
		return CommitteeCredentialRef{}, false, false
	}
	return CommitteeCredentialRef{
		Hash:      payload[1:],
		HasScript: header == committeeCredentialHeader(cold, true),
	}, cold, true
}

// handleCommitteeMemberVotes handles
// GET /api/v0/governance/committee/{cc_id}/votes.
func (b *Blockfrost) handleCommitteeMemberVotes(
	w http.ResponseWriter,
	r *http.Request,
) {
	credential, cold, ok := parseCommitteeCredential(r.PathValue("cc_id"))
	if !ok {
		writeInvalidIdentifier(w, "committee credential")
		return
	}
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]CommitteeVoteInfo, int, error) {
			return b.node.CommitteeVotes(ctx, &credential, cold, params)
		},
		committeeVoteResponse,
		"failed to retrieve committee votes",
	)
}
