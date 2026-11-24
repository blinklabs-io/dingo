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

package models

import "errors"

var ErrGovernanceProposalNotFound = errors.New("governance proposal not found")

// VoterType constants represent the type of voter casting a governance vote.
const (
	VoterTypeCC   = 0 // Constitutional Committee member
	VoterTypeDRep = 1 // DRep
	VoterTypeSPO  = 2 // Stake Pool Operator
)

// Vote constants represent the vote choice on a governance proposal.
const (
	VoteNo      = 0
	VoteYes     = 1
	VoteAbstain = 2
)

// Constitution represents the on-chain constitution document reference.
// The constitution is established via governance action and contains a URL
// and hash pointing to the full document, plus an optional guardrails script.
type Constitution struct {
	ID          uint
	AnchorURL   string
	AnchorHash  []byte
	PolicyHash  []byte
	AddedSlot   uint64
	DeletedSlot *uint64
}

// GovernanceProposal represents a governance action submitted to the chain.
// Proposals have a lifecycle: submitted -> (ratified) -> (enacted) or expired.
type GovernanceProposal struct {
	ID              uint
	TxHash          []byte
	ActionIndex     uint32
	ActionType      uint8 // GovActionType enum
	ProposedEpoch   uint64
	ExpiresEpoch    uint64
	ParentTxHash    []byte
	ParentActionIdx *uint32
	EnactedEpoch    *uint64
	EnactedSlot     *uint64 // Slot when enacted (for rollback safety)
	RatifiedEpoch   *uint64
	RatifiedSlot    *uint64 // Slot when ratified (for rollback safety)
	PolicyHash      []byte
	AnchorURL       string
	AnchorHash      []byte
	Deposit         uint64
	ReturnAddress   []byte // Reward account for deposit return (1 byte header + 28 bytes hash)
	// GovActionCbor holds the CBOR-encoded GovAction needed at enactment
	// time to extract type-specific fields (ParamUpdate, ProtocolVersion,
	// Withdrawals, Committee changes, Constitution). Populated on proposal
	// submission so enactment does not need to re-fetch the transaction.
	GovActionCbor []byte
	ExpiredEpoch  *uint64
	ExpiredSlot   *uint64 // Slot when expired (for rollback safety)
	// DroppedEpoch/DroppedSlot record when an expired proposal's deposit was
	// actually refunded and the proposal left final consideration.
	// cardano-ledger does not refund the deposit in the same epoch it detects
	// expiry -- that happens one full epoch later, the same one-epoch delay
	// ratification has before enactment. ExpiredEpoch/ExpiredSlot alone would
	// collapse that delay, so the drop is tracked separately.
	DroppedEpoch *uint64
	DroppedSlot  *uint64
	// TxIndex is the proposal's position in the proposal submission order
	// within AddedSlot: the transaction's index in its block, or for a
	// proposal imported from a ledger-state snapshot, its position in the
	// snapshot's proposal sequence. Conway RATIFY breaks equal-priority ties
	// by this order. Nil for a row stored before the position was recorded;
	// such rows keep the transaction-hash tie-break.
	TxIndex     *uint32
	AddedSlot   uint64
	DeletedSlot *uint64
}

// GovernanceVote represents a vote cast by a Constitutional Committee member,
// DRep, or Stake Pool Operator on a governance proposal.
type GovernanceVote struct {
	ID                 uint
	ProposalID         uint
	VoterType          uint8 // 0=CC, 1=DRep, 2=SPO
	VoterCredentialTag uint8
	VoterCredential    []byte
	Vote               uint8 // 0=No, 1=Yes, 2=Abstain
	AnchorURL          string
	AnchorHash         []byte
	AddedSlot          uint64
	// Slot when vote was last changed (for rollback safety).
	VoteUpdatedSlot *uint64
	DeletedSlot     *uint64
	// CastTxHash and CastCertIndex locate the voting procedure that casts
	// this value: the transaction and the procedure's position among the
	// voter's own procedures in it. They are recorded in the vote history on
	// write and are not read back into this struct.
	CastTxHash    []byte
	CastCertIndex uint32
}

// GovernanceVoteRef is a vote with the proposal it was cast on, named by the
// proposal's transaction hash and action index rather than its row ID.
type GovernanceVoteRef struct {
	TxHash      []byte
	ActionIndex uint32
	Vote        GovernanceVote
}

// GovernanceActionRef identifies one governance action without depending on
// the metadata store's surrogate proposal ID.
type GovernanceActionRef struct {
	TxHash      []byte `json:"tx_hash"`
	ActionIndex uint32 `json:"action_index"`
}

// DRepStakeDistributionEntry is one member of the DRep stake distribution
// frozen at an epoch boundary. DRepType uses the account delegation values:
// 0/1 are credential-backed DReps and 2/3 are AlwaysAbstain and
// AlwaysNoConfidence. Credential is empty for the predefined DReps.
type DRepStakeDistributionEntry struct {
	Credential    []byte `json:"credential,omitempty"`
	Stake         uint64 `json:"stake"`
	DRepType      uint8  `json:"drep_type"`
	CredentialTag uint8  `json:"credential_tag,omitempty"`
}

// GovernanceCommitteeMember is one member of the committee in the
// post-ENACT state consumed by RATIFY.
type GovernanceCommitteeMember struct {
	ColdCredentialTag uint8  `json:"cold_credential_tag"`
	ColdCredential    []byte `json:"cold_credential"`
	ExpiresEpoch      uint64 `json:"expires_epoch"`
}

// GovernanceConstitution is the constitution content in the post-ENACT state.
// Database row identity and lifecycle slots are not part of the ledger state.
type GovernanceConstitution struct {
	AnchorURL  string `json:"anchor_url"`
	AnchorHash []byte `json:"anchor_hash"`
	PolicyHash []byte `json:"policy_hash,omitempty"`
}

// ErrGovernanceBoundarySnapshotNotRetained reports that the epoch containing
// the requested slot is a Conway-or-later epoch whose boundary snapshot is not
// stored and could not be rebuilt at start-up: the live UTxO set's journal or
// the Mithril trust boundary leaves the state before that boundary
// unrecoverable. Callers must report the state as
// unavailable rather than substitute another epoch's snapshot or live voting
// state.
var ErrGovernanceBoundarySnapshotNotRetained = errors.New(
	"governance boundary snapshot not retained for epoch",
)

// GovernanceBoundarySnapshot retains the proposal-independent pulser input
// and RATIFY outcome produced at a Conway-or-later epoch boundary. Epoch is
// the epoch the boundary opens and BoundarySlot is that epoch's first slot, so
// the snapshot describes the state every slot of Epoch is validated against. The base
// enact state is the state after ENACT and before RATIFY stages the ordered
// Ratified actions; consumers apply those action bodies in order to obtain the
// completed RatifyState enact state.
type GovernanceBoundarySnapshot struct {
	Epoch                uint64                       `json:"epoch"`
	BoundarySlot         uint64                       `json:"boundary_slot"`
	DRepDistribution     []DRepStakeDistributionEntry `json:"drep_distribution"`
	Committee            []GovernanceCommitteeMember  `json:"committee"`
	CommitteeQuorum      string                       `json:"committee_quorum"`
	Constitution         *GovernanceConstitution      `json:"constitution,omitempty"`
	PurposeRoots         []*GovernanceActionRef       `json:"purpose_roots"`
	Ratified             []GovernanceActionRef        `json:"ratified"`
	Expired              []GovernanceActionRef        `json:"expired"`
	PostEnactmentPParams []byte                       `json:"post_enactment_pparams"`
	FuturePParams        []byte                       `json:"future_pparams,omitempty"`
	RatificationTreasury uint64                       `json:"ratification_treasury"`
	Delayed              bool                         `json:"delayed"`
	ImportedRatifyState  []byte                       `json:"imported_ratify_state,omitempty"`
}

// ResignCommitteeCold represents a resignation certificate for a
// Constitutional Committee cold credential.
type ResignCommitteeCold struct {
	AnchorURL         string
	ColdCredentialTag uint8
	ColdCredential    []byte
	AnchorHash        []byte
	ID                uint
	CertificateID     uint
	AddedSlot         uint64
}
