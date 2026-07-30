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

// TableName returns the table name
func (Constitution) TableName() string {
	return "constitution"
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
	AddedSlot     uint64
	DeletedSlot   *uint64
}

// TableName returns the table name
func (GovernanceProposal) TableName() string {
	return "governance_proposal"
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
}

// TableName returns the table name
func (GovernanceVote) TableName() string {
	return "governance_vote"
}

// ResignCommitteeCold represents a resignation certificate for a
// Constitutional Committee cold credential.
type ResignCommitteeCold struct {
	AnchorURL      string
	ColdCredential []byte
	AnchorHash     []byte
	ID             uint
	CertificateID  uint
	AddedSlot      uint64
}

// TableName returns the table name
func (ResignCommitteeCold) TableName() string {
	return "resign_committee_cold"
}
