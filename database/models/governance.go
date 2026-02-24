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
	ID          uint    `gorm:"primarykey"`
	AnchorURL   string  `gorm:"column:anchor_url;size:128;not null"`
	AnchorHash  []byte  `gorm:"size:32;not null"`
	PolicyHash  []byte  `gorm:"size:28"`
	AddedSlot   uint64  `gorm:"uniqueIndex;not null"`
	DeletedSlot *uint64 `gorm:"index"`
}

// TableName returns the table name
func (Constitution) TableName() string {
	return "constitution"
}

// GovernanceProposal represents a governance action submitted to the chain.
// Proposals have a lifecycle: submitted -> (ratified) -> (enacted) or expired.
type GovernanceProposal struct {
	ID              uint   `gorm:"primarykey"`
	TxHash          []byte `gorm:"uniqueIndex:idx_proposal_tx_action,priority:1;size:32;not null"`
	ActionIndex     uint32 `gorm:"uniqueIndex:idx_proposal_tx_action,priority:2;not null"`
	ActionType      uint8  `gorm:"index;not null"` // GovActionType enum
	ProposedEpoch   uint64 `gorm:"index;not null"`
	ExpiresEpoch    uint64 `gorm:"index;not null"`
	ParentTxHash    []byte `gorm:"size:32"`
	ParentActionIdx *uint32
	EnactedEpoch    *uint64
	EnactedSlot     *uint64 `gorm:"index"` // Slot when enacted (for rollback safety)
	RatifiedEpoch   *uint64
	RatifiedSlot    *uint64 `gorm:"index"` // Slot when ratified (for rollback safety)
	PolicyHash      []byte  `gorm:"size:28"`
	AnchorURL       string  `gorm:"column:anchor_url;size:128;not null"`
	AnchorHash      []byte  `gorm:"size:32;not null"`
	Deposit         uint64  `gorm:"not null"`
	ReturnAddress   []byte  `gorm:"size:29;not null"` // Reward account for deposit return (1 byte header + 28 bytes hash)
	AddedSlot       uint64  `gorm:"index;not null"`
	DeletedSlot     *uint64 `gorm:"index"`
}

// TableName returns the table name
func (GovernanceProposal) TableName() string {
	return "governance_proposal"
}

// GovernanceVote represents a vote cast by a Constitutional Committee member,
// DRep, or Stake Pool Operator on a governance proposal.
type GovernanceVote struct {
	ID              uint    `gorm:"primarykey"`
	ProposalID      uint    `gorm:"index:idx_vote_proposal;uniqueIndex:idx_vote_unique,priority:1;not null"`
	VoterType       uint8   `gorm:"index:idx_vote_voter,priority:1;uniqueIndex:idx_vote_unique,priority:2;not null"` // 0=CC, 1=DRep, 2=SPO
	VoterCredential []byte  `gorm:"index:idx_vote_voter,priority:2;uniqueIndex:idx_vote_unique,priority:3;size:28;not null"`
	Vote            uint8   `gorm:"not null"` // 0=No, 1=Yes, 2=Abstain
	AnchorURL       string  `gorm:"column:anchor_url;size:128"`
	AnchorHash      []byte  `gorm:"size:32"`
	AddedSlot       uint64  `gorm:"index;not null"`
	VoteUpdatedSlot *uint64 `gorm:"index"` // Slot when vote was last changed (for rollback safety)
	DeletedSlot     *uint64 `gorm:"index"`
}

// TableName returns the table name
func (GovernanceVote) TableName() string {
	return "governance_vote"
}

// ResignCommitteeCold represents a resignation certificate for a
// Constitutional Committee cold credential.
type ResignCommitteeCold struct {
	AnchorURL      string `gorm:"column:anchor_url;size:128"`
	ColdCredential []byte `gorm:"index;size:28"`
	AnchorHash     []byte
	ID             uint   `gorm:"primarykey"`
	CertificateID  uint   `gorm:"index"`
	AddedSlot      uint64 `gorm:"index"`
}

// TableName returns the table name
func (ResignCommitteeCold) TableName() string {
	return "resign_committee_cold"
}
