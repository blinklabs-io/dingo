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

// GovernanceVote represents a vote cast by a Constitutional Committee member,
// DRep, or Stake Pool Operator on a governance proposal.
type GovernanceVote struct {
	ID              uint    `gorm:"primarykey"`
	ProposalID      uint    `gorm:"index:idx_vote_proposal;uniqueIndex:idx_vote_unique,priority:1;not null"`
	VoterType       uint8   `gorm:"index:idx_vote_voter,priority:1;uniqueIndex:idx_vote_unique,priority:2;not null"` // 0=CC, 1=DRep, 2=SPO
	VoterCredential []byte  `gorm:"index:idx_vote_voter,priority:2;uniqueIndex:idx_vote_unique,priority:3;size:28;not null"`
	Vote            uint8   `gorm:"not null"` // 0=No, 1=Yes, 2=Abstain
	AnchorUrl       string  `gorm:"size:128"`
	AnchorHash      []byte  `gorm:"size:32"`
	AddedSlot       uint64  `gorm:"index;not null"`
	VoteUpdatedSlot *uint64 `gorm:"index"` // Slot when vote was last changed (for rollback safety)
	DeletedSlot     *uint64 `gorm:"index"`
}

// TableName returns the table name
func (GovernanceVote) TableName() string {
	return "governance_vote"
}
