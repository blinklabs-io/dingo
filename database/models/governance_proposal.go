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
	AnchorUrl       string  `gorm:"size:128;not null"`
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
