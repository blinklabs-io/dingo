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

// CommitteeMember represents a Constitutional Committee member imported
// from a Mithril snapshot. This is separate from the certificate-based
// AuthCommitteeHot/ResignCommitteeCold tables, which track committee
// membership changes from on-chain certificates. This table captures
// the committee composition at the time of the snapshot.
type CommitteeMember struct {
	ID           uint    `gorm:"primarykey"`
	ColdCredHash []byte  `gorm:"uniqueIndex;size:28;not null"` // 28-byte credential hash
	ExpiresEpoch uint64  `gorm:"not null"`
	AddedSlot    uint64  `gorm:"index;not null"` // Slot when imported/registered
	DeletedSlot  *uint64 `gorm:"index"`          // For rollback support
}

// TableName returns the table name for CommitteeMember.
func (CommitteeMember) TableName() string {
	return "committee_member"
}
