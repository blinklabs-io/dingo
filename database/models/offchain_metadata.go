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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package models

import "time"

const (
	OffchainMetadataSourcePool               = "pool"
	OffchainMetadataSourceDrep               = "drep"
	OffchainMetadataSourceDrepRegistration   = "drep_registration"
	OffchainMetadataSourceDrepUpdate         = "drep_update"
	OffchainMetadataSourceGovernanceProposal = "gov_proposal"
	OffchainMetadataSourceGovernanceVote     = "gov_vote"
	OffchainMetadataSourceConstitution       = "constitution"
	OffchainMetadataSourceCommitteeResign    = "committee_resign"

	OffchainMetadataStatusPending = "pending"
	OffchainMetadataStatusFetched = "fetched"
	OffchainMetadataStatusFailed  = "failed"
)

// OffchainMetadata stores a fetched copy of content referenced by on-chain
// metadata and governance anchors. The on-chain URL/hash pair remains
// authoritative; this table is a best-effort API cache.
type OffchainMetadata struct {
	FetchedAt      *time.Time
	NextFetchAfter *time.Time `gorm:"index:idx_offchain_metadata_status_next,priority:2"`
	CreatedAt      time.Time
	UpdatedAt      time.Time
	URL            string `gorm:"size:512;not null;uniqueIndex:idx_offchain_metadata_source_url_hash,priority:2"`
	SourceType     string `gorm:"size:32;not null;uniqueIndex:idx_offchain_metadata_source_url_hash,priority:1"`
	Status         string `gorm:"size:16;not null;index:idx_offchain_metadata_status_next,priority:1"`
	ContentType    string `gorm:"size:128"`
	LastError      string `gorm:"size:1024"`
	Hash           []byte `gorm:"size:32;not null;uniqueIndex:idx_offchain_metadata_source_url_hash,priority:3"`
	BodyHash       []byte `gorm:"size:32"`
	Content        []byte
	ID             uint `gorm:"primarykey"`
	FetchAttempts  uint
	LastHTTPStatus uint
}

func (OffchainMetadata) TableName() string {
	return "offchain_metadata"
}
