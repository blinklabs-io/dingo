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

	// OffchainFetchErrHashMismatch is the exact LastError recorded when
	// the fetched document does not match the on-chain anchor hash. The
	// API error classification matches on it, so the fetcher and API
	// must agree on the text.
	OffchainFetchErrHashMismatch = "metadata hash mismatch"
	// OffchainFetchErrBodyTooLargePrefix prefixes the LastError recorded
	// when the response body exceeds the fetch size limit.
	OffchainFetchErrBodyTooLargePrefix = "response body exceeds"
	// OffchainFetchErrDecodeErrorPrefix prefixes the LastError recorded
	// when hash-valid content fails schema validation for its off-chain
	// metadata source (for example, stake-pool metadata missing a
	// required field or violating a field length constraint). The API
	// error classification matches on it, so the fetcher and API must
	// agree on the text.
	OffchainFetchErrDecodeErrorPrefix = "metadata decode error"

	OffchainMetadataStatusPending = "pending"
	OffchainMetadataStatusFetched = "fetched"
	OffchainMetadataStatusFailed  = "failed"
)

// OffchainMetadata stores a fetched copy of content referenced by on-chain
// metadata and governance anchors. The on-chain URL/hash pair remains
// authoritative; this table is a best-effort API cache.
type OffchainMetadata struct {
	FetchedAt      *time.Time
	NextFetchAfter *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	URL            string
	SourceType     string
	Status         string
	ContentType    string
	LastError      string
	Hash           []byte
	BodyHash       []byte
	Content        []byte
	ID             uint
	FetchAttempts  uint
	LastHTTPStatus uint
}

func (OffchainMetadata) TableName() string {
	return "offchain_metadata"
}
