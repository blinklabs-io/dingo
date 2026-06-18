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

package offchain

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type pointerSource struct {
	SourceType string
	Table      string
	URLColumn  string
	HashColumn string
}

var pointerSources = []pointerSource{
	{
		SourceType: models.OffchainMetadataSourcePool,
		Table:      "pool_registration",
		URLColumn:  "metadata_url",
		HashColumn: "metadata_hash",
	},
	{
		SourceType: models.OffchainMetadataSourceDrep,
		Table:      "drep",
		URLColumn:  "anchor_url",
		HashColumn: "anchor_hash",
	},
	{
		SourceType: models.OffchainMetadataSourceDrepRegistration,
		Table:      "registration_drep",
		URLColumn:  "anchor_url",
		HashColumn: "anchor_hash",
	},
	{
		SourceType: models.OffchainMetadataSourceDrepUpdate,
		Table:      "update_drep",
		URLColumn:  "anchor_url",
		HashColumn: "anchor_hash",
	},
	{
		SourceType: models.OffchainMetadataSourceGovernanceProposal,
		Table:      "governance_proposal",
		URLColumn:  "anchor_url",
		HashColumn: "anchor_hash",
	},
	{
		SourceType: models.OffchainMetadataSourceGovernanceVote,
		Table:      "governance_vote",
		URLColumn:  "anchor_url",
		HashColumn: "anchor_hash",
	},
	{
		SourceType: models.OffchainMetadataSourceConstitution,
		Table:      "constitution",
		URLColumn:  "anchor_url",
		HashColumn: "anchor_hash",
	},
	{
		SourceType: models.OffchainMetadataSourceCommitteeResign,
		Table:      "resign_committee_cold",
		URLColumn:  "anchor_url",
		HashColumn: "anchor_hash",
	},
}

var fetchableStatuses = []string{
	models.OffchainMetadataStatusPending,
	models.OffchainMetadataStatusFailed,
}

const fetchClaimLease = 30 * time.Minute

type pointerRow struct {
	URL  string `gorm:"column:url"`
	Hash []byte `gorm:"column:hash"`
}

// EnsurePointers adds pending off-chain metadata cache rows for every
// currently known on-chain URL/hash pointer.
func EnsurePointers(
	ctx context.Context,
	db *gorm.DB,
	now time.Time,
) (int, error) {
	db = withContext(ctx, db)
	created := 0
	seen := make(map[string]struct{})
	for _, source := range pointerSources {
		rows, err := queryPointers(db, source)
		if err != nil {
			return created, err
		}
		for _, row := range rows {
			url := strings.TrimSpace(row.URL)
			if url == "" || len(row.Hash) != 32 {
				continue
			}
			key := source.SourceType + "\x00" + url + "\x00" + string(row.Hash)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			doc := models.OffchainMetadata{
				SourceType:     source.SourceType,
				URL:            url,
				Hash:           row.Hash,
				Status:         models.OffchainMetadataStatusPending,
				NextFetchAfter: &now,
			}
			result := db.Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "source_type"},
					{Name: "url"},
					{Name: "hash"},
				},
				DoNothing: true,
			}).Create(&doc)
			if result.Error != nil {
				return created, fmt.Errorf(
					"insert off-chain metadata pointer from %s: %w",
					source.Table,
					result.Error,
				)
			}
			created += int(result.RowsAffected)
		}
	}
	return created, nil
}

func queryPointers(
	db *gorm.DB,
	source pointerSource,
) ([]pointerRow, error) {
	var rows []pointerRow
	selectExpr := fmt.Sprintf(
		"%s AS url, %s AS hash",
		source.URLColumn,
		source.HashColumn,
	)
	result := db.Table(source.Table).
		Select(selectExpr).
		Where(source.URLColumn + " <> ''").
		Group(source.URLColumn + ", " + source.HashColumn).
		Scan(&rows)
	if result.Error != nil {
		return nil, fmt.Errorf(
			"query off-chain metadata pointers from %s: %w",
			source.Table,
			result.Error,
		)
	}
	return rows, nil
}

// FetchBatch claims and returns pending or failed cache rows that are due for
// another fetch attempt.
func FetchBatch(
	ctx context.Context,
	db *gorm.DB,
	limit int,
	now time.Time,
) ([]models.OffchainMetadata, error) {
	if limit <= 0 {
		limit = 1
	}
	db = withContext(ctx, db)
	claimUntil := now.Add(fetchClaimLease)
	docs := make([]models.OffchainMetadata, 0, limit)
	for len(docs) < limit {
		remaining := limit - len(docs)
		var candidates []models.OffchainMetadata
		result := db.
			Where(
				"status IN ? AND (next_fetch_after IS NULL OR next_fetch_after <= ?)",
				fetchableStatuses,
				now,
			).
			Order("next_fetch_after ASC, id ASC").
			Limit(remaining).
			Find(&candidates)
		if result.Error != nil {
			return nil, fmt.Errorf(
				"query off-chain metadata fetch batch: %w",
				result.Error,
			)
		}
		if len(candidates) == 0 {
			break
		}
		for i := range candidates {
			claimed, err := claimFetch(db, candidates[i].ID, now, claimUntil)
			if err != nil {
				return nil, err
			}
			if !claimed {
				continue
			}
			doc := candidates[i]
			nextFetchAfter := claimUntil
			doc.NextFetchAfter = &nextFetchAfter
			docs = append(docs, doc)
			if len(docs) == limit {
				break
			}
		}
	}
	return docs, nil
}

func claimFetch(
	db *gorm.DB,
	id uint,
	now time.Time,
	claimUntil time.Time,
) (bool, error) {
	result := db.Model(&models.OffchainMetadata{}).
		Where(
			"id = ? AND status IN ? AND "+
				"(next_fetch_after IS NULL OR next_fetch_after <= ?)",
			id,
			fetchableStatuses,
			now,
		).
		Update("next_fetch_after", claimUntil)
	if result.Error != nil {
		return false, fmt.Errorf(
			"claim off-chain metadata fetch row %d: %w",
			id,
			result.Error,
		)
	}
	return result.RowsAffected > 0, nil
}

// SetFetchResult persists the worker's latest fetch status for an existing
// off-chain metadata cache row.
func SetFetchResult(
	ctx context.Context,
	db *gorm.DB,
	doc *models.OffchainMetadata,
) error {
	if doc == nil || doc.ID == 0 {
		return errors.New("off-chain metadata fetch result missing row ID")
	}
	db = withContext(ctx, db)
	updates := map[string]any{
		"status":           doc.Status,
		"content_type":     doc.ContentType,
		"last_error":       doc.LastError,
		"body_hash":        doc.BodyHash,
		"content":          doc.Content,
		"fetched_at":       doc.FetchedAt,
		"next_fetch_after": doc.NextFetchAfter,
		"fetch_attempts":   doc.FetchAttempts,
		"last_http_status": doc.LastHTTPStatus,
	}
	result := db.Model(&models.OffchainMetadata{}).
		Where("id = ?", doc.ID).
		Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("update off-chain metadata fetch result: %w", result.Error)
	}
	return nil
}

func withContext(ctx context.Context, db *gorm.DB) *gorm.DB {
	if ctx == nil {
		return db
	}
	return db.WithContext(ctx)
}

// Get retrieves one cached off-chain metadata document by source and on-chain
// URL/hash pointer. It returns nil when no cache row exists yet.
func Get(
	db *gorm.DB,
	sourceType string,
	url string,
	hash []byte,
) (*models.OffchainMetadata, error) {
	var doc models.OffchainMetadata
	result := db.Where(
		"source_type = ? AND url = ? AND hash = ?",
		sourceType,
		url,
		hash,
	).First(&doc)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get off-chain metadata: %w", result.Error)
	}
	return &doc, nil
}
